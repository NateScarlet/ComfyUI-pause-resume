import unittest
import sqlite3
import os
import time
import json
from gateway.infrastructure.sqlite.queue import SQLiteQueue
from gateway.shared.models import Task, RawJSON, TaskStatus, TaskFilters, TaskSummary


class TestSQLiteQueue(unittest.TestCase):
    """测试 SQLiteQueue 的正确性以及性能指标。"""

    def setUp(self):
        # 使用内存数据库进行测试，保证测试隔离性与运行速度
        self.db_path = ":memory:"
        self.queue = SQLiteQueue(self.db_path)

    def tearDown(self):
        self.queue.close()

    def test_database_migration_and_indexes(self):
        """测试数据库版本迁移以及索引是否成功创建。"""
        conn = self.queue._conn
        cursor = conn.cursor()

        # 检查 user_version 是否确实升级为了 4
        cursor.execute("PRAGMA user_version")
        version = cursor.fetchone()[0]
        self.assertEqual(version, 4)

        # 检查旧表 tasks_v2 确实已被物理删除 (或者不存在)
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks_v2'"
        )
        self.assertIsNone(cursor.fetchone())

        # 检查新表 jobs 确实已被创建
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'"
        )
        self.assertIsNotNone(cursor.fetchone())

        # 检查核心索引是否在 jobs 表中以新命名模式 idx_jobs_... 成功创建
        cursor.execute("PRAGMA index_list('jobs')")
        indexes = [row[1] for row in cursor.fetchall()]
        self.assertIn("idx_jobs_status_number", indexes)
        self.assertIn("idx_jobs_number", indexes)
        self.assertIn("idx_jobs_workflow_id", indexes)

    def test_get_task_summaries(self):
        """测试 get_task_summaries 能够正确返回 TaskSummary，且字段匹配正确。"""
        # 写入测试数据
        task1 = Task(
            number=1.0,
            prompt_id="prompt-1",
            prompt=RawJSON('{"foo": "bar"}'),
            extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "workflow-1"}}}'),
            outputs_to_execute=["output-1"],
            create_time=1000,
        )
        task2 = Task(
            number=2.0,
            prompt_id="prompt-2",
            prompt=RawJSON('{"hello": "world"}'),
            extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "workflow-2"}}}'),
            outputs_to_execute=["output-2"],
            create_time=2000,
        )

        self.queue.add_task(task1)
        self.queue.add_task(task2)

        # 验证默认的 get_tasks，确认字段检索依然完整
        tasks = self.queue.get_tasks()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0][1].prompt_id, "prompt-1")

        # 验证专用的 get_task_summaries
        summaries = self.queue.get_task_summaries()
        self.assertEqual(len(summaries), 2)

        # 检查返回列表的类型是否均为特定的 TaskSummary
        for summary in summaries:
            self.assertIsInstance(summary, TaskSummary)

        # 校验各个数据字段内容是否正确，确认 workflow_id 已提取出来且没有 extra_data 字段
        s1 = summaries[0]
        self.assertEqual(s1.number, 1.0)
        self.assertEqual(s1.prompt_id, "prompt-1")
        self.assertEqual(s1.status, TaskStatus.PENDING)
        self.assertEqual(s1.workflow_id, "workflow-1")
        self.assertFalse(hasattr(s1, "extra_data"))
        self.assertEqual(s1.create_time, 1000)

        # 验证支持通过 workflow_id 进行 SQL 精确过滤
        workflow_filter = TaskFilters(workflow_id="workflow-2")
        summaries_filtered = self.queue.get_task_summaries(filter_by=workflow_filter)
        self.assertEqual(len(summaries_filtered), 1)
        self.assertEqual(summaries_filtered[0].prompt_id, "prompt-2")

        # 验证排序机制 (DESC)
        summaries_desc = self.queue.get_task_summaries(desc=True)
        self.assertEqual(summaries_desc[0].prompt_id, "prompt-2")

        # 验证分页限制与偏移量
        summaries_page = self.queue.get_task_summaries(limit=1, offset=1)
        self.assertEqual(len(summaries_page), 1)
        self.assertEqual(summaries_page[0].prompt_id, "prompt-2")

    def test_performance_benchmark(self):
        """性能基准测试：在大体积 extra_data 负载下，验证 get_task_summaries 比 get_tasks 具有极其恐怖的性能领先优势。"""
        # 构造大体积的 extra_data (包含庞大的 workflow 节点数据，大概 200KB)
        big_workflow_data = {
            "extra_pnginfo": {
                "workflow": {
                    "id": "workflow-perf",
                    "nodes": {str(i): {"inputs": {"val": i}} for i in range(2000)},
                }
            }
        }
        big_extra_data_str = json.dumps(big_workflow_data)

        # 写入 500 个带超大 extra_data 负载的任务
        for i in range(500):
            task = Task(
                number=float(i),
                prompt_id=f"perf-task-{i}",
                prompt=RawJSON('{"val": 1}'),
                extra_data=RawJSON(big_extra_data_str),
                outputs_to_execute=[f"out-{i}"],
                create_time=int(time.time()),
            )
            self.queue.add_task(task)

        # 1. 运行原有 get_tasks (检索包括巨大的 extra_data 字段以及 outputs 反序列化)
        t_start = time.perf_counter()
        tasks = self.queue.get_tasks(limit=200)
        t_get_tasks = (time.perf_counter() - t_start) * 1000

        # 2. 运行优化后的 get_task_summaries (完全舍弃了 extra_data，只返回已编好索引的元数据，不需反序列化)
        t_start = time.perf_counter()
        summaries = self.queue.get_task_summaries(limit=200)
        t_summaries = (time.perf_counter() - t_start) * 1000

        print(f"\n[Performance Benchmark with massive extra_data (500 tasks, limit 200)]")
        print(f"Original get_tasks: {t_get_tasks:.2f} ms")
        print(f"Optimized get_task_summaries (Zero loads): {t_summaries:.2f} ms")

        self.assertEqual(len(tasks), 200)
        self.assertEqual(len(summaries), 200)

        # 耗时必须非常小 (远低于 500ms 且理论上由于去掉了 40MB extra_data 反序列化，应该低于 5ms)
        self.assertLess(t_summaries, 50.0)

    def test_idempotent_migration_on_power_loss(self):
        """测试在断电容灾场景下，重新跑升级迁移能够幂等处理冲突，不会抛出主键约束错误。"""
        import tempfile

        # 创建一个临时的物理数据库文件来模拟文件级升级
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "test_power_loss.db")

            # 1. 模拟断电前版本 3 状态
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()

            # 创建 version 3 的 tasks_v2 表并写入两条记录
            cursor.execute("""
                CREATE TABLE tasks_v2 (
                    id TEXT PRIMARY KEY,
                    number INTEGER,
                    prompt TEXT,
                    extra_data TEXT,
                    outputs_to_execute TEXT,
                    status TEXT,
                    create_time INTEGER
                )
            """)
            cursor.execute(
                "INSERT INTO tasks_v2 VALUES ('t1', 1, '{}', '{\"extra_pnginfo\": {\"workflow\": {\"id\": \"w1\"}}}', '[]', 'pending', 100)"
            )
            cursor.execute(
                "INSERT INTO tasks_v2 VALUES ('t2', 2, '{}', '{\"extra_pnginfo\": {\"workflow\": {\"id\": \"w2\"}}}', '[]', 'pending', 200)"
            )

            # 模拟“中途断电导致新表 jobs 已经建好且写入了第一条记录('t1')，但 tasks_v2 依然存在且 user_version 依然是 3”的状态
            cursor.execute("""
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    number INTEGER,
                    prompt TEXT,
                    extra_data TEXT,
                    workflow_id TEXT,
                    outputs_to_execute TEXT,
                    status TEXT,
                    create_time INTEGER
                )
            """)
            # 模拟已迁移过的 't1' 记录
            cursor.execute(
                "INSERT INTO jobs VALUES ('t1', 1, '{}', '{\"extra_pnginfo\": {\"workflow\": {\"id\": \"w1\"}}}', 'w1', '[]', 'pending', 100)"
            )
            # 设置 user_version 仍为 3
            cursor.execute("PRAGMA user_version = 3")
            conn.commit()
            conn.close()

            # 2. 启动 SQLiteQueue 加载该数据库，这会触发 v3 -> v4 的幂等升级
            # 如果不具备幂等性（例如使用普通 INSERT），在写入 't1' 时会抛出 UNIQUE 约束错误而崩溃。
            # 这里应无冲突地正常运行通过并完成升级！
            try:
                queue = SQLiteQueue(db_file)
            except Exception as e:
                self.fail(f"SQLiteQueue migration crashed under power-loss state: {e}")

            # 3. 验证升级是否最终成功，且旧表已被删除，新表数据完整
            cursor = queue._conn.cursor()
            cursor.execute("PRAGMA user_version")
            self.assertEqual(cursor.fetchone()[0], 4)

            # tasks_v2 应该被 DROP 掉了
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks_v2'"
            )
            self.assertIsNone(cursor.fetchone())

            # 验证新表中的数据，'t1' 和 't2' 应该都完美无缺
            cursor.execute("SELECT id, workflow_id FROM jobs ORDER BY number")
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], ("t1", "w1"))
            self.assertEqual(rows[1], ("t2", "w2"))

            queue.close()


if __name__ == "__main__":
    unittest.main()
