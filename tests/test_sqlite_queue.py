import unittest
import hashlib
import sqlite3
import os
import time
import json
from typing import List, Optional
from gateway.infrastructure.sqlite.queue import SQLiteQueue
from gateway.infrastructure.sqlite.compression import (
    ZlibBackend,
    dict_fingerprint,
)
from gateway.shared.models import Job, RawJSON, JobStatus, JobFilters, JobSummary


class TestSQLiteQueue(unittest.TestCase):
    """测试 SQLiteQueue 的正确性以及性能指标。"""

    def setUp(self):
        # 使用内存数据库进行测试，保证测试隔离性与运行速度
        self.db_path = ":memory:"
        self.queue = SQLiteQueue(self.db_path, backend=ZlibBackend())

    def tearDown(self):
        self.queue.close()

    def test_database_migration_and_indexes(self):
        """测试数据库版本迁移以及索引是否成功创建。"""
        conn = self.queue._conn
        cursor = conn.cursor()

        # 检查 user_version 是否确实升级为了 6（大文本列压缩存储版本）
        cursor.execute("PRAGMA user_version")
        version = cursor.fetchone()[0]
        self.assertEqual(version, 6)

        # WAL 高水位上限：防止一次全库重建/VACUUM 留下上百 MB 常驻残留文件
        cursor.execute("PRAGMA journal_size_limit")
        self.assertEqual(cursor.fetchone()[0], 16 * 1024 * 1024)

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
        # 字典引用回收依赖该覆盖索引，避免轮换期全表扫描
        self.assertIn("idx_jobs_dict_ref", indexes)

    def test_get_summaries(self):
        """测试 get_summaries 能够正确返回 TaskSummary，且字段匹配正确。"""
        # 写入测试数据
        job1 = Job(
            number=1.0,
            prompt_id="prompt-1",
            prompt=RawJSON('{"foo": "bar"}'),
            extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "workflow-1"}}}'),
            outputs_to_execute=["output-1"],
            create_time=1000,
        )
        job2 = Job(
            number=2.0,
            prompt_id="prompt-2",
            prompt=RawJSON('{"hello": "world"}'),
            extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "workflow-2"}}}'),
            outputs_to_execute=["output-2"],
            create_time=2000,
        )

        self.queue.add(job1)
        self.queue.add(job2)

        # 验证默认的 list，确认字段检索依然完整
        jobs = self.queue.list()
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].prompt_id, "prompt-1")

        # 验证专用的 get_summaries
        summaries = self.queue.get_summaries()
        self.assertEqual(len(summaries), 2)

        # 检查返回列表的类型是否均为特定的 TaskSummary
        for summary in summaries:
            self.assertIsInstance(summary, JobSummary)

        # 校验各个数据字段内容是否正确，确认 workflow_id 已提取出来且没有 extra_data 字段
        s1 = summaries[0]
        self.assertEqual(s1.number, 1.0)
        self.assertEqual(s1.prompt_id, "prompt-1")
        self.assertEqual(s1.status, JobStatus.PENDING)
        self.assertEqual(s1.workflow_id, "workflow-1")
        # 摘要模型不携带大体积的 extra_data：列表查询不得拉取该列
        self.assertNotIn("extra_data", JobSummary.__dataclass_fields__)
        self.assertEqual(s1.create_time, 1000)

        # 验证支持通过 workflow_id 进行 SQL 精确过滤
        workflow_filter = JobFilters(workflow_id="workflow-2")
        summaries_filtered = self.queue.get_summaries(filter_by=workflow_filter)
        self.assertEqual(len(summaries_filtered), 1)
        self.assertEqual(summaries_filtered[0].prompt_id, "prompt-2")

        # 验证排序机制 (DESC)
        summaries_desc = self.queue.get_summaries(desc=True)
        self.assertEqual(summaries_desc[0].prompt_id, "prompt-2")

        # 验证分页限制与偏移量
        summaries_page = self.queue.get_summaries(limit=1, offset=1)
        self.assertEqual(len(summaries_page), 1)
        self.assertEqual(summaries_page[0].prompt_id, "prompt-2")

    def test_performance_benchmark(self):
        """性能基准测试：在大体积 extra_data 负载下，验证 get_summaries 比 list 具有极其恐怖的性能领先优势。"""
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
            job = Job(
                number=float(i),
                prompt_id=f"perf-job-{i}",
                prompt=RawJSON('{"val": 1}'),
                extra_data=RawJSON(big_extra_data_str),
                outputs_to_execute=[f"out-{i}"],
                create_time=int(time.time()),
            )
            self.queue.add(job)

        # 1. 运行原有 list (检索包括巨大的 extra_data 字段以及 outputs 反序列化)
        t_start = time.perf_counter()
        job = self.queue.list(limit=200)
        t_list = (time.perf_counter() - t_start) * 1000

        # 2. 运行优化后的 get_summaries (完全舍弃了 extra_data，只返回已编好索引的元数据，不需反序列化)
        t_start = time.perf_counter()
        summaries = self.queue.get_summaries(limit=200)
        t_summaries = (time.perf_counter() - t_start) * 1000

        print(f"\n[Performance Benchmark with massive extra_data (500 tasks, limit 200)]")
        print(f"Original list: {t_list:.2f} ms")
        print(f"Optimized get_summaries (Zero loads): {t_summaries:.2f} ms")

        self.assertEqual(len(job), 200)
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
            # （真实的 v3 库必然经过 v2 迁移链路，因此带有 metadata 表）
            cursor.execute("""
                CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)
            """)
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
                queue = SQLiteQueue(db_file, backend=ZlibBackend())
            except Exception as e:
                self.fail(f"SQLiteQueue migration crashed under power-loss state: {e}")

            # 断言失败也要关闭连接，否则临时目录清理会因文件占用而误报
            try:
                # 3. 验证升级是否最终成功，且旧表已被删除，新表数据完整
                cursor = queue._conn.cursor()
                cursor.execute("PRAGMA user_version")
                self.assertEqual(cursor.fetchone()[0], 6)

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
            finally:
                queue.close()


    def test_v5_to_v6_migration_preserves_data(self):
        """v5 旧库升级到 v6 后，大文本列被压缩存储且所有旧任务完整可读；重复升级幂等。"""
        import tempfile

        big_extra = json.dumps(
            {
                "extra_pnginfo": {"workflow": {"id": "w-big"}},
                "nodes": [{"inputs": {"v": i}} for i in range(300)],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "legacy_v5.db")

            # 1. 手工搭建 v5 状态的库：jobs 表无 dict_ref 列，文本以 TEXT 存储
            # （真实的 v5 库必然经过 v2 迁移链路，因此带有 metadata 表）
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)"
            )
            cursor.execute("""
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    number INTEGER,
                    prompt TEXT,
                    extra_data TEXT,
                    workflow_id TEXT,
                    outputs_to_execute TEXT,
                    status TEXT,
                    create_time INTEGER,
                    outputs TEXT,
                    preview_output TEXT,
                    execution_start_time REAL,
                    execution_end_time REAL,
                    execution_error TEXT
                )
            """)
            cursor.execute(
                "INSERT INTO jobs VALUES ('t1', 1, '{}', ?, 'w-big', '[]', 'completed', 100, NULL, NULL, NULL, NULL, NULL)",
                (big_extra,),
            )
            cursor.execute("PRAGMA user_version = 5")
            conn.commit()
            conn.close()

            # 断言失败也要关闭连接，否则临时目录清理会因文件占用而误报
            queues: list[SQLiteQueue] = []

            def open_queue(path: str) -> SQLiteQueue:
                q = SQLiteQueue(path, backend=ZlibBackend())
                queues.append(q)
                return q

            try:
                # 2. 打开旧库触发 v5 -> v6 迁移
                queue = open_queue(db_file)
                cursor = queue._conn.cursor()
                cursor.execute("PRAGMA user_version")
                self.assertEqual(cursor.fetchone()[0], 6)

                # 3. 迁移后的行在公共接口上完整还原
                job = queue.get("t1")
                self.assertIsNotNone(job)
                self.assertEqual(job.prompt, RawJSON("{}"))
                self.assertEqual(json.loads(str(job.extra_data)), json.loads(big_extra))

                # 4. 存储契约：大文本列已变为压缩 BLOB（dict_ref 为 NULL 表示无字典）
                cursor.execute(
                    "SELECT typeof(prompt), dict_ref FROM jobs WHERE id = 't1'"
                )
                stored_type, dict_ref = cursor.fetchone()
                self.assertEqual(stored_type, "blob")
                self.assertIsNone(dict_ref)

                # 5. 幂等：再次打开不重写、不报错，数据依旧完整
                queue2 = open_queue(db_file)
                job2 = queue2.get("t1")
                self.assertEqual(json.loads(str(job2.extra_data)), json.loads(big_extra))
            finally:
                for q in queues:
                    q.close()

    def test_new_writes_stored_compressed(self):
        """新写入的任务大文本列以压缩 BLOB 落库，读取端透明还原。"""
        big_prompt = json.dumps({"nodes": [{"v": i} for i in range(500)]})
        job = Job(
            number=1.0,
            prompt_id="compress-1",
            prompt=RawJSON(big_prompt),
            extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "w1"}}}'),
            outputs_to_execute=["out-1"],
            create_time=1000,
        )
        self.queue.add(job)

        cursor = self.queue._conn.cursor()
        cursor.execute(
            "SELECT typeof(prompt), dict_ref, length(prompt) FROM jobs WHERE id = 'compress-1'"
        )
        stored_type, dict_ref, blob_len = cursor.fetchone()
        self.assertEqual(stored_type, "blob")
        self.assertIsNone(dict_ref)
        # 压缩后的体积必须显著小于原始文本（重复性高的 JSON 压缩率远好于 50%）
        self.assertLess(blob_len, len(big_prompt) // 2)

        fetched = self.queue.get("compress-1")
        self.assertEqual(fetched.prompt, RawJSON(big_prompt))
        self.assertEqual(fetched.outputs_to_execute, ["out-1"])


class FakeDictBackend:
    """确定性假字典后端：用标记字节模拟"有/无字典"两种编码，验证轮换装配。

    与真实 zstd 后端保持同一契约：样本不足时训练必须返回 None（无可产出字典）；
    另支持让前 N 次训练失败，用于验证失败停留当前档位并可重试。
    """

    codec_id = 9
    supports_dicts = True
    _MIN_TRAIN_SAMPLES = 2

    def __init__(
        self,
        fail_first_n_trains: int = 0,
        scripted_tokens: Optional[List[bytes]] = None,
    ):
        self.fail_first_n_trains = fail_first_n_trains
        self.scripted_tokens = list(scripted_tokens or [])
        self.attempts = 0
        self._success_count = 0
        self.trained_sample_sets: List[List[bytes]] = []

    def compress(self, data: bytes, dictionary) -> bytes:
        return (b"D" if dictionary else b"-") + data

    def decompress(self, payload: bytes, dictionary) -> bytes:
        expected = b"D" if dictionary else b"-"
        if not payload.startswith(expected):
            raise ValueError(f"fake codec 标记不符: 期望 {expected!r}")
        return payload[1:]

    def train(self, samples, target_size):
        if len(samples) < self._MIN_TRAIN_SAMPLES:
            return None
        self.attempts += 1
        if self.attempts <= self.fail_first_n_trains:
            return None
        if self.scripted_tokens:
            index = min(self.attempts - 1, len(self.scripted_tokens) - 1)
            return self.scripted_tokens[index]
        # 仅成功产出计入编号，失败尝试不占用字典命名
        self._success_count += 1
        self.trained_sample_sets.append(list(samples))
        return f"dict#{self._success_count}".encode()


def _sha256_prefix4(data: bytes) -> int:
    return int.from_bytes(hashlib.sha256(data).digest()[:4], "big")


def _build_fingerprint_collision_pair() -> tuple[bytes, bytes]:
    """用生日法构造一对前 4 字节指纹相同、内容不同的字典。

    对固定 base 找碰撞是 2^32 量级的原像搜索（不可行）；
    两边都自由时按生日界只需约 2^16 次哈希。
    """
    seen: dict[int, bytes] = {}
    index = 0
    while True:
        candidate = f"collision-candidate-{index}".encode()
        fp = _sha256_prefix4(candidate)
        if fp in seen and seen[fp] != candidate:
            return seen[fp], candidate
        seen[fp] = candidate
        index += 1


class TestDictionaryRotation(unittest.TestCase):
    """测试字典轮换生命周期：计数触发、指针切换、旧版保留与引用归零回收。"""

    def _make_job(self, number: float, job_id: str) -> Job:
        return Job(
            number=number,
            prompt_id=job_id,
            prompt=RawJSON(json.dumps({"n": number})),
            extra_data=RawJSON(
                json.dumps({"extra_pnginfo": {"workflow": {"id": f"w-{job_id}"}}})
            ),
            outputs_to_execute=[f"out-{job_id}"],
            # 使用当前时间，避免任务被保留期清理误删
            create_time=int(time.time() * 1000),
        )

    def _add_job(self, queue: SQLiteQueue, job_id: str) -> None:
        """经队列分配真实任务号后入队：轮换进度依赖 next_task_number 前进。"""
        number = float(queue.new_number())
        queue.add(self._make_job(number, job_id))
    def test_rotation_lifecycle(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "rotation.db")
            backend = FakeDictBackend()
            # 轮换阈值调小到 2，便于在测试中跨越多轮
            queue = SQLiteQueue(
                db_file, backend=backend, rotation_interval=2, history_retention_days=90
            )
            cursor = queue._conn.cursor()

            def meta_keys() -> list[str]:
                cursor.execute(
                    "SELECT key FROM metadata WHERE key LIKE 'dict%' ORDER BY key"
                )
                return [r[0] for r in cursor.fetchall()]

            def table_refs() -> list[int]:
                cursor.execute(
                    "SELECT dict_ref FROM dictionaries ORDER BY dict_ref"
                )
                return [r[0] for r in cursor.fetchall()]

            def dict_ref_of(job_id: str):
                cursor.execute(
                    "SELECT dict_ref FROM jobs WHERE id = ?", (job_id,)
                )
                return cursor.fetchone()[0]

            fp1 = dict_fingerprint(b"dict#1")
            fp2 = dict_fingerprint(b"dict#2")
            fp3 = dict_fingerprint(b"dict#3")

            try:
                # 第一轮：攒满 2 条后，完成事件触发训练并切换指针
                self._add_job(queue, "j1")
                self._add_job(queue, "j2")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j1")

                self.assertEqual(len(backend.trained_sample_sets), 1)
                samples = backend.trained_sample_sets[0]
                self.assertTrue(0 < len(samples) <= 300)
                self.assertEqual(
                    cursor.execute(
                        "SELECT value FROM metadata WHERE key = 'dict_active'"
                    ).fetchone()[0],
                    str(fp1),
                )
                stored = cursor.execute(
                    "SELECT content FROM dictionaries WHERE dict_ref = ?", (fp1,)
                ).fetchone()[0]
                self.assertEqual(
                    stored.encode() if isinstance(stored, str) else stored,
                    b"dict#1",
                )

                # 指针切换后的新行落在 v1 且可读；旧行保持无外键引用(NULL)且仍可读
                self._add_job(queue, "j3")
                self.assertEqual(dict_ref_of("j3"), fp1)
                self.assertIsNone(dict_ref_of("j1"))
                self.assertIsNotNone(queue.get("j3"))
                self.assertIsNotNone(queue.get("j1"))

                # 第二轮：v1 被 j3、j4 引用，训练 v2 后必须保留
                self._add_job(queue, "j4")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j2")
                self.assertIn(fp1, table_refs())
                self.assertIn(fp2, table_refs())

                # 回收：删除全部引用 v1 的行后，攒满周期触发下一轮训练并物理清除 v1
                # （j5/j6 落在 v2 上，因此 v2 有引用必须保留）
                queue.delete_pending(["j3", "j4"])
                self._add_job(queue, "j5")
                self._add_job(queue, "j6")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j5")
                self.assertNotIn(fp1, table_refs())
                self.assertIn(fp2, table_refs())
                self.assertIn(fp3, table_refs())
                self.assertEqual(dict_ref_of("j6"), fp2)

                # 三轮之后所有现存行依旧完整可读
                for job in queue.list():
                    self.assertEqual(job.outputs_to_execute, [f"out-{job.prompt_id}"])
            finally:
                queue.close()

    def test_rotation_survives_failed_training(self):
        """训练失败时停留在基础档且可重试：下一个完整周期的训练照常产出字典。"""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "rotation_retry.db")
            backend = FakeDictBackend(fail_first_n_trains=1)
            queue = SQLiteQueue(
                db_file, backend=backend, rotation_interval=2, history_retention_days=90
            )
            cursor = queue._conn.cursor()

            def dict_meta_keys() -> list[str]:
                cursor.execute(
                    "SELECT key FROM metadata WHERE key LIKE 'dict%' ORDER BY key"
                )
                return [r[0] for r in cursor.fetchall()]

            def stored_refs() -> list[int]:
                cursor.execute(
                    "SELECT dict_ref FROM dictionaries ORDER BY dict_ref"
                )
                return [r[0] for r in cursor.fetchall()]

            try:
                self._add_job(queue, "j1")
                self._add_job(queue, "j2")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j1")

                # 第一次训练失败：无字典落库，但训练基线照常推进（下个完整周期重试）
                self.assertEqual(backend.attempts, 1)
                self.assertEqual(dict_meta_keys(), ["dict_trained_until"])
                self.assertEqual(stored_refs(), [])
                self._add_job(queue, "j3")
                self.assertIsNotNone(queue.get("j3"))

                # 下一个完整周期重试成功
                self._add_job(queue, "j4")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j2")
                self.assertEqual(backend.attempts, 2)
                fp1 = dict_fingerprint(b"dict#1")
                self.assertEqual(dict_meta_keys(), ["dict_active", "dict_trained_until"])
                self.assertEqual(stored_refs(), [fp1])
                self.assertIsNone(cursor.execute(
                    "SELECT dict_ref FROM jobs WHERE id = 'j3'"
                ).fetchone()[0])
                self.assertIsNotNone(queue.get("j3"))
            finally:
                queue.close()

    def test_rotation_reuses_identical_and_skips_colliding_dictionary(self):
        """指纹语义：同内容重训直接复用；指纹冲突内容不同则放弃本轮且指针不动。"""
        import tempfile

        base_token, colliding_token = _build_fingerprint_collision_pair()
        self.assertEqual(
            _sha256_prefix4(base_token), _sha256_prefix4(colliding_token)
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "collision.db")
            backend = FakeDictBackend(scripted_tokens=[base_token, colliding_token])
            queue = SQLiteQueue(
                db_file, backend=backend, rotation_interval=2, history_retention_days=90
            )
            cursor = queue._conn.cursor()
            fp_base = dict_fingerprint(base_token)

            def stored_content(ref: int) -> bytes:
                stored = cursor.execute(
                    "SELECT content FROM dictionaries WHERE dict_ref = ?", (ref,)
                ).fetchone()[0]
                return stored.encode() if isinstance(stored, str) else stored

            try:
                # 第一轮：base 字典落库并激活，j3 落在其上
                self._add_job(queue, "j1")
                self._add_job(queue, "j2")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j1")
                cursor.execute(
                    "SELECT 1 FROM dictionaries WHERE dict_ref = ?", (fp_base,)
                )
                self.assertIsNotNone(cursor.fetchone())
                self._add_job(queue, "j3")
                self.assertEqual(
                    cursor.execute(
                        "SELECT dict_ref FROM jobs WHERE id = 'j3'"
                    ).fetchone()[0],
                    fp_base,
                )

                # 第二轮：训练出指纹相同但内容不同的字典 → 放弃本轮，
                # 已存内容不被覆盖、活跃指针不变、旧行照常可读
                self._add_job(queue, "j4")
                queue.update_status(JobStatus.COMPLETED, prompt_id="j2")
                self.assertEqual(stored_content(fp_base), base_token)
                self.assertEqual(
                    cursor.execute(
                        "SELECT value FROM metadata WHERE key = 'dict_active'"
                    ).fetchone()[0],
                    str(fp_base),
                )
                self._add_job(queue, "j5")
                self.assertIsNotNone(queue.get("j5"))
            finally:
                queue.close()


class TestV6MigrationStates(unittest.TestCase):
    """覆盖 v5->v6 迁移的三种起始状态：空库、有库、断电中途态。"""

    @staticmethod
    def _create_v5_jobs_table(cursor: sqlite3.Cursor) -> None:
        cursor.execute("""
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                number INTEGER,
                prompt TEXT,
                extra_data TEXT,
                workflow_id TEXT,
                outputs_to_execute TEXT,
                status TEXT,
                create_time INTEGER,
                outputs TEXT,
                preview_output TEXT,
                execution_start_time REAL,
                execution_end_time REAL,
                execution_error TEXT
            )
        """)

    def test_empty_legacy_db_lands_at_base_tier(self):
        """空 v5 库升级后直接可用，新任务写入读取正常。"""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "empty_v5.db")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
            self._create_v5_jobs_table(cursor)
            cursor.execute("PRAGMA user_version = 5")
            conn.commit()
            conn.close()

            queue = SQLiteQueue(db_file, backend=ZlibBackend())
            try:
                cursor = queue._conn.cursor()
                cursor.execute("PRAGMA user_version")
                self.assertEqual(cursor.fetchone()[0], 6)

                job = Job(
                    number=1.0,
                    prompt_id="first",
                    prompt=RawJSON('{"a": 1}'),
                    extra_data=RawJSON('{"extra_pnginfo": {"workflow": {"id": "w"}}}'),
                    outputs_to_execute=["o"],
                    create_time=1000,
                )
                queue.add(job)
                fetched = queue.get("first")
                self.assertIsNotNone(fetched)
                self.assertEqual(fetched.prompt, RawJSON('{"a": 1}'))
            finally:
                queue.close()

    def test_power_loss_mid_migration_mixed_rows_readable(self):
        """断电中途态（部分行已 BLOB、部分仍 TEXT）重新打开后完成迁移且全部可读。"""
        import tempfile
        import zlib as zlib_mod

        from gateway.infrastructure.sqlite.compression import (
            CODEC_ZLIB,
            pack_blob,
        )

        text_extra = json.dumps({"extra_pnginfo": {"workflow": {"id": "w-text"}}})
        blob_prompt_raw = json.dumps({"nodes": [{"v": i} for i in range(64)]})

        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = os.path.join(tmpdir, "mid_migration.db")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            # 断电时 ALTER 已执行（dict_ref 可空外键列 + dictionaries 表存在），
            # 但 user_version 仍为 5：t_blob 行已完成压缩重写，t_text 行还是原始 TEXT
            cursor.execute("""
                CREATE TABLE dictionaries (
                    dict_ref INTEGER PRIMARY KEY,
                    content  BLOB NOT NULL
                )
            """)
            cursor.execute("""
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    number INTEGER,
                    prompt TEXT,
                    extra_data TEXT,
                    workflow_id TEXT,
                    outputs_to_execute TEXT,
                    status TEXT,
                    create_time INTEGER,
                    outputs TEXT,
                    preview_output TEXT,
                    execution_start_time REAL,
                    execution_end_time REAL,
                    execution_error TEXT,
                    dict_ref INTEGER REFERENCES dictionaries(dict_ref)
                )
            """)
            blob_prompt = pack_blob(
                CODEC_ZLIB, 0, zlib_mod.compress(blob_prompt_raw.encode("utf-8"), 6)
            )
            # 单行 UPDATE 原子生效，断电不会产生"行内部分迁移"；
            # 已迁移行的三个大文本列必然全部是 BLOB
            blob_extra = pack_blob(CODEC_ZLIB, 0, zlib_mod.compress(b"{}", 6))
            blob_outputs = pack_blob(CODEC_ZLIB, 0, zlib_mod.compress(b"[]", 6))
            cursor.execute(
                "INSERT INTO jobs VALUES ('t_blob', 1, ?, ?, 'w-blob', ?, 'pending', 100, NULL, NULL, NULL, NULL, NULL, NULL)",
                (blob_prompt, blob_extra, blob_outputs),
            )
            cursor.execute(
                "INSERT INTO jobs VALUES ('t_text', 2, '{}', ?, 'w-text', '[]', 'pending', 200, NULL, NULL, NULL, NULL, NULL, NULL)",
                (text_extra,),
            )
            cursor.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT)")
            cursor.execute("PRAGMA user_version = 5")
            conn.commit()
            conn.close()

            queue = SQLiteQueue(db_file, backend=ZlibBackend())
            try:
                cursor = queue._conn.cursor()
                cursor.execute("PRAGMA user_version")
                self.assertEqual(cursor.fetchone()[0], 6)

                # 已迁移的行不被二次压缩，未迁移的行补齐；两行均完整可读
                blob_job = queue.get("t_blob")
                self.assertEqual(json.loads(str(blob_job.prompt)), json.loads(blob_prompt_raw))
                text_job = queue.get("t_text")
                self.assertEqual(json.loads(str(text_job.extra_data)), json.loads(text_extra))

                cursor.execute("SELECT typeof(prompt), typeof(extra_data) FROM jobs ORDER BY number")
                rows = cursor.fetchall()
                self.assertEqual(rows[0], ("blob", "blob"))
                self.assertEqual(rows[1], ("blob", "blob"))
            finally:
                queue.close()


if __name__ == "__main__":
    unittest.main()
