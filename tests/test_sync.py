import asyncio
import unittest
from unittest.mock import MagicMock

from gateway.application.sync import JobDownstreamSyncer
from gateway.shared.interfaces import (
    DownstreamClient,
    EventBus,
    JobQueueReader,
    JobQueueWriter,
)
from gateway.shared.models import Job, JobStatus, RawJSON


def _make_job(prompt_id: str) -> Job:
    """构造一个正在运行的本地任务，其执行反馈字段均为空（尚未同步过）。"""
    return Job(
        number=1,
        prompt_id=prompt_id,
        prompt=RawJSON("{}"),
        extra_data=RawJSON("{}"),
        outputs_to_execute=[],
        create_time=1,
        status=JobStatus.RUNNING,
    )


def _make_syncer(task: Job):
    reader = MagicMock(spec=JobQueueReader)
    reader.get.return_value = task
    writer = MagicMock(spec=JobQueueWriter)
    downstream = MagicMock(spec=DownstreamClient)
    downstream.downstream_ready = True
    event_bus = MagicMock(spec=EventBus)
    syncer = JobDownstreamSyncer(
        queue_reader=reader,
        queue_writer=writer,
        downstream=downstream,
        event_bus=event_bus,
    )
    return syncer, writer


class TestSyncOutputSummaryLog(unittest.TestCase):
    """测试任务完成同步落盘时输出结果摘要日志的行为。"""

    def test_completed_sync_logs_first_image_relpath_and_total(self):
        """任务完成后同步时应记录首图相对路径与图片总数。"""
        task = _make_job("prompt-1")
        syncer, writer = _make_syncer(task)
        ds_job = {
            "id": "prompt-1",
            "status": "completed",
            "outputs": {
                "12": {
                    "images": [
                        {
                            "filename": "20260723_134155_00001_.png",
                            "subfolder": "TODO",
                            "type": "output",
                        },
                        {
                            "filename": "20260723_134155_00002_.png",
                            "subfolder": "TODO",
                            "type": "output",
                        },
                    ]
                }
            },
            "execution_end_time": 123.0,
        }

        with self.assertLogs("gateway.application.sync", level="INFO") as captured:
            asyncio.run(syncer._sync_single_job_data("prompt-1", [], ds_job))

        joined = "\n".join(captured.output)
        self.assertIn("TODO/20260723_134155_00001_.png", joined)
        self.assertIn("2 image(s)", joined)
        writer.save.assert_called_once()

    def test_running_sync_without_outputs_does_not_log_summary(self):
        """运行中任务的普通同步不应产生输出摘要日志。"""
        task = _make_job("prompt-1")
        syncer, _ = _make_syncer(task)
        ds_job = {
            "id": "prompt-1",
            "status": "in_progress",
            "execution_start_time": 100.0,
        }

        with self.assertLogs("gateway.application.sync", level="INFO") as captured:
            asyncio.run(syncer._sync_single_job_data("prompt-1", [], ds_job))

        self.assertFalse(any("image(s)" in line for line in captured.output))

    def test_completed_sync_without_outputs_does_not_log_summary(self):
        """完成但无 outputs 数据时不应产生输出摘要日志。"""
        task = _make_job("prompt-1")
        syncer, _ = _make_syncer(task)
        ds_job = {
            "id": "prompt-1",
            "status": "completed",
            "outputs": {},
            "execution_end_time": 123.0,
        }

        with self.assertLogs("gateway.application.sync", level="INFO") as captured:
            asyncio.run(syncer._sync_single_job_data("prompt-1", [], ds_job))

        self.assertFalse(any("image(s)" in line for line in captured.output))


if __name__ == "__main__":
    unittest.main()
