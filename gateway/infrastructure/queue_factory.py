import os
import json
import random
import logging
from typing import Tuple, Callable

from gateway.config import GatewayConfig, BASE_DIR
from gateway.shared.interfaces import JobQueueReader, JobQueueWriter
from .sqlite.queue import SQLiteQueue
from .file.json_queue import JSONFileQueue

logger = logging.getLogger(__name__)


def init_queue(
    config: GatewayConfig,
) -> Tuple[JobQueueReader, JobQueueWriter, Callable[[], None]]:
    """根据配置初始化 JobQueue，返回 (reader, writer, close_fn)。

    close_fn 用于释放队列占用的底层资源，由构建方在关闭时调用。
    """
    os.makedirs(config.data_dir, exist_ok=True)

    if config.queue_type == "json":
        logger.info("💾 Using JSONFileQueue.")
        queue_instance = JSONFileQueue(os.path.join(config.data_dir, "queue.json"))
    else:
        if config.queue_type != "sqlite":
            logger.warning(
                f"⚠️ Unknown queue type '{config.queue_type}'. Defaulting to 'sqlite'."
            )
        db_path = os.path.join(config.data_dir, "queue.db")
        logger.info(f"🗃️ Using SQLiteQueue. DB path: {db_path}")
        queue_instance = SQLiteQueue(
            db_path, history_retention_days=config.history_retention_days
        )

    # 检查并迁移旧根目录下的 queue.json
    old_json_path = os.path.join(BASE_DIR, "queue.json")
    if os.path.exists(old_json_path):
        logger.info(f"📦 Found legacy queue file {old_json_path}. Migrating...")
        try:
            legacy_queue = JSONFileQueue(old_json_path)
            for job in legacy_queue.list():
                queue_instance.add(job)
            legacy_queue.close()

            suffix = "".join(random.choices("0123456789abcdef", k=8))
            bak_path = f"{old_json_path}~{suffix}"
            os.rename(old_json_path, bak_path)
            logger.info(
                f"✅ Migration successful! Legacy queue file renamed to {bak_path}"
            )
        except (OSError, json.JSONDecodeError, ValueError) as e:
            logger.error(f"❌ Migration failed: {e}")

    return queue_instance, queue_instance, queue_instance.close
