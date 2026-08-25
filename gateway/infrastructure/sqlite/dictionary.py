"""队列大文本列的编解码门面与字典状态管理。

字典实体存放在独立的 dictionaries 表（指纹即主键，内容不可变），
metadata 表仅保存活跃指针与训练基线两个标量。对队列暴露的完整接口：
- encode_cell / decode_cell：大文本列与自描述压缩 blob 的双向转换；
- train_and_activate：用调用方给出的样本训练并激活下一代字典（迁移引导用）；
- rotate_if_due：完成事件检查点——进度差攒满一个周期才自行采样训练；
- active_ref / active_column_ref / sample_count：迁移重写打列值、
  组装采样查询所需的只读事实。

轮换进度取自 metadata 的 next_task_number（由队列的 new_number 维护，
本模块只读）：以"上次训练时的任务号快照"为基线，差值达到阈值即触发，
因此只统计真正新增的任务，且重启不丢周期。

注册表加载是内部按需行为：首次触达任何读状态的路径时自动执行，
调用方没有"先加载再使用"的排序约束。
标识采用内容指纹而非序号：没有计数上界；同内容重训天然去重，
指纹冲突时放弃新字典即可无损自愈。
"""

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from gateway.infrastructure.sqlite.compression import (
    CompressionBackend,
    decompress_payload,
    dict_fingerprint,
    pack_blob,
    unpack_blob,
)

logger = logging.getLogger(__name__)

DICT_ACTIVE_KEY = "dict_active"
# 值为任务号（非时间）：记录训练推进到的位置，与 next_task_number 配对度量轮换进度
DICT_TRAINED_UNTIL_KEY = "dict_trained_until"
# 队列侧 new_number 维护的任务号分配器键名；本模块只读它来度量轮换进度
_NEXT_TASK_NUMBER_KEY = "next_task_number"


class DictionaryStore:
    """dictionaries 表的字典注册表，兼作大文本列的编解码边界。"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        backend: CompressionBackend,
        *,
        rotation_interval: int,
        sample_count: int,
        target_size: int,
    ):
        self._conn = conn
        self._backend = backend
        self._rotation_interval = rotation_interval
        self._sample_count = sample_count
        self._target_size = target_size
        # 指纹 -> 字典内容；活跃指纹 None 表示当前处于无字典的基础档
        self._dicts: Dict[int, bytes] = {}
        self._active_ref: Optional[int] = None
        # 上次训练推进到的任务号（轮换基线；单位是任务号，不是时间）
        self._trained_until_task = 0
        self._loaded = False

    # ---- 大文本列的编解码边界（队列唯一的使用方式） ----

    def encode_cell(self, value: Optional[str]) -> Optional[bytes]:
        """写入边界：文本 -> 自描述压缩 blob（None 保持为 NULL）。

        行头的 0 是 wire 格式的"无字典"哨兵，仅在打包边界出现。
        """
        if value is None:
            return None
        self._ensure_loaded()
        content = (
            self._dicts.get(self._active_ref) if self._active_ref is not None else None
        )
        payload = self._backend.compress(value.encode("utf-8"), content)
        header_ref = 0 if self._active_ref is None else self._active_ref
        return pack_blob(self._backend.codec_id, header_ref, payload)

    def decode_cell(self, raw: Any) -> Optional[str]:
        """读取边界：按行头分发解压还原文本。

        TEXT 值仅存在于迁移中断的过渡态，原样透传。
        """
        if raw is None:
            return None
        if isinstance(raw, str):
            return raw
        self._ensure_loaded()
        codec_id, dict_ref, payload = unpack_blob(raw)
        dictionary = self._dicts.get(dict_ref) if dict_ref else None
        if dict_ref and dictionary is None:
            raise RuntimeError(
                f"行引用了未知的字典 {dict_ref}，数据库可能不完整或已损坏"
            )
        # 自有编码优先走注入的后端（支持自定义后端）；其余交给内置编解码注册表，
        # 保证换后端/卸载可选依赖后旧行依旧可读
        if codec_id == self._backend.codec_id:
            return self._backend.decompress(payload, dictionary).decode("utf-8")
        return decompress_payload(codec_id, payload, dictionary).decode("utf-8")

    # ---- 轮换周期（完成事件检查点） ----

    def rotate_if_due(self) -> None:
        """完成事件检查点：任务号进度差攒满一个周期才尝试训练下一代字典。

        基线推进只发生在 train_and_activate 内部（与指针同事务）或
        无可采样内容时的单独推进，避免每个完成事件都重试训练。
        """
        if not self._backend.supports_dicts:
            return
        self._ensure_loaded()
        progress = self._read_progress()
        if progress - self._trained_until_task < self._rotation_interval:
            return
        samples = self._collect_recent_samples()
        if not samples:
            # 无可采样内容也推进基线，防止后续每个完成事件都空转重试
            self._mark_trained(progress)
            return
        self.train_and_activate(samples)
        self._gc_unreferenced()

    # ---- 显式训练入口（迁移引导与轮换检查点共用） ----

    def train_and_activate(self, samples: List[bytes]) -> None:
        """用调用方给出的样本训练并激活下一代字典。

        - 后端无法产出（样本不足/退化）：保持现状，仅单独推进训练基线；
        - 内容与已存字典相同：天然去重，仅把活跃指针指过去；
        - 指纹相同但内容不同（真碰撞）：放弃本轮、保持现状——无损且自愈，
          待样本窗口漂移后重新训练大概率换指纹。
        字典内容一经写入永不覆盖；成功路径下基线与活跃指针同事务落盘。
        """
        self._ensure_loaded()
        trained_until = self._read_progress()
        token = self._backend.train(samples, self._target_size)
        if token is None:
            # 样本不足/退化：仅推进基线（指针保持旧档），下个周期重试
            logger.warning(
                f"⚠️ 本轮未能产出新字典（样本不足或退化），"
                f"训练基线推进到任务号 {trained_until}，待下个周期重试。"
            )
            self._mark_trained(trained_until)
            return
        dict_ref = dict_fingerprint(token)
        existing = self._dicts.get(dict_ref)
        if existing is not None and existing != token:
            # 指纹碰撞：放弃本轮（指针保持旧档），无损且自愈
            logger.warning(
                f"⚠️ 新训练字典与已存字典 {dict_ref} 指纹冲突但内容不同，"
                "放弃本轮以避免覆盖，待下个周期重试。"
            )
            self._mark_trained(trained_until)
            return

        cursor = self._conn.cursor()
        if existing is None:
            # 字典内容一经写入永不覆盖（不可变红线）；主键即指纹，
            # 主键冲突说明状态被破坏，直接失败
            cursor.execute(
                "INSERT INTO dictionaries (dict_ref, content) VALUES (?, ?)",
                (dict_ref, token),
            )
            self._dicts[dict_ref] = token
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (DICT_ACTIVE_KEY, f"{dict_ref}"),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (DICT_TRAINED_UNTIL_KEY, str(trained_until)),
        )
        self._conn.commit()
        self._active_ref = dict_ref
        self._trained_until_task = trained_until
        if existing is not None:
            logger.info(f"🧠 Retrained dictionary matches stored {dict_ref}; reused.")
        else:
            logger.info(f"🧠 Trained compression dictionary {dict_ref}.")

    # ---- 仅迁移重写需要：把活跃指纹打到行的 dict_ref 列上 ----

    @property
    def active_ref(self) -> Optional[int]:
        """当前活跃字典指纹；None 表示处于无字典的基础档。

        迁移重写把它打到 jobs.dict_ref 列（外键语义，None = 无字典引用）。
        """
        self._ensure_loaded()
        return self._active_ref

    @property
    def sample_count(self) -> int:
        """训练样本窗口上限，供调用方组装采样查询使用。"""
        return self._sample_count

    # ---- 内部实现 ----

    def _read_progress(self) -> int:
        """读取队列的任务号分配进度。"""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT value FROM metadata WHERE key = ?", (_NEXT_TASK_NUMBER_KEY,)
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def _mark_trained(self, trained_until: int) -> None:
        """把训练基线推进到指定任务号并持久化。"""
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (DICT_TRAINED_UNTIL_KEY, str(trained_until)),
        )
        self._conn.commit()
        self._trained_until_task = trained_until

    def _ensure_loaded(self) -> None:
        """按需加载注册表与训练基线；幂等，进程内首次触达执行一次。"""
        if self._loaded:
            return
        cursor = self._conn.cursor()
        cursor.execute("SELECT dict_ref, content FROM dictionaries")
        dicts: Dict[int, bytes] = {}
        for dict_ref, content in cursor.fetchall():
            # 字典内容只可能以 BLOB 写入；其他类型说明数据被外部改动
            if not isinstance(content, bytes):
                raise RuntimeError(f"字典 {dict_ref} 的内容类型异常，数据库可能已损坏")
            dicts[dict_ref] = content
        cursor.execute("SELECT value FROM metadata WHERE key = ?", (DICT_ACTIVE_KEY,))
        row = cursor.fetchone()
        active_ref = int(row[0]) if row else None
        if active_ref is not None and active_ref not in dicts:
            raise RuntimeError(
                f"活跃字典 {active_ref} 缺失对应的字典内容，数据库可能不完整"
            )
        cursor.execute(
            "SELECT value FROM metadata WHERE key = ?", (DICT_TRAINED_UNTIL_KEY,)
        )
        row = cursor.fetchone()
        self._dicts = dicts
        self._active_ref = active_ref
        self._trained_until_task = int(row[0]) if row else 0
        self._loaded = True

    def _collect_recent_samples(self) -> List[bytes]:
        """取最近的任务大文本作为训练样本窗口。"""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT prompt, extra_data FROM jobs ORDER BY number DESC LIMIT ?",
            (self._sample_count * 2,),
        )
        samples: List[bytes] = []
        for prompt, extra_data in cursor.fetchall():
            for cell in (prompt, extra_data):
                if len(samples) >= self._sample_count:
                    return samples
                text = self.decode_cell(cell)
                if text:
                    samples.append(text.encode("utf-8"))
        return samples

    def _gc_unreferenced(self) -> None:
        """物理删除不再被任何行引用、且非当前活跃指纹的旧字典内容。

        存活集合通过 idx_jobs_dict_ref 覆盖索引一次取回（不触表页），
        与其做逐候选的存在性探测，不如一次拿全再按集合差删除。
        即便误删仍被引用的行，jobs 的外键约束也会拒绝（防护网）。
        """
        cursor = self._conn.cursor()
        cursor.execute("SELECT DISTINCT dict_ref FROM jobs")
        referenced = {row[0] for row in cursor.fetchall()}
        stale = [
            r for r in self._dicts if r != self._active_ref and r not in referenced
        ]
        for dict_ref in stale:
            cursor.execute("DELETE FROM dictionaries WHERE dict_ref = ?", (dict_ref,))
            del self._dicts[dict_ref]
            logger.info(f"♻️ Recycled unreferenced compression dictionary {dict_ref}.")
        self._conn.commit()
