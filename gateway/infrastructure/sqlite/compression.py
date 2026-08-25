"""队列大文本列的压缩后端实现。

存储格式自描述：每个落库 blob 的头部携带编解码标识与字典指纹，
读取端按行头分发解压，保证跨后端、跨字典代际的行永远可读。
字典以内容指纹（SHA-256 截断）而非序号标识：没有计数上界，
内容相同的训练结果天然去重，指纹冲突时放弃新字典即可无损自愈。
"""

import hashlib
import logging
import zlib
from typing import ClassVar, Optional, Protocol, Sequence, Tuple

# 可选依赖：缺失时由组合根通过 select_backend 降级为基础档，而非在运行中途切换
try:
    import zstandard as _zstd
except ImportError:  # pragma: no cover - 取决于宿主环境是否安装可选依赖
    _zstd = None

logger = logging.getLogger(__name__)

CODEC_ZLIB = 1
CODEC_ZSTD = 2

# 头部布局：1 字节编解码标识 + 4 字节大端字典指纹（0 表示无字典）
_HEADER_LEN = 5
_DICT_REF_LEN = 4
_NO_DICT_REF = 0


def dict_fingerprint(dictionary: bytes) -> int:
    """计算字典内容的存储指纹：SHA-256 前 4 字节的大端整数。"""
    return int.from_bytes(hashlib.sha256(dictionary).digest()[:_DICT_REF_LEN], "big")


def pack_blob(codec_id: int, dict_ref: int, payload: bytes) -> bytes:
    """将头部信息与压缩载荷打包为落库 blob；dict_ref 为字典指纹，0 表示无字典。"""
    return bytes((codec_id,)) + dict_ref.to_bytes(_DICT_REF_LEN, "big") + payload


def unpack_blob(blob: bytes) -> Tuple[int, int, bytes]:
    """拆出（编解码标识, 字典指纹, 载荷）三元组。"""
    if len(blob) < _HEADER_LEN:
        raise ValueError("压缩 blob 头部不完整，数据可能已损坏")
    codec_id = blob[0]
    dict_ref = int.from_bytes(blob[1:_HEADER_LEN], "big")
    return codec_id, dict_ref, blob[_HEADER_LEN:]


def decompress_payload(
    codec_id: int, payload: bytes, dictionary: Optional[bytes]
) -> bytes:
    """按行头的编解码标识分发解压；与写入时所用的后端无关。"""
    if codec_id == CODEC_ZLIB:
        return zlib.decompress(payload)
    if codec_id == CODEC_ZSTD:
        zstd_mod = _load_zstd()
        if dictionary is not None:
            dctx = zstd_mod.ZstdDecompressor(
                dict_data=zstd_mod.ZstdCompressionDict(dictionary)
            )
        else:
            dctx = zstd_mod.ZstdDecompressor()
        return dctx.decompressobj().decompress(payload)
    raise ValueError(f"未知的压缩编码标识: {codec_id}")


class CompressionBackend(Protocol):
    """队列存储的压缩后端契约：纯编解码能力 + 可选的训练能力。"""

    codec_id: ClassVar[int]
    supports_dicts: ClassVar[bool]

    def compress(self, data: bytes, dictionary: Optional[bytes]) -> bytes:
        """压缩原始字节；dictionary 为当前活跃字典内容或 None。"""
        ...

    def decompress(self, payload: bytes, dictionary: Optional[bytes]) -> bytes:
        """解压载荷；dictionary 必须与压缩时一致。"""
        ...

    def train(self, samples: Sequence[bytes], target_size: int) -> Optional[bytes]:
        """从样本训练字典内容；返回 None 表示本轮无法产出可用字典。"""
        ...


def _load_zstd():
    """获取 zstandard 模块；缺失时快速失败并给出安装指引。

    采用函数内延迟导入而非模块级可选引用，既保证类型非 Optional，
    也让"卸载依赖后读取旧数据"的场景在调用点拿到明确的报错。
    """
    try:
        import zstandard
    except ImportError as exc:
        raise RuntimeError(
            "该操作需要可选依赖 zstandard，请先执行: pip install zstandard"
        ) from exc
    return zstandard


class ZlibBackend:
    """标准库基础档：逐行 zlib 压缩，永不使用字典。"""

    codec_id: ClassVar[int] = CODEC_ZLIB
    supports_dicts: ClassVar[bool] = False

    def compress(self, data: bytes, dictionary: Optional[bytes]) -> bytes:
        return zlib.compress(data, 6)

    def decompress(self, payload: bytes, dictionary: Optional[bytes]) -> bytes:
        return zlib.decompress(payload)

    def train(self, samples: Sequence[bytes], target_size: int) -> Optional[bytes]:
        return None


class ZstdDictBackend:
    """zstd 字典档：共享字典压缩 + 从近期样本重训下一代字典。"""

    codec_id: ClassVar[int] = CODEC_ZSTD
    supports_dicts: ClassVar[bool] = True

    # 训练所需的最小样本数，低于此值直接视为本轮无可训练字典
    _MIN_TRAIN_SAMPLES = 16

    def __init__(self) -> None:
        # 立即探测依赖可用性，让配置错误在组合根暴露而非首次使用时
        self._zstd = _load_zstd()

    def _build_compressor(self, dictionary: Optional[bytes]):
        if dictionary is not None:
            return self._zstd.ZstdCompressor(
                level=3, dict_data=self._zstd.ZstdCompressionDict(dictionary)
            )
        return self._zstd.ZstdCompressor(level=3)

    def _build_decompressor(self, dictionary: Optional[bytes]):
        if dictionary is not None:
            return self._zstd.ZstdDecompressor(
                dict_data=self._zstd.ZstdCompressionDict(dictionary)
            )
        return self._zstd.ZstdDecompressor()

    def compress(self, data: bytes, dictionary: Optional[bytes]) -> bytes:
        return self._build_compressor(dictionary).compress(data)

    def decompress(self, payload: bytes, dictionary: Optional[bytes]) -> bytes:
        return self._build_decompressor(dictionary).decompressobj().decompress(payload)

    def train(self, samples: Sequence[bytes], target_size: int) -> Optional[bytes]:
        if len(samples) < self._MIN_TRAIN_SAMPLES:
            return None
        try:
            trained = self._zstd.train_dictionary(target_size, list(samples))
        except self._zstd.ZstdError:
            # 样本过少或退化属于预期状况：本轮放弃训练、保持当前档位，待数据积累后重试
            return None
        return trained.as_bytes()


def select_backend(mode: str = "auto") -> CompressionBackend:
    """队列装配入口专用：按配置档位选择压缩后端。

    - auto：检测到可选依赖用字典档，缺失则降级基础档并给出安装指引；
    - zstd：声明使用字典档，依赖缺失直接失败（避免误以基础档完成一次性迁移）；
    - zlib：显式固定基础档，即使可选依赖可用。
    """
    if mode not in ("auto", "zstd", "zlib"):
        raise ValueError(f"未知的压缩档位: {mode}")
    if mode == "zlib":
        return ZlibBackend()
    if _zstd is not None:
        return ZstdDictBackend()
    if mode == "zstd":
        raise RuntimeError(
            "COMFYUI_QUEUE_COMPRESSION=zstd 要求已安装可选依赖 zstandard，"
            "请先执行: pip install zstandard（或改用 auto / zlib）"
        )
    logger.warning(
        "⚠️ 未检测到 zstandard，队列压缩退回标准库 zlib 基础档（体积大于字典档）。"
        "如需最优压缩率请执行: pip install zstandard"
    )
    return ZlibBackend()
