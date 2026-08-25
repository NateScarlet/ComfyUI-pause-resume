import hashlib
import json
import logging
import sys
import unittest
from unittest import mock

from gateway.infrastructure.sqlite.compression import (
    CODEC_ZSTD,
    CODEC_ZLIB,
    ZlibBackend,
    ZstdDictBackend,
    decompress_payload,
    dict_fingerprint,
    pack_blob,
    select_backend,
    unpack_blob,
)

try:
    import zstandard as _zstd
except ImportError:  # 可选依赖缺失时跳过真实 zstd 用例
    _zstd = None


def _similar_sample(seed: int) -> bytes:
    """构造彼此高度相似的样本，模拟同一工作流反复提交的场景。"""
    return json.dumps(
        {"workflow": "demo", "nodes": [{"v": i, "seed": seed} for i in range(64)]}
    ).encode("utf-8")


class TestBlobHeader(unittest.TestCase):
    """测试自描述 blob 头部的打包与拆包契约。"""

    def test_round_trip(self):
        blob = pack_blob(CODEC_ZSTD, 42, b"payload")
        self.assertEqual(unpack_blob(blob), (CODEC_ZSTD, 42, b"payload"))

    def test_zero_ref_means_no_dictionary(self):
        blob = pack_blob(CODEC_ZLIB, 0, b"x")
        self.assertEqual(unpack_blob(blob), (CODEC_ZLIB, 0, b"x"))

    def test_full_u32_range_representable(self):
        # 指纹是内容哈希截断，天然占满 u32 值域且无计数上界
        blob = pack_blob(CODEC_ZSTD, 0xFFFFFFFF, b"payload")
        self.assertEqual(unpack_blob(blob), (CODEC_ZSTD, 0xFFFFFFFF, b"payload"))

    def test_fingerprint_is_truncated_sha256(self):
        expected = int.from_bytes(hashlib.sha256(b"abc").digest()[:4], "big")
        self.assertEqual(dict_fingerprint(b"abc"), expected)

    def test_truncated_header_rejected(self):
        with self.assertRaises(ValueError):
            unpack_blob(b"\x01")

    def test_unknown_codec_dispatch_rejected(self):
        with self.assertRaises(ValueError):
            decompress_payload(99, b"payload", None)


class TestZlibBackend(unittest.TestCase):
    def setUp(self):
        self.backend = ZlibBackend()

    def test_round_trip(self):
        data = _similar_sample(1)
        blob = pack_blob(self.backend.codec_id, 0, self.backend.compress(data, None))
        codec_id, dict_ref, payload = unpack_blob(blob)
        self.assertEqual(
            self.backend.decompress(payload, None),
            decompress_payload(codec_id, payload, None),
        )

    def test_train_never_produces_dictionary(self):
        samples = [_similar_sample(i) for i in range(32)]
        self.assertIsNone(self.backend.train(samples, 65536))


@unittest.skipUnless(_zstd is not None, "需要可选依赖 zstandard")
class TestZstdDictBackend(unittest.TestCase):
    def setUp(self):
        self.backend = ZstdDictBackend()
        self.samples = [_similar_sample(i) for i in range(32)]

    def test_plain_round_trip_before_any_dictionary(self):
        data = _similar_sample(7)
        restored = self.backend.decompress(self.backend.compress(data, None), None)
        self.assertEqual(restored, data)

    def test_trained_dictionary_round_trip_and_shrinkage(self):
        token = self.backend.train(self.samples, 65536)
        self.assertIsNotNone(token)
        target = _similar_sample(999)
        compressed = self.backend.compress(target, token)
        self.assertLess(len(compressed), len(target) // 2)
        self.assertEqual(self.backend.decompress(compressed, token), target)

    def test_train_with_insufficient_samples_returns_none(self):
        self.assertIsNone(self.backend.train([], 65536))
        self.assertIsNone(self.backend.train(self.samples[:3], 65536))

    def test_zstd_row_decodes_via_header_dispatch(self):
        """zstd 行必须能走"按行头分发"路径解码（换后端实例后的读取方式）。"""
        token = self.backend.train(self.samples, 65536)
        self.assertIsNotNone(token)
        target = _similar_sample(5)
        blob = pack_blob(CODEC_ZSTD, dict_fingerprint(token), self.backend.compress(target, token))
        codec_id, dict_ref, payload = unpack_blob(blob)
        self.assertEqual(codec_id, CODEC_ZSTD)
        self.assertEqual(dict_ref, dict_fingerprint(token))
        restored = decompress_payload(codec_id, payload, token)
        self.assertEqual(restored, target)


class TestBackendSelection(unittest.TestCase):
    """按配置选择压缩后端：auto 探测、zstd 声明意图、zlib 显式固定。"""

    def test_auto_uses_zstd_when_available(self):
        if _zstd is None:
            self.skipTest("需要可选依赖 zstandard")
        self.assertIsInstance(select_backend("auto"), ZstdDictBackend)

    def test_auto_falls_back_to_zlib_with_install_hint_when_missing(self):
        with mock.patch("gateway.infrastructure.sqlite.compression._zstd", None):
            with self.assertLogs(
                "gateway.infrastructure.sqlite.compression", level="WARNING"
            ) as logs:
                backend = select_backend("auto")
        self.assertIsInstance(backend, ZlibBackend)
        self.assertIn("pip install zstandard", "\n".join(logs.output))

    def test_zstd_mode_fails_fast_when_dependency_missing(self):
        """声明使用字典档但依赖缺失：必须拒绝启动，防止以基础档完成一次性迁移。"""
        with mock.patch("gateway.infrastructure.sqlite.compression._zstd", None):
            with self.assertRaises(RuntimeError) as ctx:
                select_backend("zstd")
        self.assertIn("pip install zstandard", str(ctx.exception))

    def test_zstd_mode_uses_dictionary_backend(self):
        if _zstd is None:
            self.skipTest("需要可选依赖 zstandard")
        self.assertIsInstance(select_backend("zstd"), ZstdDictBackend)

    def test_zlib_mode_forced_even_when_zstd_available(self):
        if _zstd is None:
            self.skipTest("需要可选依赖 zstandard")
        self.assertIsInstance(select_backend("zlib"), ZlibBackend)

    def test_unknown_mode_rejected(self):
        with self.assertRaises(ValueError):
            select_backend("lzma")

    def test_direct_construction_without_dependency_fails_fast(self):
        # 通过让 import zstandard 直接失败来模拟未安装可选依赖
        with mock.patch.dict(sys.modules, {"zstandard": None}):
            with self.assertRaises(RuntimeError):
                ZstdDictBackend()


if __name__ == "__main__":
    unittest.main()
