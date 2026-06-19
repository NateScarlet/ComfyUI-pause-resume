"""RawJSON / raw_json_dumps / _RawJSONEncoder 单元测试"""

import json
import io
import unittest
from typing import Any

from gateway.shared.models import RawJSON
from gateway.shared.utils import raw_json_dumps, RawJSONEncoder


class TestRawJSON(unittest.TestCase):
    """RawJSON 基础类型测试"""

    def test_is_str_subclass(self):
        """RawJSON 是 str 的子类"""
        r = RawJSON('{"a": 1}')
        self.assertIsInstance(r, str)
        self.assertEqual(r, '{"a": 1}')

    def test_json_loads_compatible(self):
        """RawJSON 可以被 json.loads 正常解析"""
        r = RawJSON('{"a": 1}')
        self.assertEqual(json.loads(r), {"a": 1})


class TestRawJSONDumps(unittest.TestCase):
    """raw_json_dumps 序列化测试"""

    def test_rawjson_embedded_as_raw(self):
        """RawJSON 对象在序列化时直接嵌入，不做转义"""
        result = raw_json_dumps({"key": RawJSON('{"a": 1}')})
        self.assertEqual(result, '{"key": {"a": 1}}')

    def test_rawjson_not_escaped(self):
        """RawJSON 不会被转义成字符串（没有外层引号）"""
        result = raw_json_dumps(RawJSON("42"))
        self.assertEqual(result, "42")

    def test_regular_str_escaped(self):
        """普通 str 仍然正常转义"""
        result = raw_json_dumps({"key": "hello"})
        self.assertEqual(result, '{"key": "hello"}')

    def test_regular_int(self):
        """普通 int 正常序列化"""
        result = raw_json_dumps({"key": 42})
        self.assertEqual(result, '{"key": 42}')

    def test_regular_bool(self):
        """普通 bool 正常序列化"""
        result = raw_json_dumps({"key": True})
        self.assertEqual(result, '{"key": true}')

    def test_regular_none(self):
        """None 正常序列化"""
        result = raw_json_dumps({"key": None})
        self.assertEqual(result, '{"key": null}')

    def test_mixed_raw_and_regular(self):
        """RawJSON 与普通类型混合"""
        result = raw_json_dumps(
            {
                "id": "abc",
                "data": RawJSON('{"x": 1, "y": 2}'),
                "count": 3,
            }
        )
        self.assertEqual(result, '{"id": "abc", "data": {"x": 1, "y": 2}, "count": 3}')

    def test_nested_list_with_rawjson(self):
        """嵌套列表中包含 RawJSON"""
        result = raw_json_dumps(
            [
                "hello",
                RawJSON('{"a": 1}'),
                42,
            ]
        )
        self.assertEqual(result, '["hello", {"a": 1}, 42]')

    def test_rawjson_in_sublist(self):
        """深层嵌套中的 RawJSON"""
        result = raw_json_dumps(
            {
                "rows": [
                    [1, RawJSON('"embedded"')],
                    [2, RawJSON("null")],
                ]
            }
        )
        self.assertEqual(result, '{"rows": [[1, "embedded"], [2, null]]}')

    def test_multiple_rawjson(self):
        """多个 RawJSON 对象"""
        result = raw_json_dumps(
            {
                "prompt": RawJSON('{"nodes": [1, 2]}'),
                "extra_data": RawJSON('{"client_id": "abc"}'),
            }
        )
        self.assertEqual(
            result,
            '{"prompt": {"nodes": [1, 2]}, "extra_data": {"client_id": "abc"}}',
        )

    def test_roundtrip_parseable(self):
        """序列化结果可以被 json.loads 重新解析"""
        result = raw_json_dumps(
            {
                "prompt": RawJSON('{"nodes": [1, 2]}'),
                "extra_data": RawJSON('{"client_id": "abc"}'),
            }
        )
        parsed = json.loads(result)
        self.assertEqual(
            parsed,
            {
                "prompt": {"nodes": [1, 2]},
                "extra_data": {"client_id": "abc"},
            },
        )

    def test_empty_rawjson(self):
        """空 JSON 对象/数组的 RawJSON"""
        self.assertEqual(raw_json_dumps(RawJSON("{}")), "{}")
        self.assertEqual(raw_json_dumps(RawJSON("[]")), "[]")

    def test_rawjson_complex_object(self):
        """复杂嵌套 JSON 的 RawJSON"""
        r = RawJSON(json.dumps({"a": {"b": [1, 2, 3]}, "c": "hello"}))
        result = raw_json_dumps({"data": r})
        self.assertEqual(result, '{"data": {"a": {"b": [1, 2, 3]}, "c": "hello"}}')


class TestRawJSONEncoder(unittest.TestCase):
    """RawJSONEncoder 配合 json.dump 的测试"""

    def test_json_dump_with_encoder(self):
        """json.dump 使用 RawJSONEncoder 能正确嵌入 RawJSON"""
        buf = io.StringIO()
        data: dict[str, Any] = {"prompt": RawJSON('{"a": 1}')}
        json.dump(data, buf, ensure_ascii=False, cls=RawJSONEncoder)
        self.assertEqual(buf.getvalue(), '{"prompt": {"a": 1}}')

    def test_json_dump_nested(self):
        """json.dump 嵌套结构"""
        buf = io.StringIO()
        data: dict[str, list[list[Any]]] = {
            "queue_running": [[1.0, "id", RawJSON('{"a": 1}'), RawJSON('{"b": 2}'), []]]
        }
        json.dump(data, buf, ensure_ascii=False, cls=RawJSONEncoder)
        self.assertEqual(
            buf.getvalue(),
            '{"queue_running": [[1.0, "id", {"a": 1}, {"b": 2}, []]]}',
        )


class TestRawJSONPerformance(unittest.TestCase):
    """验证 RawJSON 确实避免了重复解析"""

    def test_rawjson_dumps_does_not_parse(self):
        """raw_json_dumps 不会对 RawJSON 内容做 json.loads

        用非法 JSON 内容作为 RawJSON 值：如果内部做了 json.loads 解析会报错，
        但因为 RawJSON 是直接嵌入的，所以即使内容是非法 JSON 也能正常输出。
        """
        invalid_json = "{not valid json!!!}"
        r = RawJSON(invalid_json)

        # 如果 raw_json_dumps 尝试解析，json.loads 会抛出异常
        result = raw_json_dumps({"data": r})
        expected = '{"data": {not valid json!!!}}'
        self.assertEqual(result, expected)

    def test_rawjson_dumps_does_not_parse_large(self):
        """大对象也能正确嵌入，不经过解析"""
        large_dict = {"key" + str(i): i for i in range(1000)}
        large_json = json.dumps(large_dict)
        r = RawJSON(large_json)

        result = raw_json_dumps({"data": r})
        expected = '{"data": ' + large_json + "}"
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
