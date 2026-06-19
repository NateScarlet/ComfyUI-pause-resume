import json
from typing import Any, Dict, List, Generator, cast
from .models import RawJSON


class RawJSONEncoder(json.JSONEncoder):
    """自定义 JSON 编码器。

    当遇到 RawJSON 对象时，直接将其作为原始 JSON 字符串流输出，而不进行多余的转义和外层加双引号。
    """

    def iterencode(self, o: Any, _one_shot: bool = False) -> Generator[str, None, None]:
        if isinstance(o, RawJSON):
            yield o
            return
        elif isinstance(o, dict):
            yield "{"
            first = True
            for key, value in cast(Dict[str, Any], o).items():
                if not first:
                    yield ", "
                first = False
                yield from self.iterencode(key)
                yield ": "
                yield from self.iterencode(value)
            yield "}"
        elif isinstance(o, list):
            yield "["
            first = True
            for item in cast(List[Any], o):
                if not first:
                    yield ", "
                first = False
                yield from self.iterencode(item)
            yield "]"
        else:
            yield from super().iterencode(o, _one_shot)


def raw_json_dumps(obj: Any, **kwargs: Any) -> str:
    """替代原生的 json.dumps，支持以不转义的形式直接嵌入 RawJSON 格式的字段。"""
    return "".join(RawJSONEncoder(**kwargs).iterencode(obj))
