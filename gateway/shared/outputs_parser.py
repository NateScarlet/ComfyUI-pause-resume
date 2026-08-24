import json
from typing import Optional, Dict, Any, List, Tuple, cast

THREE_D_EXTENSIONS = frozenset({".obj", ".fbx", ".gltf", ".glb", ".usdz"})


def extract_workflow_id(extra_data_json: str) -> Optional[str]:
    """从 extra_data JSON 字符串中提取 workflow_id。"""
    try:
        extra_data = json.loads(extra_data_json)
        return extra_data.get("extra_pnginfo", {}).get("workflow", {}).get("id")
    except (json.JSONDecodeError, TypeError, KeyError, AttributeError):
        return None


def parse_outputs_count(outputs: Dict[str, Any]) -> int:
    """从 outputs 字典中扁平化计算生成资产文件数量。"""
    count = 0
    for node_id, node_outputs in outputs.items():
        if not isinstance(node_outputs, dict):
            raise ValueError(
                f"Unexpected node outputs format for node '{node_id}': expected dict, got {type(node_outputs)}"
            )

        node_outputs_dict = cast(Dict[str, Any], node_outputs)
        for media_type, items in node_outputs_dict.items():
            if media_type == "animated" or not isinstance(items, list):
                continue

            for item in cast(List[Any], items):
                if not isinstance(item, dict):
                    if _is_3d_item(str(item)) or media_type == "text":
                        count += 1
                    continue
                count += 1
    return count


def parse_output_images_summary(outputs: Dict[str, Any]) -> Tuple[Optional[str], int]:
    """从 outputs 字典中提取第一张输出图片的相对路径和图片总数。

    相对路径为 ComfyUI /view 接口语义下的 subfolder/filename 形式，
    只统计 images 媒体类型，跨节点按出现顺序累加。
    """
    total = 0
    first_rel_path: Optional[str] = None
    for node_id, node_outputs in outputs.items():
        if not isinstance(node_outputs, dict):
            raise ValueError(
                f"Unexpected node outputs format for node '{node_id}': expected dict, got {type(node_outputs)}"
            )

        node_outputs_dict = cast(Dict[str, Any], node_outputs)
        items = node_outputs_dict.get("images", [])
        if not isinstance(items, list):
            continue

        for item in cast(List[Any], items):
            if not isinstance(item, dict):
                continue
            item_dict = cast(Dict[str, Any], item)
            total += 1
            if first_rel_path is None:
                subfolder = str(item_dict.get("subfolder") or "")
                filename = str(item_dict.get("filename") or "")
                first_rel_path = f"{subfolder}/{filename}" if subfolder else filename
    return first_rel_path, total


def _is_3d_item(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(ext) for ext in THREE_D_EXTENSIONS)
