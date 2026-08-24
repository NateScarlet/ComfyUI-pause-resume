import unittest

from gateway.shared.outputs_parser import parse_output_images_summary


class TestParseOutputImagesSummary(unittest.TestCase):
    """测试从 outputs 字段提取第一张输出图片相对路径与图片总数。"""

    def test_sample_outputs_returns_first_relpath_and_total(self):
        """典型 SaveImage 输出：应返回首图相对路径（subfolder/filename）和总数。"""
        outputs = {
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
                    {
                        "filename": "20260723_134155_00003_.png",
                        "subfolder": "TODO",
                        "type": "output",
                    },
                    {
                        "filename": "20260723_134155_00004_.png",
                        "subfolder": "TODO",
                        "type": "output",
                    },
                ]
            }
        }
        self.assertEqual(
            parse_output_images_summary(outputs),
            ("TODO/20260723_134155_00001_.png", 4),
        )

    def test_counts_images_across_multiple_nodes(self):
        """图片分布在多个节点时，计数累加且首图按节点顺序取第一个。"""
        outputs = {
            "1": {
                "images": [
                    {"filename": "first.png", "subfolder": "", "type": "output"}
                ]
            },
            "2": {
                "images": [
                    {"filename": "second.png", "subfolder": "", "type": "output"},
                    {"filename": "third.png", "subfolder": "sub", "type": "output"},
                ]
            },
        }
        self.assertEqual(parse_output_images_summary(outputs), ("first.png", 3))

    def test_empty_subfolder_returns_bare_filename(self):
        """subfolder 为空时相对路径就是文件名本身。"""
        outputs = {
            "9": {"images": [{"filename": "a.png", "subfolder": "", "type": "temp"}]}
        }
        self.assertEqual(parse_output_images_summary(outputs), ("a.png", 1))

    def test_non_image_media_types_are_not_counted(self):
        """只有非 images 媒体类型（如 audio）时不计入图片。"""
        outputs = {"9": {"audio": [{"filename": "a.wav"}]}}
        self.assertEqual(parse_output_images_summary(outputs), (None, 0))

    def test_empty_outputs_returns_none_and_zero(self):
        self.assertEqual(parse_output_images_summary({}), (None, 0))

    def test_malformed_node_outputs_raises_value_error(self):
        """节点输出不是字典时应快速失败，与 parse_outputs_count 行为一致。"""
        with self.assertRaises(ValueError):
            parse_output_images_summary({"12": ["not-a-dict"]})


if __name__ == "__main__":
    unittest.main()
