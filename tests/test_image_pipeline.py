"""图片流水线的轻量回归测试；真实模型冒烟测试由手工/CI 缓存环境执行。"""
from __future__ import annotations

import json
import inspect
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import image_pipeline
from image_vlm_client import build_prompt, validate_response


class ImagePipelineTests(unittest.TestCase):
    def test_text_query_defaults_to_top_three(self):
        self.assertEqual(inspect.signature(image_pipeline.text_query).parameters["top_k"].default, 3)

    def test_manifest_has_expected_scenarios(self):
        rows = json.loads((ROOT / "data" / "images" / "manifest.json").read_text("utf-8"))
        self.assertEqual(len(rows), 15)
        self.assertEqual(len({row["doc_id"] for row in rows}), 15)
        scenarios = {row["scenario"] for row in rows}
        self.assertTrue(
            {
                "standard", "multiple_people", "lower_face_occluded", "eyes_occluded",
                "hand_occluded", "side_profile", "face_blurred", "image_blurred",
                "face_cropped", "tiny_face", "overexposed", "underexposed",
                "corrupt_file", "missing_file",
            }.issubset(scenarios)
        )
        expected = {row["doc_id"]: row["expected_avatar"] for row in rows}
        self.assertTrue(expected["id_standard_woman"])
        self.assertTrue(expected["id_standard_man"])
        self.assertFalse(expected["id_medical_mask"])
        self.assertFalse(expected["id_hand_occluded"])

    def test_vlm_response_contract(self):
        result = validate_response(
            {
                "has_face": True,
                "is_blurry": False,
                "is_face_blurry": False,
                "is_avatar": True,
                "clarity_confidence": 0.95,
                "avatar_confidence": 0.91,
                "reason": "单人脸清晰且占比合理",
            }
        )
        self.assertTrue(result["is_avatar"])
        self.assertEqual(result["avatar_confidence"], 0.91)

    def test_vlm_rejects_conflicting_result(self):
        with self.assertRaises(ValueError):
            validate_response(
                {
                    "has_face": False,
                    "is_blurry": False,
                    "is_face_blurry": False,
                    "is_avatar": True,
                    "clarity_confidence": 0.8,
                    "avatar_confidence": 0.8,
                    "reason": "冲突结果",
                }
            )

    def test_ingest_keeps_missing_row(self):
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            result = image_pipeline.ingest(uri)
            self.assertEqual(result["rows"], 15)
            self.assertEqual(result["missing"], 1)
            rows = image_pipeline._read_rows(uri)
            missing = next(row for row in rows if row["doc_id"] == "missing_image")
            self.assertEqual(missing["analysis_status"], "download_failed")
            self.assertIsNone(missing["image_blob"])

    def test_public_row_never_returns_large_columns(self):
        row = {
            "doc_id": "demo",
            "image_blob": b"bytes",
            "image_embedding": [0.0] * image_pipeline.IMAGE_EMBED_DIM,
        }
        result = image_pipeline._public_row(row)
        self.assertNotIn("image_blob", result)
        self.assertNotIn("image_embedding", result)
        self.assertEqual(result["preview_url"], "/api/image/assets/demo")

    def test_list_records_exposes_analysis_without_large_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            image_pipeline.ingest(uri)
            with patch("image_pipeline.get_face_detector") as detector:
                detector.return_value.detect.return_value = []
                image_pipeline.analyze("local", uri)
            result = image_pipeline.list_records(5, uri)
            self.assertEqual(result["count"], 15)
            self.assertEqual(len(result["records"]), 5)
            self.assertEqual(result["summary"]["analyzed"], 13)
            self.assertEqual(result["summary"]["failed"], 2)
            self.assertIn("analysis_status", result["schema"])
            self.assertIn("is_avatar", result["schema"])
            self.assertNotIn("image_blob", result["records"][0])
            self.assertNotIn("image_embedding", result["records"][0])
            self.assertIn("preview_url", result["records"][0])

    def test_vlm_mode_clears_local_scores(self):
        class FakeClient:
            def analyze(self, _jpeg_bytes):
                return {
                    "has_face": True,
                    "is_blurry": False,
                    "is_face_blurry": False,
                    "is_avatar": True,
                    "clarity_confidence": 0.96,
                    "avatar_confidence": 0.92,
                    "reason": "清晰单人头像",
                }

        row = {
            "doc_id": "avatar",
            "image_blob": (ROOT / "data" / "images" / "id_standard_woman.jpg").read_bytes(),
            "face_count": 99,
            "blur_score": 999.0,
        }
        image_pipeline._vlm_analysis(row, FakeClient())
        self.assertEqual(row["analysis_backend"], "vlm")
        self.assertEqual(row["analysis_status"], "ok")
        self.assertIsNone(row["face_count"])
        self.assertIsNone(row["blur_score"])
        self.assertTrue(row["is_avatar"])

    def test_vlm_failure_does_not_fallback(self):
        class FailingClient:
            def analyze(self, _jpeg_bytes):
                raise TimeoutError("vision timeout")

        row = {
            "doc_id": "avatar",
            "image_blob": (ROOT / "data" / "images" / "id_standard_woman.jpg").read_bytes(),
        }
        image_pipeline._vlm_analysis(row, FailingClient())
        self.assertEqual(row["analysis_status"], "llm_failed")
        self.assertIsNone(row["is_avatar"])
        self.assertIn("vision timeout", row["analysis_error"])

    def test_batch_vlm_keeps_bad_rows_and_clears_local_fields(self):
        class FakeClient:
            def analyze(self, _jpeg_bytes):
                return {
                    "has_face": True,
                    "is_blurry": False,
                    "is_face_blurry": False,
                    "is_avatar": True,
                    "clarity_confidence": 0.9,
                    "avatar_confidence": 0.9,
                    "reason": "测试结论",
                }

        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            image_pipeline.ingest(uri)
            with patch("image_vlm_client.ImageVLMClient", FakeClient):
                result = image_pipeline.analyze("vlm", uri)
            self.assertEqual(result["ok"], 13)
            self.assertEqual(result["failed"], 2)
            valid = next(row for row in result["results"] if row["doc_id"] == "id_standard_woman")
            corrupt = next(row for row in result["results"] if row["doc_id"] == "corrupt_image")
            self.assertEqual(valid["analysis_backend"], "vlm")
            self.assertIsNone(valid["face_count"])
            self.assertEqual(corrupt["analysis_status"], "decode_failed")
            self.assertIsNone(corrupt["is_avatar"])

    def test_local_rule_counts_all_detector_faces_like_toolkit(self):
        class FakeDetector:
            def detect(self, _image):
                return [
                    {"bbox": (40.0, 40.0, 400.0, 400.0), "score": 0.95},
                    {"bbox": (2.0, 2.0, 30.0, 30.0), "score": 0.35},
                ]

        row = {
            "doc_id": "avatar",
            "image_blob": (ROOT / "data" / "images" / "id_standard_woman.jpg").read_bytes(),
        }
        with patch("image_pipeline.get_face_detector", return_value=FakeDetector()), patch(
            "image_pipeline.laplacian_variance", return_value=500.0
        ):
            image_pipeline._local_analysis(row)
        self.assertEqual(row["face_count"], 2)
        self.assertTrue(row["has_face"])
        self.assertFalse(row["is_avatar"])

    def test_local_rule_uses_largest_raw_face_like_toolkit(self):
        class FakeDetector:
            def detect(self, _image):
                return [
                    {"bbox": (10.0, 10.0, 490.0, 490.0), "score": 0.40},
                    {"bbox": (100.0, 100.0, 350.0, 350.0), "score": 0.90},
                ]

        row = {
            "doc_id": "avatar",
            "image_blob": (ROOT / "data" / "images" / "id_standard_woman.jpg").read_bytes(),
        }
        with patch("image_pipeline.get_face_detector", return_value=FakeDetector()), patch(
            "image_pipeline.laplacian_variance", return_value=500.0
        ):
            image_pipeline._local_analysis(row)
        self.assertEqual(row["face_count"], 2)
        self.assertEqual(row["face_score"], 0.40)
        self.assertFalse(row["has_face"])
        self.assertFalse(row["is_avatar"])

    def test_vlm_prompt_matches_toolkit_policy(self):
        expected = "\n".join(
            [
                "你是图片质量与头像合规分析助手。请观察图片并只输出严格 JSON，不要解释或使用 Markdown。",
                "",
                "判断标准：",
                "- has_face(bool)：画面中是否存在清晰可见的真人脸部，不要求图片适合作为头像。",
                "- is_blurry(bool)：整张图片的主要内容是否明显模糊、失焦，导致细节难以辨认。",
                "  正常压缩、轻微噪点或背景虚化不算整图模糊。",
                "- is_face_blurry(bool)：存在真人脸部但脸部明显模糊、失焦或无法辨认时为 true；",
                "  没有人脸时必须为 false。",
                "- is_avatar(bool)：是否为适合作为个人头像的真人单人图片。必须只有一个真人作为主要主体，",
                "  脸部清楚可见且占画面合理比例。多人照、卡通、Logo、动物、风景、产品、背景小脸均为 false。",
                "- clarity_confidence(float)：对 is_blurry 判断的置信度，范围 0 到 1。",
                "- avatar_confidence(float)：对 is_avatar 判断的置信度，范围 0 到 1。",
                "- reason(str)：用一句简短中文同时说明清晰度与头像判断依据。",
                "",
                "JSON 必须恰好包含以下字段：has_face、is_blurry、is_face_blurry、is_avatar、",
                "clarity_confidence、avatar_confidence、reason。",
            ]
        )
        self.assertEqual(build_prompt(), expected)

    def test_text_query_rejects_missing_vectors_before_loading_model(self):
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            image_pipeline.ingest(uri)
            with patch("image_pipeline.get_embedder", side_effect=AssertionError("不应加载模型")):
                with self.assertRaisesRegex(image_pipeline.PipelineStateError, "没有可用图片向量"):
                    image_pipeline.text_query("咖啡", lance_uri=uri)

    def test_text_query_rejects_embedding_model_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            image_pipeline.ingest(uri)
            rows = image_pipeline._read_rows(uri)
            rows[0]["embedding_status"] = "ok"
            rows[0]["embedding_model"] = "different/model"
            rows[0]["image_embedding"] = [0.0] * image_pipeline.IMAGE_EMBED_DIM
            image_pipeline._write_rows(rows, uri)
            with patch("image_pipeline.get_embedder", side_effect=AssertionError("不应加载模型")):
                with self.assertRaisesRegex(image_pipeline.PipelineStateError, "模型不一致"):
                    image_pipeline.text_query("头像", lance_uri=uri)

    def test_embed_all_failures_marks_stage_failed_and_persists_errors(self):
        class FailingEmbedder:
            def embed_images(self, _images):
                raise RuntimeError("encoder unavailable")

        with tempfile.TemporaryDirectory() as directory:
            uri = str(Path(directory) / "images.lance")
            image_pipeline.ingest(uri)
            with patch("image_pipeline.get_embedder", return_value=FailingEmbedder()):
                with self.assertRaisesRegex(RuntimeError, "全部失败"):
                    image_pipeline.embed(uri)
            rows = image_pipeline._read_rows(uri)
            decodable = [row for row in rows if row["doc_id"].startswith("id_")]
            self.assertTrue(decodable)
            self.assertTrue(all(row["embedding_status"] == "failed" for row in decodable))
            self.assertFalse(image_pipeline.get_status(uri)["embedded"])


if __name__ == "__main__":
    unittest.main()
