#!/usr/bin/env python3
"""检查 Demo 与同级 multimodal_toolkit 的图片策略是否发生漂移。"""
from __future__ import annotations

import argparse
import ast
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOOLKIT = ROOT.parent / "multimodal_toolkit"
CONFIG_NAMES = (
    "INSIGHTFACE_MODEL",
    "INSIGHTFACE_ROOT",
    "FACE_DET_SIZE",
    "FACE_DET_THRESH",
    "IMAGE_LONG_EDGE",
    "FACE_DET_SCORE_MIN",
    "MIN_FACE_RATIO",
    "BLUR_THRESHOLD",
    "FACE_BLUR_THRESHOLD",
    "AVATAR_MIN_FACE_RATIO",
    "IMAGE_EMBED_MODEL",
    "IMAGE_EMBED_DEVICE",
    "IMAGE_EMBED_DIM",
    "IMAGE_VLM_CONCURRENCY",
)


def _env_defaults(path: Path) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    defaults: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or target.id not in CONFIG_NAMES:
            continue
        for call in ast.walk(node.value):
            if not isinstance(call, ast.Call) or len(call.args) < 2:
                continue
            func = call.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "getenv"
                and isinstance(call.args[0], ast.Constant)
                and call.args[0].value == target.id
                and isinstance(call.args[1], ast.Constant)
            ):
                defaults[target.id] = str(call.args[1].value)
                break
    return defaults


def _load_function(path: Path, function_name: str):
    spec = importlib.util.spec_from_file_location(f"policy_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, function_name)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--toolkit-dir", type=Path, default=DEFAULT_TOOLKIT)
    args = parser.parse_args()
    toolkit = args.toolkit_dir.resolve()

    demo_defaults = _env_defaults(ROOT / "backend" / "image_pipeline.py")
    toolkit_defaults = _env_defaults(toolkit / "multimodal_toolkit" / "image" / "config.py")
    differences = [
        f"{name}: demo={demo_defaults.get(name)!r}, toolkit={toolkit_defaults.get(name)!r}"
        for name in CONFIG_NAMES
        if demo_defaults.get(name) != toolkit_defaults.get(name)
    ]

    demo_prompt = _load_function(
        ROOT / "backend" / "image_vlm_client.py", "build_prompt"
    )()
    toolkit_prompt = _load_function(
        toolkit / "multimodal_toolkit" / "image" / "prompt.py",
        "build_image_analysis_prompt",
    )()
    if demo_prompt != toolkit_prompt:
        differences.append("VLM prompt 不一致")

    if differences:
        print("图片策略存在差异：")
        for difference in differences:
            print(f"- {difference}")
        return 1
    print("图片策略一致：配置默认值与 VLM prompt 均匹配 multimodal_toolkit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
