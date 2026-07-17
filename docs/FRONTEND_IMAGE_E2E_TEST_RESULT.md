# 证件照图片场景前端 E2E 测试结果

- 测试时间：2026-07-16（Asia/Tokyo）
- 测试方式：Playwright 1.61.1 + Headless Chromium 149
- 页面地址：`http://127.0.0.1:8888/`
- Server：项目 `start.sh`，使用项目 `.venv`
- 图片后端：本地规则（InsightFace SCRFD + OpenCV）
- 向量模型：`OFA-Sys/chinese-clip-vit-base-patch16`
- VLM：实现与 `multimodal_toolkit` 当前 prompt 对齐；对齐后尚未重新执行付费调用

## 结论

新的证件照图片数据集及前端主流程通过。Playwright 自动验证了 15 条 Manifest、四阶段 SSE 流水线、结果卡片、错误文件隔离以及中文文本搜图；浏览器 console 无错误，相关网络请求均成功。

后续界面信息架构已调整为“音频处理 / 图片处理 / 公共数据”三个顶层入口：音频内部只保留批量、实时、录制和文字入口，图片流水线不再混入音频二级导航，Lance、Iceberg 和 SQL 集中到公共数据。桌面及 390px 移动端导航回归通过，页面级横向溢出为 0。

本地规则在 13 张有效图片上判断正确 11 张，符合预期地漏判了“墨镜遮眼”和“严重过曝”。此前强化 Demo prompt 得到的千问 VLM 13/13 结果不再计入当前结论，因为该 prompt 超出了 `multimodal_toolkit` 的实际合规口径；当前实现已经回退并与 toolkit 对齐。

## 汇总

| 类别 | 结果 |
| --- | --- |
| Manifest | 15 条（13 张有效图片、1 张损坏、1 张缺失） |
| Playwright 自动断言 | 8 PASS / 0 FAIL |
| 本地规则页面汇总 | 4 个合规、9 个不合规、2 个处理失败 |
| 本地规则有效图片准确率 | 11 / 13 |
| 千问 VLM 有效图片准确率 | 待按对齐后的 toolkit prompt 重新测试 |
| ChineseCLIP 向量 | 13 / 13 |
| 浏览器 console | 0 条错误 |
| 网络请求 | 全部成功 |
| 工作区导航回归 | PASS（音频/图片/公共数据互不混入） |
| 390px 移动端布局 | PASS（横向溢出 0px） |

## 数据场景

数据集覆盖以下证件照场景：

- 标准正面男性、女性单人证件照
- 两人同时入镜
- 医用口罩遮挡下半脸
- 不透明墨镜遮挡眼睛
- 手掌大面积遮脸
- 接近九十度侧脸
- 人脸局部模糊和整图失焦
- 人脸严重裁切出框
- 人脸在画面中占比过小
- 严重过曝和严重欠曝
- 文件损坏与源文件缺失

## 前端 E2E 检查

| 检查项 | 结果 | 关键数据 |
| --- | --- | --- |
| 页面加载 | PASS | 展示 15 条 Manifest，默认选择本地规则 |
| SSE 完整流水线 | PASS | ingest、analyze、embed、query 四阶段全部完成 |
| 结果卡片 | PASS | 页面展示全部 15 条处理记录 |
| 汇总数字 | PASS | 4 个合规、9 个不合规、2 个处理失败 |
| 遮挡样本 | PASS | 口罩、墨镜、手遮脸等卡片均正常展示 |
| 中文文本搜图 | PASS | “戴口罩”Top 1 为 `id_medical_mask` |
| 图片资源 | PASS | 13 个有效 `/api/image/assets/*` 均返回 200 |
| 浏览器 console | PASS | 无 warning/error |

本轮完整流水线耗时约 17.2 秒，其中 ChineseCLIP 首次生成 13 张图片向量约 14.9 秒。

## 本地规则结果

本地规则正确识别 11/13 个有效场景，以下两张是有意保留的规则能力边界：

| 图片 | 期望 | 本地规则 |
| --- | --- | --- |
| `id_sunglasses` | 不合规 | 合规（漏判） |
| `id_overexposed` | 不合规 | 合规（漏判） |

Demo 当前的本地阈值、SCRFD 原始人脸数、最大脸指标口径、VLM prompt 和默认并发数均与 `multimodal_toolkit` 对齐。这样测试样本仍可展示规则边界，但不会通过 Demo 专用策略制造正式工具箱没有的判断能力。

可用以下命令检查两仓库是否再次发生配置或 prompt 漂移：

```bash
python scripts/check_image_policy_parity.py
```

## 测试证据

证据目录：`/tmp/multimodal-id-photo-e2e`

| 文件 | 内容 |
| --- | --- |
| `trace.zip` | 完整 Playwright trace、DOM snapshot 和截图 |
| `results.json` | 8 项自动断言结果 |
| `console.json` | 浏览器 console 记录，空数组 |
| `network.json` | 图片 API 请求和状态码 |
| `screenshots/01-id-photo-tab.png` | 证件照图片页初始状态 |
| `screenshots/02-local-id-results.png` | 本地规则完整流水线结果 |
| `screenshots/03-mask-query.png` | “戴口罩”文本搜图结果 |
| `01-audio-workspace.png` | 重排后的音频处理工作区 |
| `02-image-workspace.png` | 重排后的图片处理工作区 |
| `03-shared-data.png` | 音频与图片共用的数据工作区 |
| `04-mobile-image-workspace.png` | 390px 移动端图片工作区 |

后端真实结果文件：

- `/tmp/id-photo-local-results.json`：本地规则结果
- `/tmp/id-photo-contact-sheet.jpg`：全部证件照场景联系表

## 未覆盖范围

- 本轮 Playwright 使用本地规则模式；对齐 toolkit prompt 后尚未再次触发千问付费调用。
- Safari、Firefox 和 Windows 浏览器兼容性。
- 移动端响应式布局沿用上一轮已知问题，本轮未重新验证。
