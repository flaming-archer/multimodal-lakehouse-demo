# 多模态湖仓处理平台 - 演示版

多模态湖仓处理平台 MVP 演示。聚焦**运营商客服语音实时解析**场景。

## 快速开始

```bash
# 首次运行：创建虚拟环境并安装依赖
./start.sh --setup

# 后续直接启动
./start.sh

# 访问
# 前端: http://localhost:8888
# API文档: http://localhost:8888/docs
```

只检查本地环境而不启动服务：

```bash
./start.sh --check
```

启用千问 VLM 时，在启动前通过环境变量提供密钥，不要将密钥写入脚本或提交到仓库：

```bash
export IMAGE_VLM_API_KEY='你的 API Key'
export IMAGE_VLM_MODEL='qwen-vl-max'
./start.sh
```

首次运行图片流水线会下载并缓存 InsightFace 人脸检测模型和约 1.4 GB 的
ChineseCLIP 模型。模型加载完成后，图片向量和中文查询向量都由真实模型现场生成。

### 图片头像合规与文本搜图

顶部“图片处理”工作区提供四步流水线：图片入库、头像合规分析、ChineseCLIP
向量生成、中文文本搜图。图片后端是独立 Python 实现，只使用 OpenCV、
InsightFace、ChineseCLIP 和 Lance，不依赖 Daft 或 Gravitino。

演示数据包含 15 条证件照场景：2 张标准照、多人、口罩、墨镜、手遮脸、
强侧脸、脸部/整图模糊、半脸裁切、小脸、过曝、欠曝，以及损坏/缺失文件。
7 张基础照片由通义万相生成，均提示为虚构成年人；其余质量异常由 OpenCV
确定性派生。生成 prompt、模型和任务 ID 记录在
`data/images/generation_metadata.json`，可通过以下命令重新生成：

```bash
export DASHSCOPE_API_KEY='你的 DashScope API Key'
python scripts/generate_id_photo_samples.py
```

生成脚本不会保存或打印 API key；已有基础图片会被跳过，模糊、裁切、曝光等
派生场景会根据标准照片重新构造。

默认使用本地规则判断头像合规。切换到视觉大模型时，需要配置一个支持图片输入的
OpenAI-compatible Chat Completions 服务：

```bash
export IMAGE_VLM_API_KEY=sk-...
export IMAGE_VLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
export IMAGE_VLM_MODEL=qwen-vl-max
```

常用图片模型配置：

```bash
export IMAGE_EMBED_MODEL=OFA-Sys/chinese-clip-vit-base-patch16
export IMAGE_EMBED_DEVICE=cpu       # 可改为 cuda
export INSIGHTFACE_MODEL=buffalo_l
export IMAGE_VLM_CONCURRENCY=1
```

VLM 与本地规则是互斥的合规后端。VLM 单张调用失败时仅将该行标记为
`llm_failed`，不会回退成本地规则；可解码图片仍可在后续阶段生成向量。
本地规则阈值、最大脸/人脸数计算口径、VLM prompt 和默认并发数与
`multimodal_toolkit` 的图片实现保持一致，避免 Demo 展示出正式工具箱没有的
判断能力。Demo 只重写执行与存储链路，不单独扩展合规策略。

两仓库位于同级目录时，可执行策略漂移检查：

```bash
python scripts/check_image_policy_parity.py
```

## 项目结构

```
multimodal-lakehouse-demo/
├── docs/
│   ├── ARCHITECTURE.md              # 架构设计文档
│   └── architecture-realtime.svg    # 实时解析架构图
├── backend/
│   ├── main.py                      # FastAPI + WebSocket 服务
│   ├── voice_parser.py              # 语音解析引擎
│   ├── storage_layer.py             # Lance/Iceberg 存储模拟
│   ├── gravitino_client.py          # Gravitino 元数据模拟
│   └── config.py                    # 配置
├── frontend/
│   ├── index.html                   # 演示前端（待开发）
│   ├── css/
│   └── js/
├── data/samples/                    # 示例数据
├── config/                          # 配置文件
├── requirements.txt
└── README.md
```

## 核心架构

双路架构（热路径 + 冷路径分离）：

- **热路径**：WebSocket 音频流 → ASR → LLM 意图提取 → 前端实时看板（1-3秒端到端）
- **冷路径**：结果 → Kafka 事件 → Lance + Iceberg + S3 写入 → Gravitino 注册（异步）

详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## API 概览

| 端点 | 用途 |
|------|------|
| `POST /api/voice/analyze` | 热路径：提交文本 → 实时 LLM 解析 |
| `WebSocket /ws/realtime` | 实时双向流式通信 |
| `POST /api/simulation/start` | 启动模拟推流 |
| `GET /api/gravitino/catalogs` | 查看元数据 |
| `GET /api/storage/datasets` | 查看数据集 |
| `POST /api/image/analyze` | 使用本地规则或 VLM 批量判断头像合规 |
| `POST /api/image/embed` | 现场生成 ChineseCLIP 图片向量 |
| `POST /api/image/query` | 标量筛选或中文文本搜图 |
| `GET /api/image/records` | 浏览图片 Lance 表中的分析明细（排除 blob/向量大列） |
| `GET /api/image/run-all-stream` | SSE 运行完整图片流水线 |
| `GET /api/lance/records?dataset=all` | 统一浏览音频与图片 Lance 数据；也可选择 `audio` 或 `image` |
