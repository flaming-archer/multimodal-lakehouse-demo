# 多模态湖仓处理平台 - 演示版

多模态湖仓处理平台 MVP 演示。聚焦**运营商客服语音实时解析**场景。

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
cd backend
python main.py

# 访问
# 前端: http://localhost:8888
# API文档: http://localhost:8888/docs
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
