# 多模态湖仓处理平台 - 架构设计文档

## 1. 平台概述

多模态湖仓处理平台，统一管理语音、视频、图像等多模态数据。
当前 MVP 阶段聚焦**运营商客服语音实时解析**场景。

平台包含两条处理路径：

| 路径 | 场景 | 延迟 | 描述 |
|------|------|------|------|
| **实时** | 实时语音解析 | 秒级 | WebSocket 流式关键词匹配 + 实时推送看板 |
| **离线** | 湖上异步处理 | 异步/分钟级 | 持久化(Lance/Iceberg/S3 + Gravitino) + 湖上多模数据流水线(入湖→分析→embedding→查询) |

## 2. 技术栈

| 层次 | 技术选型 | 状态 | 作用 |
|------|---------|------|------|
| 元数据管理 | Gravitino | ✅ 已实现 | 统一 Catalog/Schema/Table 管理 |
| 多模态存储 | Lance | ✅ 已实现 | 结构化结果 + 向量 embedding + blob v2 原始文件存储 |
| 批量分析 | Iceberg | ✅ 已实现 | 聚合统计 + 时间分区，支持 time travel |
| 对象存储 | S3 兼容 (MinIO/Ozone) | ✅ 已实现 | 原始音频 + 元数据长期归档 |
| 离线计算引擎 | Daft | ✅ 已实现 | 批处理 ETL 编排（manifest 读取、S3 下载、Lance 读写） |
| 离线计算引擎 | daft-lance | ✅ 已实现 | Daft 与 Lance 桥接层（take_blobs 等） |
| 分布式计算 | Ray | ✅ 已实现 | 流式任务调度，离线批处理分布式执行 |
| ASR | 关键词模拟 / SenseVoice (FunASR) | 🟡 模拟中 | MVP 阶段使用关键词模拟，后续接入真实 ASR |
| 大模型 | 规则引擎 / CodeBuddy AI | 🟡 降级模式 | 优先关键词匹配，LLM 可用时作为补充分析 |
| 消息队列 | Kafka | 🗓 规划中 | 实时→离线解耦（当前通过内存事件替代） |
| Web 框架 | FastAPI + WebSocket | ✅ 已实现 | API 服务 + 实时推送 |

## 3. 双路架构设计

系统采用**实时 + 离线**分离设计：

### 3.1 实时（目标 < 4 秒端到端）

```
WebSocket 音频流 → ASR 流式转写 → LLM 实时解析 → 前端看板
```

| 阶段 | 延迟 | 说明 |
|------|------|------|
| ASR 流式转写 | 0.5-2s | 分句实时输出，不等待整段 |
| LLM 意图提取 | 0.3-0.5s | 提取意图、原因、情绪、实体 |
| 推送到前端 | 0.1s | WebSocket 广播 |

**关键设计原则：实时路径不等待任何存储写入。** LLM 解析结果立刻推送前端看板。

### 3.2 离线 —— 湖上异步处理

离线路径是湖上的异步处理层，融合了实时结果持久化与湖上多模数据批处理。

#### 3.2.1 实时结果持久化

```
LLM 解析结果 → Kafka 事件 → Lance 写入 → Iceberg 写入 → S3 归档 → Gravitino 注册
```

- 存储写入全部异步，不阻塞实时路径
- Gravitino 注册在存储写入后执行
- 提供历史查询、趋势分析、报表回放

#### 3.2.2 湖上多模数据处理流水线

对 S3 历史音频数据，在湖上完成端到端处理：

```
S3 Manifest → Daft download → Lance blob v2 入湖
                                         ↓
            Lance blob v2 → SenseVoice ASR → 声学情绪标签提炼
                                         ↓
            PII 脱敏 → DeepSeek/CodeBuddy LLM → 向量嵌入 → add_columns
                                         ↓
            Daft 标量查询 / Lance native ANN 向量检索
```

| 步骤 | 模块 | 引擎 | 说明 |
|------|------|------|------|
| 1 ingest | `mmt-ingest` | Daft | 加载语音，从 S3 Manifest 拉取原始音频，以 Lance blob v2 入湖 |
| 2 transcribe | `mmt-transcribe` | Daft+Lance | 语音转文字(ASR) + 声学情绪标签提炼 |
| 3 analyze | `mmt-analyze` | Lance+LLM | PII脱敏 + LLM意图/情绪分析 + 128 维向量嵌入 |
| 4 query | `mmt-query` | Daft+Lance | 标量过滤 / ANN 向量检索 |

**关键设计原则**：
- 湖上处理与实时路径完全解耦，异步运行不阻塞实时链路
- 向量嵌入在步骤 3 分析阶段一并计算，步骤 4 可直接进行标量和向量检索
- LLM 分析走统一 llm_client，支持 CodeBuddy/OpenAI，不可用时降级规则兜底

## 4. 语音解析场景流程

### 4.1 场景描述

运营商客服通话中，用户表达转网意愿，系统需实时提取：

| 提取项 | 来源 | 示例输出 |
|--------|------|---------|
| 意图 | LLM NLU | "转网咨询" / "携号转网" / "投诉" |
| 原因 | LLM 关键词 + 语义 | ["套餐价格偏高", "网络覆盖差", "客服响应慢"] |
| 情绪 | 情感分析 | "负面" / "中性" / "正面" |
| 流失风险 | 情绪 + 原因数 | "高" / "中" / "低" |
| 关键实体 | NER | {"资费": "199元/月", "区域": "朝阳区"} |
| 建议动作 | LLM 推理 | "优先处理网络覆盖，安排技术测试" |

### 4.2 数据流

```
音频文件/流 → VoiceParser.analyze()
  ├── 实时: 立即返回分析结果 → WebSocket 推送到前端
  └── 离线（湖上异步处理）:
        ├── 实时结果持久化: _async_store() 后台写入 Lance + Iceberg + S3
        └── 湖上多模数据流水线 (mmt-*):
              ├── ingest:     S3 拉取 → Lance blob v2 入湖
              ├── transcribe: ASR 转写 + 声学情绪标签提炼
              ├── analyze:    PII 脱敏 + LLM 分析 + 向量嵌入(128维)
              └── query:      标量过滤 / ANN 向量检索
```

## 5. Gravitino 元数据模型

```
Metalake: demo_metalake
├── lance_catalog (type: lance)
│   ├── voice_analysis       -- call_analysis, intent_classification
│   ├── camera_signal        -- checkpoint_data, daily_comparison
│   └── image_processing     -- id_card_ocr, image_quality
├── iceberg_catalog (type: iceberg)
│   ├── analytics            -- daily_aggregation, hourly_metrics
│   └── churn_risk           -- churn_prediction, retention_strategy
└── fileset_catalog (type: fileset)
    ├── raw_audio            -- 原始录音文件
    ├── raw_video            -- 原始监控视频
    └── raw_images           -- 原始图像数据
```

## 6. API 设计

### 6.1 REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/health | 健康检查 |
| GET | /api/overview | 系统总览 |
| GET | /api/voice/demo-transcripts | 获取演示通话文本 |
| POST | /api/voice/analyze | 提交文本→LLM实时解析（热路径） |
| POST | /api/voice/batch-analyze | 批量分析 |
| POST | /api/voice/demo-analyze-all | 分析全部演示文本 |
| POST | /api/simulation/start | 启动实时模拟（WebSocket推送） |
| POST | /api/simulation/stop | 停止模拟 |
| GET | /api/simulation/status | 模拟状态 |
| GET | /api/storage/datasets | Lance数据集列表 |
| GET | /api/storage/tables | Iceberg表列表 |
| GET | /api/gravitino/catalogs | Gravitino Catalogs |
| GET | /api/gravitino/schemas/{catalog} | Catalog下的Schemas |

### 6.2 WebSocket 实时流

```
ws://host:8888/ws/realtime
```

- 客户端发送文本 → 服务端实时分析并返回
- `/api/simulation/start` 启动后自动推送三条演示通话

## 7. 后续扩展场景（MVP 后）

| 场景 | 数据源 | 处理方式 | 存储 |
|------|--------|---------|------|
| 监控摄像头 vs 信令人数比对 | 摄像头视频流 + 信令日志 | Ray 分布式比对 | Lance + Iceberg |
| 身份证件结构化比对 | 证件图像 | 清洗 → 标注 → 去毒 → OCR | Lance 向量 |
| 闸机过车数量统计 | 闸机日志 | 时序数据对齐 | Iceberg 聚合 |

## 8. 部署要求

- Python 3.10+
- 依赖：fastapi, uvicorn, websockets
- 可选：MinIO（对象存储）、Ray（分布式）、Gravitino（元数据）
- 演示模式：核心存储组件（Lance/Iceberg/S3）使用真实库，ASR/LLM 使用模拟/降级实现
