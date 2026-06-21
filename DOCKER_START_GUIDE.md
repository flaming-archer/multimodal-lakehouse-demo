# 多模态湖仓处理平台 — Docker 启动指南

## 服务状态

| 服务 | 端口 | 说明 |
|------|------|------|
| 前端 + API | http://localhost:8888 | FastAPI + 静态页面 |
| Gravitino 元数据 | http://localhost:8090 | Apache Gravitino 1.1.1 |
| MinIO 控制台 | http://localhost:9001 | S3 兼容存储 (minioadmin/minioadmin) |
| Ray Dashboard | http://localhost:8265 | 分布式计算监控 |

## 启动命令

```bash
cd multimodal-lakehouse-demo
docker compose up -d
```

## 停止命令

```bash
docker compose down
```

## 查看日志

```bash
docker compose logs -f backend    # 后端日志
docker compose logs -f gravitino  # Gravitino 日志
```

## 验证所有服务

```bash
curl http://localhost:8888/api/health
curl http://localhost:8090/api/version
```

## 重置数据（可选）

```bash
docker compose down -v   # 清除所有数据卷
docker compose up -d     # 重新启动
```

## 技术组件说明

| 组件 | 真实/模拟 | 说明 |
|------|-----------|------|
| Lance | ✅ 真实 pylance | 向量存储，支持语义搜索 |
| Iceberg | ✅ 真实 pyiceberg | ACID 事务，时间旅行 |
| S3/MinIO | ✅ 真实 MinIO | S3 兼容对象存储 |
| Gravitino | ✅ 真实 1.1.1 | 统一元数据管理 |
| Ray | ✅ 真实 2.44.1 | 分布式计算 |
| Daft | ✅ 真实 daft-lance | 离线 ETL 编排引擎 |
| LLM Parser | ✅ 真实规则引擎 | 意图/情绪/风险提取 |
