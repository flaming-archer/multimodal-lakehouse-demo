# 图片场景前端端到端测试交接

## 目标

验证顶部“图片处理”工作区的完整用户流程，而不只是调用后端 API：

1. 页面能进入图片处理页签并正确显示模型状态。
2. 本地规则模式可以运行图片入库、头像合规分析和 ChineseCLIP 向量生成。
3. 完整流水线能通过 SSE 持续更新进度并正常结束。
4. 中文文本搜图和标量筛选能正确渲染结果卡片及图片。
5. 未配置或已配置 VLM 时，页面行为分别符合预期。

## 重要结论：测试前必须启动 Server

必须启动 FastAPI Server。它同时负责：

- 在 `http://127.0.0.1:8888/` 提供前端页面；
- 提供 `/api/image/*` 图片处理 API；
- 提供 `/api/image/run-all-stream` SSE 流；
- 提供 `/api/image/assets/{doc_id}` 图片预览。

直接打开 `frontend/index.html` 不能完成端到端测试。

## 当前环境和约束

- 项目目录：`/Users/fanng/opensource/multimodal-lakehouse-demo`
- 前端地址：`http://127.0.0.1:8888/`
- API 文档：`http://127.0.0.1:8888/docs`
- 图片数据：`data/images/manifest.json`，共 15 条记录，其中 13 张可解码证件照场景和 2 个文件错误样本。
- 默认合规后端：本地规则（InsightFace SCRFD + OpenCV 清晰度）。
- 向量模型：`OFA-Sys/chinese-clip-vit-base-patch16`，输出 512 维向量。
- 首次真实运行会下载 InsightFace 模型和约 1.4 GB ChineseCLIP 模型，不能把首次模型加载时间当作前端卡死。
- Codex CLI 没有 in-app Browser/Chrome 插件后端。CLI agent 应使用 Playwright/Chromium；有桌面 Browser 能力的 agent 可直接操作页面。
- 不需要启动 Daft 或 Gravitino，图片场景后端不依赖它们。

## 启动步骤

在项目根目录安装依赖（已安装时跳过）：

```bash
python -m pip install -r requirements.txt
```

启动服务：

```bash
cd /Users/fanng/opensource/multimodal-lakehouse-demo/backend
python main.py
```

保持该进程运行，在另一个终端执行预检：

```bash
curl -fsS http://127.0.0.1:8888/api/image/status
curl -fsS http://127.0.0.1:8888/
```

两条命令都成功后再启动浏览器测试。如果 8888 端口已被占用，应先确认是否已有本项目 Server 在运行，不要直接终止未知进程。

## 测试工具建议

优先顺序：

1. 有 in-app Browser 能力：直接打开 `http://127.0.0.1:8888/`。
2. Codex CLI：使用 Playwright 的 Chromium，建议开启截图、console 监听、失败 trace。
3. 无自动化浏览器：可人工执行本清单，但必须保存截图并记录浏览器控制台和 Network 结果。

项目当前没有 `package.json` 或既有 Playwright 测试框架。不要为了测试无意改动业务文件；如需落地长期 Playwright 测试，先单独说明新增依赖和文件。

## 测试前清理与证据目录

每轮测试使用新的证据目录，例如：

```text
/tmp/multimodal-frontend-e2e/
├── screenshots/
├── traces/
├── console.log
├── network.log
└── result.md
```

页面测试开始时记录：

- Git commit 或 `git status --short`；
- Python 和浏览器版本；
- Server 启动命令与关键环境变量（密钥必须脱敏）；
- `/api/image/status` 返回；
- 是否为模型首次下载/加载。

不要删除或覆盖用户已有的 Lance 数据、模型缓存或未提交代码。

## 核心测试用例

### E2E-01 页面加载与导航

步骤：

1. 打开 `http://127.0.0.1:8888/`。
2. 点击顶部“图片处理”。

验收：

- 页面没有白屏或明显布局错位。
- 出现“图片头像合规与文本搜图”。
- 合规后端默认选择“本地规则 · SCRFD + 清晰度”。
- 模型状态由“正在读取模型状态...”更新为 ChineseCLIP/VLM 状态。
- 浏览器 console 没有未处理异常。
- `GET /api/image/status` 返回 200。

截图：`01-image-tab.png`。

### E2E-02 分步执行及顺序约束

步骤：

1. 点击“1. 图片入库”。
2. 等待控制台显示 ingest 完成。
3. 点击“2. 合规分析”。
4. 等待结果卡片出现。
5. 点击“3. 生成向量”。

验收：

- 入库完成后共有 15 条 Manifest 记录。
- 合规分析后出现汇总和图片卡片。
- `corrupt_image` 显示解码失败，`missing_image` 显示图片缺失/下载失败，且均不能显示为合规头像。
- 正常图片展示人脸数、人脸占比、清晰度、头像置信度和判断原因。
- 生成向量阶段对 13 张可解码/存在的图片生成真实向量；两个坏样本保持行级错误。
- 任一步骤运行时发生错误，前端控制台和 toast 应显示可理解的失败信息，不应伪装成功。

截图：`02-local-analysis.png`、`03-embedding-done.png`。

说明：若直接在空状态执行分析、向量或查询，后端可能返回 409 状态错误；前端必须展示错误且保持可继续操作。

### E2E-03 本地规则完整流水线与 SSE

步骤：

1. 保持合规后端为“本地规则”。
2. 点击“启动图片流水线”。
3. 观察四个阶段和逐图片进度。
4. 等待按钮变为“重新运行图片流水线”。

验收：

- 按钮运行期间 disabled，避免重复提交。
- 控制台依次出现 `[1/4]` 入库、`[2/4]` 合规分析、`[3/4]` 图片向量、`[4/4]` 中文文本搜图。
- SSE 结束后显示总耗时，按钮恢复可用，并出现完成 toast。
- 页面最终展示 15 条分析记录；两个标准证件照应为合规基线，13 张有效图片应生成向量。遮挡、侧脸、裁切和曝光场景重点比较本地规则与 VLM 的差异。
- 两个错误样本不计入合规头像。
- Network 中 SSE 请求保持连接至 `done`，没有意外断流。

截图：`04-pipeline-progress.png`、`05-pipeline-done.png`。

备注：头像数量基线与当前固定样本和模型对应。如果模型版本变化导致结果变化，应记录模型版本、具体差异和检测指标，不要直接修改断言掩盖问题。

### E2E-04 中文文本搜图

前置：E2E-02 的向量步骤或 E2E-03 已成功完成。

步骤：

1. 选择“中文文本搜图”。
2. 依次测试“脸部模糊”“戴口罩”“戴墨镜”“手遮脸”“多人”快捷查询。
3. 默认验证 Top 3，并补充测试 Top 5 和 Top 10。

验收：

- 查询期间显示 loading，成功后 loading 消失。
- 返回数量不超过所选 Top K。
- 每张卡片包含排名、图片、doc_id、描述和向量距离。
- 检查 Top 结果是否与查询语义对应，并记录标准照、模糊、遮挡和多人查询的 Top 1。
- 图片资源请求返回 200；坏样本没有破图占位以外的异常。
- console 中没有 HTML/JavaScript 异常。

截图：`06-text-id-standard.png`、`07-text-id-occluded.png`。

### E2E-05 标量筛选

步骤：

1. 选择“标量筛选”，确认文本输入隐藏、where 输入显示。
2. 点击快捷条件“合规”，其条件应为 `is_avatar = true`。
3. 点击快捷条件“处理失败”，其条件应为 `analysis_status != 'ok'`。

验收：

- 合规筛选只返回 `is_avatar = true` 的记录。
- 处理失败筛选至少覆盖 `corrupt_image` 和 `missing_image`。
- 无匹配条件时显示“没有匹配图片”，而不是空白区域。
- 非法过滤表达式返回错误时，页面显示“检索失败：...”且不会残留 loading。

截图：`08-scalar-avatar.png`、`09-scalar-failed.png`。

### E2E-06 VLM 未配置

仅在没有设置 `IMAGE_VLM_API_KEY`/`IMAGE_VLM_MODEL` 时执行。

步骤：

1. 确认模型状态显示 VLM 未配置及缺少的配置项。
2. 选择“视觉大模型 · OpenAI Compatible”。
3. 点击合规分析或启动完整流水线。

验收：

- 请求失败且页面给出明确错误，不得静默回退到本地规则。
- 页面按钮恢复可用，不应永久停留在“运行中...”。
- 后端单步接口应返回 503；SSE 路径应发送 `error` 事件。

截图：`10-vlm-not-configured.png`。

### E2E-07 VLM 已配置（可选）

启动 Server 前配置：

```bash
export IMAGE_VLM_API_KEY='***'
export IMAGE_VLM_BASE_URL='https://dashscope.aliyuncs.com/compatible-mode/v1'
export IMAGE_VLM_MODEL='qwen-vl-max'
```

验收：

- 页面模型状态展示已配置的 VLM 模型。
- 结果卡片的分析后端为 VLM。
- VLM 单张失败时该行标记为 `llm_failed`，不得回退为本地规则结果。
- 可解码图片即使合规分析失败，后续仍可生成图片向量。
- 密钥不得出现在截图、日志、trace 或测试报告中。

### E2E-08 重复提交、刷新和断连

步骤：

1. 启动完整流水线，在运行中尝试触发另一个图片写任务。
2. 在 SSE 运行中刷新或关闭测试页面。
3. 立即请求新的写任务；等待原后台任务完成后再次请求。

验收：

- 并发任务收到“图片流水线正在执行，请稍后重试”或 HTTP 409。
- SSE 客户端断开不会提前释放后端互斥锁。
- 原后台任务结束后锁被释放，新任务可以执行。
- 页面重开后能重新读取 `/api/image/status`，没有永久卡死状态。

### E2E-09 基本响应式与可用性

至少测试桌面 `1440x900` 和移动端 `390x844`：

- 页签和按钮可点击，不被其他元素覆盖。
- 四步流程、搜索表单和结果卡片允许合理换行/滚动。
- 页面无明显横向溢出导致关键控件不可访问。
- 图片使用合适比例展示，错误图片有明确占位。
- 键盘可聚焦下拉框、输入框和按钮。

截图：`11-desktop.png`、`12-mobile.png`。

## Network 与 Console 检查

测试期间至少确认：

| 请求 | 预期 |
| --- | --- |
| `GET /api/image/status` | 200 JSON |
| `POST /api/image/ingest` | 200，正常入库 |
| `POST /api/image/analyze` | 200；VLM 未配置时 503 |
| `POST /api/image/embed` | 200，成功向量数为 8 |
| `POST /api/image/query` | 200；非法输入为明确的 4xx |
| `GET /api/image/run-all-stream?...` | 200 `text/event-stream`，最终 `done` 或明确 `error` |
| `GET /api/image/assets/{doc_id}` | 有效图片 200；不存在资源为 404 |

失败报告必须附上：

- 操作步骤；
- 实际与预期结果；
- 页面截图；
- console error；
- 相关请求的 URL、方法、状态码和脱敏响应；
- Server traceback 或关键日志；
- 是否可稳定复现。

## 测试结束后的最小回归

前端 E2E 完成后运行已有后端单元测试：

```bash
cd /Users/fanng/opensource/multimodal-lakehouse-demo
python -m unittest discover -s tests -p 'test_*.py'
```

当前基线：13 个测试通过。若数量变化，以实际测试发现数为准，但必须说明新增、减少或失败原因。

## 交接输出模板

在测试报告中使用以下格式：

```markdown
# 图片前端 E2E 测试结果

- 时间：
- commit / 工作区状态：
- Server 启动方式：
- 浏览器与版本：
- 本地/VLM 模式：
- 模型是否首次加载：

## 汇总

- Pass：
- Fail：
- Blocked：

## 用例结果

| 用例 | 结果 | 证据 | 备注 |
| --- | --- | --- | --- |
| E2E-01 | PASS/FAIL/BLOCKED | 截图/trace | |

## 缺陷

### 标题

- 严重级别：Blocker/Critical/Major/Minor
- 复现步骤：
- 预期：
- 实际：
- console/network/server 证据：

## 未覆盖范围

- ...
```

## 完成标准

只有同时满足以下条件才可声明前端测试完成：

- 核心用例 E2E-01 至 E2E-06 有明确结果；
- 本地完整流水线至少成功运行一次；
- 文本搜图和标量筛选均通过页面交互验证；
- 保存关键截图以及 console/network 证据；
- 所有失败均有可复现记录，Blocked 项说明具体外部依赖；
- 不以 API 单测通过代替前端端到端测试。
