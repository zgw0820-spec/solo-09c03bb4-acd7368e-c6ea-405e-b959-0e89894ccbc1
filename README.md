# UDP 双端口出口 · 运维演练

FastAPI 演练页 + 真实 UDP 网关。三个本地客户端向两个回显服务发包，网关只有两个上游出口端口，
按「客户端 + 上游」识别会话，处理端口复用、1 秒会话失效、2 秒迟到回包保留。

## 启动

```bash
docker compose up --build
# 打开 http://127.0.0.1:8000
```

三个容器：`gateway`（网页 + UDP 网关 + 三个本地客户端）、`echo-a`、`echo-b`。

## 规则

| 规则 | 实现 |
| --- | --- |
| 会话识别 | `(客户端 IP, 客户端端口, 上游)` |
| 出口端口 | 共 2 个（默认 17000/17001）；**同一上游同一时刻每个端口只能有 1 个会话**；不同上游可共用端口 |
| 已有会话 | 沿用原出口端口，并刷新最后转发时间 |
| 会话失效 | 最后一次向上游转发后 1 秒 |
| 组合保留 | 失效后 `(原出口端口, 该上游)` 再保留 2 秒；期间该组合的回包丢弃，端口可分配给**其他上游** |
| 回包投递 | 按 `(到达的出口端口, 来源上游)` 找活动会话，只发给该会话的客户端 |
| 无可用端口 | 不向上游转发，网关回一条 `gateway_error`，网页在该客户端卡片与事件日志中显示原因 |

端口、时长可通过环境变量覆盖（见 `compose.yaml`）。

## 结构

```
app/gateway_core.py    会话/端口/保留期状态机（无 socket，可用假时钟测试）
app/gateway.py         asyncio UDP 入口、出口、本地客户端
app/echo_service.py    带 0–2 秒回包延迟的回显服务
app/main.py            FastAPI 页面与 JSON API
app/static/index.html  演练页（2 Hz 轮询 /api/state）
tests/                 14 个规则单测 + 5 个真实 UDP 集成测试
scripts/verify_api.py  对运行中的服务跑场景脚本
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q                    # 规则单测 + 本机真实 UDP 集成
python3 -I scripts/verify_api.py        # 需先启动服务
```

## API

`GET /healthz` · `GET /api/state` · `GET /api/events` · `POST /api/send` ·
`POST /api/send-batch` · `POST /api/reset` · `GET /api/client/{id}`
