# UDP 多端口出口 · 运维演练

FastAPI 演练页 + 真实 UDP 网关。三个本地客户端向两个回显服务发包，网关默认带 **600 个上游出口端口**
（`17000-17599`），按「客户端 + 上游」识别会话，处理端口复用、1 秒会话失效、2 秒迟到回包保留。
收发单包开销不随会话数/端口数线性增长。

## 启动

```bash
docker compose up --build
# 打开 http://127.0.0.1:8000
```

三个容器：`gateway`（网页 + UDP 网关 + 三个本地客户端）、`echo-a`、`echo-b`。
默认 600 个出口端口；想复现原来的双端口演练：

```bash
EGRESS_PORTS=17000,17001 docker compose up --build
```

## 规则

| 规则 | 实现 |
| --- | --- |
| 会话识别 | `(客户端 IP, 客户端端口, 上游)` |
| 出口端口 | 默认 600 个（`17000-17599`，支持 `端口,列表` 与 `起始-结束` 区间）；**同一上游同一时刻每个端口只能有 1 个会话**；不同上游可共用端口，同一端口在两个上游上的占用与保留期各自独立计算 |
| 端口选择 | 新会话按**配置顺序**取首个可用端口（每上游一个空闲端口小顶堆，`heappop` 即得） |
| 已有会话 | 沿用原出口端口，并刷新最后转发时间 |
| 会话失效 | 最后一次向上游转发后 1 秒；收发路径按最后转发时间即时判定，不依赖后台清理是否及时 |
| 组合保留 | 自**应失效时刻**起 `(原出口端口, 该上游)` 再保留 2 秒；期间该组合的回包丢弃，端口可分配给**其他上游** |
| 回包投递 | 按 `(到达的出口端口, 来源上游)` 经反向索引 O(1) 找到活动会话，只发给该会话的客户端 |
| 无可用端口 | 不向上游转发，逐个端口报明原因（被谁占用/保留期剩余），网关回 `gateway_error` |

端口、时长可通过环境变量覆盖（见 `compose.yaml`）。

## 性能设计（`app/gateway_core.py`）

* `(出口端口, 上游) → 会话` 反向索引：回包投递、占用判定都是 O(1) 字典查询。
* 每个上游一个「空闲端口下标」小顶堆：按配置顺序取首个可用端口 = 一次 `heappop`。
* 到期由 `(到期时刻, 序号, 类型, 键)` 小顶堆驱动：每次只弹出**实际到期**的条目，
  开销与到期项数量同阶，与总会话数无关。
* 续期只压入新计时条目、旧条目弹出时按「最后转发时间 + 空闲时长」校验作废；
  堆超过 `2 × (会话数 + 保留项数) + 32` 时重建，**频繁续期下计时记录量仍与存活对象同阶**。
* 只有「逐个列出拒绝原因」和「渲染状态快照」才会遍历全部端口。

## 结构

```
app/gateway_core.py    会话/端口/保留期状态机（无 socket，可用假时钟测试）
app/gateway.py         asyncio UDP 入口、出口（每端口一个真实套接字）、本地客户端
app/echo_service.py    带 0–2 秒回包延迟的回显服务
app/main.py            FastAPI 页面与 JSON API
app/static/index.html  演练页（2 Hz 轮询 /api/state；数百端口时默认折叠空闲端口）
tests/                 14 个规则单测 + 16 个规模/懒到期/边界单测 + 6 个真实 UDP 集成测试
scripts/verify_api.py  对运行中的服务跑场景脚本（按端口数自适应）
scripts/bench_scale.py 不同规模下的热路径基准
scripts/check_compose.py 无 Docker 环境下校验 compose/Dockerfile 与应用接线一致
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q                    # 规则单测 + 规模单测 + 本机真实 UDP 集成
python3 -I scripts/bench_scale.py       # 64/256/1024 端口热路径基准
python3 -I scripts/verify_api.py        # 需先启动服务
```

## API

`GET /healthz` · `GET /api/state` · `GET /api/events` · `POST /api/send` ·
`POST /api/send-batch` · `POST /api/reset` · `GET /api/client/{id}`
