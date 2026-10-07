# resilient-upstream-gateway

对接不稳定上游接口的弹性网关：把超时、连接失败、上游 4xx/5xx 等失败归一成
可区分的错误结论，配合重试、熔断、限流、并发名额（舱壁）、幂等键与多版本兼容，
绝不把底层库（httpx 等）的异常原样透给调用方。**支持同时登记多个带名字的上游，
每个上游的配置与运行状态完全隔离。**

## 结构

```
gateway/
  errors.py           错误归一化：底层异常 -> 可区分的 GatewayError
  circuit_breaker.py  熔断器：关闭/打开/半开三态，线程安全
  rate_limiter.py     令牌桶限流
  concurrency.py      并发名额限制（舱壁）：reject/wait 两种溢出策略
  retry.py            指数退避 + 抖动的重试策略
  idempotency.py      幂等键：重复提交复用结论；同键换内容报冲突
  versioning.py       版本协商 + 请求按版本组装校验 + 响应 schema 校验
  client.py           ResilientUpstreamClient：以上能力的组合（单上游）
  registry.py         多上游注册表：按名字路由，配置与状态逐上游隔离
main.py               FastAPI 入口：POST /gateway/{upstream}/{path}（新）
                      与 POST /gateway/{path}（单上游老路径，继续可用）
tests/                单测 + 本地模拟上游（不依赖任何外部服务）
```

## 多上游配置

环境变量 `UPSTREAMS` 为 JSON，键是上游名，值是该上游的完整配置
（对应 `UpstreamConfig` 的字段，全部有默认值，可只写差异项）：

```bash
export UPSTREAMS='{
  "pay":  {"base_url": "http://127.0.0.1:9001", "timeout": 3.0,
           "max_in_flight": 10, "overflow": "reject",
           "failure_threshold": 5, "rate": 50.0, "rate_capacity": 100.0},
  "risk": {"base_url": "http://127.0.0.1:9002", "timeout": 8.0,
           "max_in_flight": 3, "overflow": "wait", "max_wait": 0.5,
           "max_attempts": 2, "recovery_timeout": 60.0}
}'
uv run python main.py
```

未设置 `UPSTREAMS` 时回退到单上游：`UPSTREAM_BASE_URL`（默认
`http://127.0.0.1:9000`），名字为 `default`。

每个上游可独立配置的项（`UpstreamConfig`）：

| 配置项 | 默认 | 说明 |
|---------------------|----------|--------------------------------|
| `base_url` / `timeout` | — / 5.0 | 上游地址与单次调用超时 |
| `max_attempts` 等 `retry_*` | 3 次 | 重试次数、退避基数/上限、抖动 |
| `failure_threshold` 等 | 5 / 2 / 30s | 熔断阈值、半开成功阈值、恢复时长 |
| `rate` / `rate_capacity` | 50 / 100 | 令牌桶限速与囤积上限 |
| `max_in_flight` | 20 | 该上游允许同时在飞的请求数 |
| `overflow` / `max_wait` | reject / 0 | 名额满：立刻报容量满，或最多等 `max_wait` 秒 |

代码侧等价用法：

```python
from gateway.registry import UpstreamConfig, UpstreamRegistry

registry = UpstreamRegistry()
registry.register(UpstreamConfig(name="pay", base_url="http://pay:9001"))
registry.register(UpstreamConfig(name="risk", base_url="http://risk:9002",
                                 max_in_flight=3, overflow="wait", max_wait=0.5))
registry.call("pay", "/pay", payload={"amount": 100, "currency": "CNY"})
```

也可以把调用方自己按老方式构造的客户端直接登记进来（见下文“向后兼容”）：

```python
registry.register_client("pay", your_client)
# 或一步到位
registry = UpstreamRegistry.with_client("default", your_client)
```

## 向后兼容（单上游老调用方式不丢）

多上游是在旁边新增能力，不顶掉原来的用法：

- **老式注入照常可用**：单上游时代直接
  `create_app(ResilientUpstreamClient(base_url=...))` 的构造/注入方式保留，
  该客户端会被包成名为 `default` 的唯一上游，行为与升级前一致。
- **老 HTTP 路径保留**：`POST /gateway/{path}`（含 `/gateway/orders/create`
  这类多段老路径）仍然可用，路由到 `default` 上游。它与新路径
  `POST /gateway/{upstream}/{path}` 在同一个入口上按第一段是否为**已登记的
  上游名**消歧：命中才按多上游解析，否则整段都当老路径。因此只要不把上游
  起名成业务路径的第一段，两种调用方式互不干扰；注册表里没有 `default` 时，
  老路径会得到明确的 `unknown_upstream`（HTTP 404），而不是行为错乱。
- **老健康字段保留**：存在 `default` 上游时，`GET /health` 除了新的
  `upstreams` 分上游视图，还带老版本的顶层 `circuit` 字段。
- **直接使用 `ResilientUpstreamClient` 的代码无需改动**：构造签名保持兼容，
  仅新增了可选的 `name` 参数（由注册表登记时自动回填）。

## 隔离与配额规则

- **逐上游独立组件**：每个上游独享 httpx 客户端（独立地址/超时）、重试策略、
  熔断器、令牌桶、并发名额、幂等存储。某个上游挂了或一直超时，只会熔断、
  限流、占满**它自己**的额度，别的上游照常服务。
- **并发名额（舱壁）**：每个上游限制同时在飞请求数 `max_in_flight`。名额在
  进入调用时获取，无论成功、失败还是中途被限流/熔断放弃，都在 finally 中
  归还——慢上游不会泄漏名额，也不会占别的上游的名额。溢出策略：
  - `overflow="reject"`：立刻返回 `capacity_full`（HTTP 503），
    与超时（504）/熔断（503 但 `circuit_open`）是可区分的类别；
  - `overflow="wait"`：最多等 `max_wait` 秒，等到名额就继续，等不到才报
    `capacity_full`，不会无限堆积拖垮内存。
- **健康检查按上游分别上报**，不给笼统的整体状态：
  - `GET /health` → `{"status": "ok", "upstreams": {"pay": {...}, "risk": {...}}}`
  - `GET /health/{upstream}` → 单个上游的
    `{"circuit": ..., "in_flight": ..., "max_in_flight": ..., "available_tokens": ...}`

## 错误映射

所有失败都归一为 `GatewayError` 子类，HTTP 层返回统一错误体
`{"error": <类别>, "message": ..., "retryable": ...}`：

| 场景                     | 错误类别 (`error` 字段)   | HTTP 状态码 | 可重试 |
|--------------------------|---------------------------|-------------|--------|
| 上游超时                 | `timeout`                 | 504         | 是     |
| 连接失败（拒绝/DNS/重置）| `connection`              | 502         | 是     |
| 上游 4xx                 | `upstream_4xx`            | 上游状态码  | 否     |
| 上游 5xx                 | `upstream_5xx`            | 502         | 是     |
| 熔断打开（快速失败）     | `circuit_open`            | 503         | 稍后   |
| 本地限流拒绝             | `rate_limited`            | 429         | 稍后   |
| 该上游并发名额已满       | `capacity_full`           | 503         | 稍后   |
| 调用了未登记的上游       | `unknown_upstream`        | 404         | 否     |
| 同一幂等键换了请求内容   | `idempotency_conflict`    | 409         | 否     |
| 不支持的接口版本         | `version_unsupported`     | 400         | 否     |
| 请求不符合所对接版本     | `invalid_request`         | 400         | 否     |
| 响应字段缺失/结构不符    | `schema_mismatch`         | 502         | 否     |
| 响应不是合法 JSON/非对象 | `invalid_response`        | 502         | 否     |
| 未分类异常（兜底）       | `unknown`                 | 500         | 否     |

底层异常类型名只放在内部 `detail` 字段，不出现在对外的 `error`/`message` 中；
JSON 解析等底层异常也绝不原样抛给调用方。

**非法响应的判定先于状态码**：只要响应体压根不是合法 JSON（比如上游直接
甩回一坨 HTML 错误页），或顶层不是 JSON 对象，无论它的 HTTP 状态码是多少
——哪怕顶着 500——都归为独立的 `invalid_response`（HTTP 502），**不会被当成
上游 5xx 反复重试**，也不计入熔断失败。它与超时、连接失败、4xx、5xx、
`schema_mismatch`（结构对得上但缺字段）都是互不混淆的结论。

## 重试与熔断规则

**重试**（`RetryPolicy`）：只对可重试错误（超时、连接失败、上游 5xx）重试，
4xx、请求/响应校验问题（`invalid_request` / `schema_mismatch` /
`invalid_response`，包括顶着 5xx 的 HTML 错误页）一律不重试。退避公式
`min(max_delay, base_delay * 2^(n-1)) * (1 ± jitter)`，默认最多 3 次尝试。

**熔断**（`CircuitBreaker`）三态切换：

- **关闭**：正常放行；连续失败达到 `failure_threshold`（默认 5）切到打开。
- **打开**：快速失败，请求不打到上游；经过 `recovery_timeout`（默认 30s）后
  下一个请求触发半开。
- **半开**：同一时刻只放行一个探针；探针连续成功 `success_threshold`
  （默认 2）次回到关闭；任何一次失败立即回到打开并重新计时。

所有状态迁移在同一把锁内完成，并发下状态不会被写乱；
`time_func` 可注入假时钟，恢复重试时机完全可控（见单测）。
限流（令牌桶）与并发名额在熔断之前执行，被拦下的请求不会打到上游。

## 幂等

调用方在 `Idempotency-Key` 头携带幂等键：

- 首个请求真正执行；执行期间的并发重复提交阻塞等待并拿到相同结论；
- 完成后的结论（成功或失败）在 TTL（默认 300s）内缓存，重复提交直接复用；
- **冲突判定覆盖整个请求身份**，指纹 = 目标上游 + 目标接口(path) + 接口版本
  + 请求报文（规范化 JSON）。同一幂等键只要换了其中任意一项——换上游、换接口、
  换版本（未指定版本与显式指定默认版本视为同一份请求）、换报文——都明确返回
  `idempotency_conflict`（HTTP 409），绝不默默把上一次的结论丢回来；
- 冲突请求不会打到上游，也不会污染原键：报过冲突之后，用原来的完整请求
  重复提交仍能拿到原结论；owner 执行期间并发撞入的不同请求会等待 owner
  结论落定后再报冲突，并发下不会把原结论弄坏；
- 因此重试或重复提交不会造成重复副作用。
- 每个上游默认独享幂等存储（跨上游天然隔离）；若显式给多个客户端注入同一
  存储，指纹里包含上游名，同键跨上游仍会判冲突而不是串结论。

## 版本兼容

- 支持上游接口版本 `v1`、`v2`；`X-Api-Version` 头缺省用 `v2`；
- 不支持的版本直接返回 `version_unsupported`，不发请求；
- **请求侧**：报文按所对接版本组装校验——
  - v1 请求必填 `amount`，允许 `memo`，不支持 `currency`；
  - v2 请求必填 `amount`、`currency`，允许 `memo`；
  - 字段缺失或字段不被该版本支持都返回 `invalid_request`（HTTP 400），
    并列出缺失/多余字段，请求不会打到上游；
- **响应侧**：v1 响应必填 `id/status/amount`，v2 额外必填 `currency`；
  缺字段返回 `schema_mismatch` 并列出缺失字段，绝不默默取默认值；
  响应不是合法 JSON 或顶层不是对象返回 `invalid_response`（与状态码无关，
  顶着 5xx 的 HTML 错误页同样如此，且不重试），与超时/4xx/5xx/
  `schema_mismatch` 都是可区分的类别；
- 归一化输出显式携带 `version` 字段，v1 没有的字段不会出现在输出里。

请求/响应兼容范围一览：

| 版本 | 请求必填 | 请求允许 | 响应必填 | 响应可选 |
|------|----------|----------|----------|----------|
| v1 | `amount` | `amount`, `memo` | `id`, `status`, `amount` | — |
| v2（默认） | `amount`, `currency` | `amount`, `currency`, `memo` | `id`, `status`, `amount`, `currency` | `memo` |

## 验证方法

全部验证基于本地模拟上游（`tests/mock_upstream.py`，httpx MockTransport），
不依赖任何外部服务：

```bash
uv run pytest tests/ -v
```

`-v` 下每个用例打印 `[业务域] PASS 用例名`，会话结束按业务域汇总通过数。
覆盖范围：

- `tests/test_errors.py`：底层异常到各类错误的映射；
- `tests/test_circuit_breaker.py`：三态切换、半开探针互斥、并发下状态一致性；
- `tests/test_client.py`：正常调用、超时/连接失败/4xx/5xx 归类、重试恢复、
  熔断快速失败与恢复、限流、幂等去重（含 20 并发同键只产生 1 次副作用）；
- `tests/test_concurrency.py`：并发名额——reject/wait 两种溢出策略、
  成功/失败（非法 JSON/结构缺失/4xx/超时）/中途放弃/等待超时放弃后名额
  正确归还、30 并发打 2 个名额不堆积；
- `tests/test_registry.py`：多上游隔离——一个上游熔断/限流/占满名额/卡慢
  不影响其他上游，同幂等键跨上游各执行一次/共享存储时跨上游报冲突，
  同键换接口/换版本/换报文在注册表入口报冲突，老式客户端登记可用，
  健康状态逐上游分别上报；
- `tests/test_semantics.py`：同幂等键换接口/换版本/换报文报冲突且不污染
  原键（含 owner 执行期间并发撞入）、请求按 v1/v2 组装校验、
  非法 JSON/顶层非对象归 `invalid_response`、顶着 5xx 的 HTML 错误页同样归
  `invalid_response` 且不重试、缺字段仍归 `schema_mismatch`、解析异常不外泄；
- `tests/test_app.py`：HTTP 层多上游路由、统一错误体、状态码、幂等头、
  版本头、分上游健康端点，以及单上游老路径/老式客户端注入/老健康字段的
  向后兼容验证。

本地启动网关（多上游）：

```bash
export UPSTREAMS='{"pay": {"base_url": "http://127.0.0.1:9000"}}'
uv run python main.py
curl -X POST localhost:8000/gateway/pay/pay \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' -H 'X-Api-Version: v2' \
  -d '{"amount": 100, "currency": "CNY"}'
curl localhost:8000/health        # 每个上游分别的状态
curl localhost:8000/health/pay    # 单个上游的状态
```

本地启动网关（单上游老方式，路由与注入都不变）：

```bash
export UPSTREAM_BASE_URL='http://127.0.0.1:9000'   # 不设 UPSTREAMS 即回退
uv run python main.py
curl -X POST localhost:8000/gateway/pay \
  -H 'Content-Type: application/json' \
  -d '{"amount": 100, "currency": "CNY"}'          # 老路径照常可用
```
