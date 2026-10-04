# resilient-upstream-gateway

把几个脾气各不相同的第三方接口挂在同一个网关后面：对每个**带名字的上游**分别配置
地址、超时、重试、熔断、限流与并发名额，并统一做错误归一化、重试、熔断、限流、
并发隔离、幂等键与多版本兼容。某个上游超时/熔断/爆满，不会连累其他上游；
底层库（httpx 等）的异常也绝不原样透给调用方。

## 结构

```
gateway/
  errors.py           错误归一化：底层异常 -> 可区分的 GatewayError
  circuit_breaker.py  熔断器：关闭/打开/半开三态，线程安全
  rate_limiter.py     令牌桶限流（每上游独立）
  bulkhead.py         并发隔离舱：每上游在飞名额 + 有界等待队列
  retry.py            指数退避 + 抖动的重试策略
  idempotency.py      幂等键：重复提交复用首个结论；同键换内容报冲突
  versioning.py       版本协商、按版本组装/校验请求、响应 schema 校验
  client.py           ResilientUpstreamClient：以上能力的组合（单个上游）
  registry.py         UpstreamRegistry：登记多个命名上游，各自独立组件与健康状态
main.py               FastAPI 入口：命名上游路由 / 默认路由 / 逐上游健康检查
tests/                单测 + 本地模拟上游（不依赖任何外部服务）
```

## 多上游配置

### 代码内登记

```python
from gateway.registry import UpstreamConfig, UpstreamRegistry

registry = UpstreamRegistry()
registry.register(UpstreamConfig(
    name="payment", base_url="http://10.0.0.1:9000",
    timeout=3.0, max_attempts=2,
    failure_threshold=5, recovery_timeout=30.0,
    rate_limit_rate=100.0, rate_limit_capacity=200.0,
    max_concurrent=16, max_waiters=32, acquire_timeout=2.0,
))
registry.register(UpstreamConfig(
    name="risk", base_url="http://10.0.0.2:9000",
    max_concurrent=8, max_waiters=0,   # 满了立刻失败，不排队
))

client = registry.get("payment")       # 未登记 -> UpstreamNotFoundError
result = client.call("/pay", version="v2", payload=payload,
                     idempotency_key="order-123")
```

### 通过环境变量配置（HTTP 服务）

`GATEWAY_UPSTREAMS` 是一个 JSON 数组，每项对应一个命名上游：

```bash
export GATEWAY_UPSTREAMS='[
  {"name": "payment", "base_url": "http://10.0.0.1:9000", "timeout": 3.0,
   "max_concurrent": 16, "max_waiters": 32, "rate_limit_rate": 100.0},
  {"name": "risk", "base_url": "http://10.0.0.2:9000",
   "failure_threshold": 3, "max_concurrent": 8, "max_waiters": 0}
]'
uv run python main.py
```

未设置 `GATEWAY_UPSTREAMS` 时退化为单个名为 `default` 的上游
（地址取 `UPSTREAM_BASE_URL`），保持向后兼容。

`UpstreamConfig` 全部字段：

| 字段 | 默认 | 含义 |
|------|------|------|
| `name` / `base_url` | — | 上游名字与地址（必填） |
| `timeout` | 5.0 | 单次请求超时（秒） |
| `max_attempts` / `base_delay` / `max_delay` | 3 / 0.1 / 2.0 | 重试次数与退避 |
| `failure_threshold` / `success_threshold` / `recovery_timeout` | 5 / 2 / 30.0 | 熔断阈值、半开成功数、恢复时间 |
| `rate_limit_rate` / `rate_limit_capacity` | 50 / 100 | 令牌桶补充速率（个/秒）与容量 |
| `max_concurrent` | 20 | 该上游**同时在飞**请求数上限 |
| `max_waiters` | 50 | 名额满后允许排队的请求数；`0`=立即拒绝，`-1`=排队不限 |
| `acquire_timeout` | 5.0 | 排队等名额的最长时间；`null`=一直等 |

## 隔离与配额规则

每个命名上游拥有**各自独立的一套实例**：熔断器、令牌桶、并发隔离舱、幂等存储
都是按上游 new 出来的，互不共享。因此：

- A 连续超时/5xx 只会把 **A 的熔断器**打开，B 照常放行；
- A 把自己的令牌用光不会减少 B 的令牌；
- A 的慢请求占的是 **A 自己的并发名额**，B 的请求不会为它排队，也不会被拖慢。

**并发隔离舱（Bulkhead）**：

- 有名额立即放行；名额满时按配置选择：排队等待（受 `acquire_timeout` 限制）
  或立即给出 `capacity_full` 结论（`max_waiters=0`）。
- 等待队列长度有上限（`max_waiters`），队列也满时立即失败，
  **请求不会无限堆积把内存拖垮**。
- 无论调用成功、失败还是中途被放弃，名额都在 `finally` 中归还
  （`Bulkhead.slot()` 上下文管理器保证），慢上游不会一直占着名额。
- `capacity_full` 与 `timeout`、`circuit_open` 是**不同的错误类别**，
  调用方可分别处理（HTTP 层都为 503，但 `error` 字段不同）。

**健康检查分别可见**：`GET /health` 与 `registry.health()` 返回每个上游各自的
熔断态（`circuit`）、在飞数（`in_flight`）、排队数（`waiting`）、
令牌余量（`available_tokens`），不是一个笼统的整体状态。

## 错误映射

所有失败都归一为 `GatewayError` 子类，HTTP 层返回统一错误体
`{"error": <类别>, "message": ..., "retryable": ...}`：

| 场景                     | 错误类别 (`error` 字段)        | HTTP 状态码 | 可重试 |
|--------------------------|--------------------------------|-------------|--------|
| 上游超时                 | `timeout`                      | 504         | 是     |
| 连接失败（拒绝/DNS/重置）| `connection`                   | 502         | 是     |
| 上游 4xx                 | `upstream_4xx`                 | 上游状态码  | 否     |
| 上游 5xx                 | `upstream_5xx`                 | 502         | 是     |
| 熔断打开（快速失败）     | `circuit_open`                 | 503         | 稍后   |
| 本地限流拒绝             | `rate_limited`                 | 429         | 稍后   |
| 该上游并发名额满/太忙    | `capacity_full`                | 503         | 稍后   |
| 同幂等键换了请求内容     | `idempotency_conflict`         | 409         | 否     |
| 不支持的接口版本         | `version_unsupported`          | 400         | 否     |
| 请求缺字段/字段不被版本支持 | `request_invalid`            | 422         | 否     |
| 上游响应不是合法 JSON    | `invalid_json`                 | 502         | 否     |
| 响应字段缺失/结构不符    | `schema_mismatch`              | 502         | 否     |
| 指定了未登记的上游名字   | `upstream_not_found`           | 404         | 否     |
| 未分类异常（兜底）       | `unknown`                      | 500         | 否     |

几点说明：

- 底层异常类型名只放在内部 `detail` 字段，不出现在对外的 `error`/`message` 中；
  响应不是合法 JSON 时也不会把 `JSONDecodeError` 这类解析层原始异常甩给调用方。
- 即使响应是 5xx，只要**响应体无法解析为 JSON**，结论归为 `invalid_json`
  （独立、不重试），而不是笼统地按 5xx 重试。
- `capacity_full`、`timeout`、`circuit_open` 三者 `error` 类别互不相同。

## 重试与熔断规则

**重试**（`RetryPolicy`）：只对可重试错误（超时、连接失败、上游 5xx）重试，
4xx、请求不合规、非法 JSON、结构不符、幂等冲突等不重试。退避公式
`min(max_delay, base_delay * 2^(n-1)) * (1 ± jitter)`，默认最多 3 次尝试。

**熔断**（`CircuitBreaker`）三态切换：

- **关闭**：正常放行；连续失败达到 `failure_threshold`（默认 5）切到打开。
- **打开**：快速失败，请求不打到上游；经过 `recovery_timeout`（默认 30s）后
  下一个请求触发半开。
- **半开**：同一时刻只放行一个探针；探针连续成功 `success_threshold`
  （默认 2）次回到关闭；任何一次失败立即回到打开并重新计时。

一次调用的检查顺序：版本协商 → 请求组装/校验 → 幂等登记（含内容指纹）
→ 取并发名额 → 每次尝试：限流 → 熔断 → 发请求。被限流/熔断/容量拒绝的请求
都不会打到上游。

## 幂等与内容冲突

调用方在 `Idempotency-Key` 头携带幂等键：

- 首个请求真正执行；执行期间的并发重复提交阻塞等待并拿到相同结论；
- 完成后的结论（成功或失败）在 TTL（默认 300s）内缓存，重复提交直接复用；
- 幂等记录与**请求内容指纹**（版本 + 路径 + 确定性序列化的请求体）绑定：
  同一个键若换了请求内容（金额、版本、路径不同等），直接返回
  `idempotency_conflict`（HTTP 409），**不会默默把上一次的结论复用给新内容**，
  旧内容仍可用原请求安全重放；
- 每个上游有独立的幂等空间，不同上游之间同键互不相干。

## 版本兼容

支持上游接口版本 `v1`、`v2`；`X-Api-Version` 头缺省用 `v2`；
不支持的版本返回 `version_unsupported`，不发请求。

**请求侧**：调用方传的是统一的语义字段，网关按目标版本组装线上报文（wire），
缺必填字段或带了该版本不支持的字段都返回 `request_invalid`（不静默丢弃、不发请求）：

| 语义字段 | v1 | v2 |
|----------|----|----|
| `id` | 必填 | 必填 |
| `amount` | 必填 | 必填 |
| `callback_url` | **必填** | 不支持（带了报错） |
| `currency` | 不支持（带了报错） | **必填** |
| `note` | 不支持 | 可选 |

组装后的线上外壳也不同：

- v1（扁平、旧字段名）：
  `{"order_id": <id>, "amount": <amount>, "callback_url": <callback_url>}`
- v2（嵌套支付外壳，备注改名 `memo` 之外保留 `note`）：
  `{"payment": {"id", "amount", "currency"}, "note"?: ...}`

**响应侧**：每个版本声明必填字段（v2 比 v1 多 `currency`），响应缺字段返回
`schema_mismatch` 并列出缺失字段，绝不默默取默认值；响应体不是合法 JSON 返回
`invalid_json`。归一化输出显式携带 `version`，v1 没有的字段不会出现在输出里。

## HTTP 接口

- `POST /upstreams/{name}/call/{path}`：调用指定名字的上游；
  头：`Idempotency-Key`、`X-Api-Version`；body 为语义请求体（按版本校验）。
- `POST /gateway/{path}`：向后兼容入口，固定走 `default` 上游。
- `GET /health`：`{"status": "ok", "circuit": <default 上游熔断态>,
  "upstreams": {<名字>: {base_url, circuit, in_flight, waiting, available_tokens}}}`。

## 验证方法

全部验证基于本地模拟上游（`tests/mock_upstream.py`，httpx MockTransport，
含正常/超时/连接失败/4xx/5xx/缺字段/非法 JSON/可控阻塞等场景），
不依赖任何外部服务：

```bash
uv run pytest tests/ -v
```

每个测试文件开始会打印一行“业务：……”横幅，配合 `-v` 的逐用例 `PASSED`
可清楚看到覆盖了哪些业务及每个用例的通过情况。覆盖文件：

- `tests/test_errors.py`：底层异常到各类错误的映射；
- `tests/test_circuit_breaker.py`：三态切换、半开探针互斥、并发下状态一致性；
- `tests/test_bulkhead.py`：名额耗尽快速失败、有界排队、等待超时、
  在飞数绝不超过配额、成功/失败/放弃都释放名额；
- `tests/test_idempotency_conflict.py`：同键换内容冲突（执行中/完成后/换版本/换路径）、
  TTL 过期后可重新绑定、独立幂等空间；
- `tests/test_request_versioning.py`：v1/v2 请求组装外壳、缺字段/不支持字段拒绝、
  200 与 5xx 非法 JSON 的独立结论；
- `tests/test_multi_upstream.py`：多上游登记、未登记报错、熔断/限流/并发互不连累、
  慢上游只占自己名额、失败后名额归还、逐上游健康状态分化；
- `tests/test_client.py`：单上游组合客户端端到端（原有路径 + 容量满/请求守卫/非法 JSON）；
- `tests/test_app.py`：HTTP 统一错误体、状态码、幂等头、版本头、命名上游路由、
  未登记 404、409/422/502(invalid_json)、逐上游健康端点。

本地启动并调用（多上游）：

```bash
export GATEWAY_UPSTREAMS='[
  {"name":"payment","base_url":"http://127.0.0.1:9000"},
  {"name":"risk","base_url":"http://127.0.0.1:9001"}]'
uv run python main.py

# 调用指定名字的上游
curl -X POST localhost:8000/upstreams/payment/call/pay \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' -H 'X-Api-Version: v2' \
  -d '{"id":"order-123","amount":100,"currency":"CNY"}'

# 查看每个上游各自的健康状态
curl localhost:8000/health
```
