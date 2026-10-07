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
  idempotency.py      幂等键：重复提交复用结论；同键换请求（上游/接口/版本/报文）报冲突
  versioning.py       版本协商 + 请求按版本组装校验 + 响应 schema 校验
  client.py           ResilientUpstreamClient：以上能力的组合（单上游）
  registry.py         多上游注册表：按名字路由，配置与状态逐上游隔离
main.py               FastAPI 入口：POST /gateway/{upstream}/{path}（按名字）
                      及兼容老路径 POST /gateway/{path}（打到 default 上游）
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

### 与单上游老用法的兼容

多上游是**在旁边加**的能力，单上游时代的用法升级后照常可用：

- **老路径**：`POST /gateway/{path}`（不带上游名）仍然有效，固定打到名为
  `default` 的上游；未登记 `default` 时返回明确的 `unknown_upstream`（404）。
  多段路径 `/gateway/a/b` 优先按「名字路由」解析（`a` 为上游名）。
- **老构造方式**：`create_app(client)` 仍接受直接构造的
  `ResilientUpstreamClient`，客户端会被登记为 `default` 上游，
  其熔断/限流/并发组件原样保留，健康端点照常可读：

  ```python
  from main import create_app
  from gateway.client import ResilientUpstreamClient

  app = create_app(ResilientUpstreamClient(base_url="http://127.0.0.1:9000"))
  ```

- 代码侧也可以把现成客户端登记为具名上游：
  `registry.register_client("pay", client)`。

## 隔离与配额规则

- **逐上游独立组件**：每个上游独享 httpx 客户端（独立地址/超时）、重试策略、
  熔断器、令牌桶、并发名额。某个上游挂了或一直超时，只会熔断、
  限流、占满**它自己**的额度，别的上游照常服务。
  （幂等存储是注册表级共享的，见「幂等」一节——幂等是调用方层面的语义，
  与上游运行状态隔离互不影响。）
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

**非法响应优先于状态码归类**：只要响应体不是合法 JSON（比如上游甩回一坨
HTML 错误页），无论状态码是 200 还是 502，都归 `invalid_response` 且不重试——
不会被当成上游 5xx 反复重试。5xx 且报文是合法 JSON 时仍按 `upstream_5xx`
正常重试。

## 重试与熔断规则

**重试**（`RetryPolicy`）：只对可重试错误（超时、连接失败、上游 5xx）重试，
4xx、请求/响应校验问题、非法 JSON 响应（`invalid_response`）都不重试。
退避公式
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
- **判定范围是整个请求**：打到哪个上游、哪个接口（路径）、走哪个版本、
  报文内容，全部计入请求指纹。同一幂等键只要其中任何一项变了，
  就是两次不同的调用，明确返回 `idempotency_conflict`（HTTP 409），
  绝不把上一次的结论原样复用回去，也不会在新目标上重复执行；
- 同键、上游/接口/版本/报文完全一致的重复提交照常复用旧结论；
- 并发同时打进来不会乱：同请求的都拿到同一结论，换请求的全报冲突，
  只有 owner 真正执行一次；冲突不会污染原键——报过冲突之后，
  用原请求重复提交仍能拿到原结论；
- 因此重试或重复提交不会造成重复副作用。

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
  响应不是合法 JSON 或顶层不是对象返回 `invalid_response`，
  与超时/4xx/5xx/`schema_mismatch` 都是可区分的类别；
- 归一化输出显式携带 `version` 字段，v1 没有的字段不会出现在输出里。

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
  成功/失败/中途放弃后名额正确归还、30 并发打 2 个名额不堆积；
- `tests/test_registry.py`：多上游隔离——一个上游熔断/限流/占满名额/卡慢
  不影响其他上游，健康状态逐上游分别上报；同一幂等键换上游报冲突、
  同键同请求同上游照常复用、不同键互不影响；
- `tests/test_semantics.py`：同幂等键换接口/换版本/换报文报冲突且不污染原键、
  并发下同键混合请求结论一致、请求按 v1/v2 组装校验、
  非法 JSON（含 5xx+HTML 错误页）归 `invalid_response` 且不重试、
  缺字段仍归 `schema_mismatch`、解析异常不外泄；
- `tests/test_app.py`：HTTP 层多上游路由、统一错误体、状态码、幂等头、
  版本头、分上游健康端点，以及单上游老用法兼容（老路径
  `/gateway/{path}` 打到 default 上游、`create_app(client)` 老构造方式）。

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

本地启动网关（单上游老用法，老路径不打上游名）：

```bash
export UPSTREAM_BASE_URL='http://127.0.0.1:9000'   # 不设置 UPSTREAMS，自动登记为 default
uv run python main.py
curl -X POST localhost:8000/gateway/pay \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' \
  -d '{"amount": 100, "currency": "CNY"}'
```
