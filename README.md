# resilient-upstream-gateway

对接不稳定上游接口的弹性网关：把超时、连接失败、上游 4xx/5xx 等失败归一成
可区分的错误结论，配合重试、熔断、限流、幂等键与多版本兼容，
绝不把底层库（httpx 等）的异常原样透给调用方。

## 结构

```
gateway/
  errors.py           错误归一化：底层异常 -> 可区分的 GatewayError
  circuit_breaker.py  熔断器：关闭/打开/半开三态，线程安全
  rate_limiter.py     令牌桶限流
  retry.py            指数退避 + 抖动的重试策略
  idempotency.py      幂等键：重复提交复用首个请求的结论
  versioning.py       版本协商与响应 schema 校验
  client.py           ResilientUpstreamClient：以上能力的组合
main.py               FastAPI 入口：POST /gateway/{path}
tests/                单测 + 本地模拟上游（不依赖任何外部服务）
```

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
| 不支持的接口版本         | `version_unsupported`     | 400         | 否     |
| 响应字段缺失/结构不符    | `schema_mismatch`         | 502         | 否     |
| 未分类异常（兜底）       | `unknown`                 | 500         | 否     |

底层异常类型名只放在内部 `detail` 字段，不出现在对外的 `error`/`message` 中。

## 重试与熔断规则

**重试**（`RetryPolicy`）：只对可重试错误（超时、连接失败、上游 5xx）重试，
4xx 与 schema 问题不重试。退避公式
`min(max_delay, base_delay * 2^(n-1)) * (1 ± jitter)`，默认最多 3 次尝试。

**熔断**（`CircuitBreaker`）三态切换：

- **关闭**：正常放行；连续失败达到 `failure_threshold`（默认 5）切到打开。
- **打开**：快速失败，请求不打到上游；经过 `recovery_timeout`（默认 30s）后
  下一个请求触发半开。
- **半开**：同一时刻只放行一个探针；探针连续成功 `success_threshold`
  （默认 2）次回到关闭；任何一次失败立即回到打开并重新计时。

所有状态迁移在同一把锁内完成，并发下状态不会被写乱；
`time_func` 可注入假时钟，恢复重试时机完全可控（见单测）。
限流（令牌桶）在熔断之前执行，被限流的请求不会打到上游。

## 幂等

调用方在 `Idempotency-Key` 头携带幂等键：

- 首个请求真正执行；执行期间的并发重复提交阻塞等待并拿到相同结论；
- 完成后的结论（成功或失败）在 TTL（默认 300s）内缓存，重复提交直接复用；
- 因此重试或重复提交不会造成重复副作用。

## 版本兼容

- 支持上游接口版本 `v1`、`v2`；`X-Api-Version` 头缺省用 `v2`；
- 不支持的版本直接返回 `version_unsupported`，不发请求；
- 每个版本声明必填字段（v2 比 v1 多 `currency`），响应缺字段返回
  `schema_mismatch` 并列出缺失字段，绝不默默取默认值；
- 归一化输出显式携带 `version` 字段，v1 没有的字段不会出现在输出里。

## 验证方法

全部验证基于本地模拟上游（`tests/mock_upstream.py`，httpx MockTransport），
不依赖任何外部服务：

```bash
uv run pytest tests/ -v
```

覆盖范围（`pytest -v` 可看到每个用例的通过情况）：

- `tests/test_errors.py`：底层异常到各类错误的映射；
- `tests/test_circuit_breaker.py`：三态切换、半开探针互斥、并发下状态一致性；
- `tests/test_client.py`：正常调用、超时/连接失败/4xx/5xx 归类、重试恢复、
  熔断快速失败与恢复、限流、幂等去重（含 20 并发同键只产生 1 次副作用）、
  版本协商与字段缺失；
- `tests/test_app.py`：HTTP 层统一错误体、状态码、幂等头、版本头。

本地启动网关（上游地址用环境变量指定）：

```bash
UPSTREAM_BASE_URL=http://127.0.0.1:9000 uv run python main.py
curl -X POST localhost:8000/gateway/pay \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-123' -H 'X-Api-Version: v2' \
  -d '{"amount": 100}'
```
