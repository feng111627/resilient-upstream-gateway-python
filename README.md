# Resilient Upstream Gateway

一个对接不稳定上游接口的 FastAPI 网关。核心目标：**任何上游故障都以我们自己的、
可区分的错误结论返回给调用方，底层库（httpx/asyncio）的异常绝不外泄**。

## 结构

```
app/
  errors.py           统一错误分类（ErrorKind + HTTP 状态码）
  upstream.py         httpx 封装：把所有底层异常归一为 GatewayError
  retry.py            指数退避 + 抖动的重试策略（sleep 可注入）
  circuit_breaker.py  熔断器：CLOSED / OPEN / HALF_OPEN，asyncio.Lock 保证并发安全
  rate_limiter.py     令牌桶限流（时钟可注入）
  idempotency.py      幂等键存储：single-flight + 结果回放 + 冲突检测
  versions.py         上游 v1/v2 接口适配，缺字段显式报错
  service.py          编排：限流 → 幂等 → 熔断 → 重试 → 版本适配
  main.py             FastAPI 入口，GatewayError → 统一 JSON 错误响应
tests/                全部使用进程内模拟上游（httpx.MockTransport），无外部依赖
```

## 错误映射

调用方收到的永远是如下结构，HTTP 状态码与 `error.kind` 一一对应：

```json
{"error": {"kind": "upstream_timeout", "message": "...", "detail": {...}}}
```

| 上游/网关情况                     | kind                     | HTTP |
|----------------------------------|--------------------------|------|
| 调用上游超时                      | `upstream_timeout`       | 504  |
| 连接失败（拒绝/重置/DNS）          | `upstream_connection_error` | 502 |
| 上游返回 4xx                     | `upstream_client_error`  | 502  |
| 上游返回 5xx                     | `upstream_server_error`  | 502  |
| 上游返回非 JSON/非对象响应        | `upstream_bad_response`  | 502  |
| 重试全部失败（包装最后一次错误）   | `retries_exhausted`      | 502  |
| 熔断器打开，快速失败              | `circuit_open`           | 503  |
| 网关限流                         | `rate_limited`           | 429  |
| 幂等键被不同载荷复用              | `idempotency_conflict`   | 409  |
| 请求了不支持的接口版本            | `unsupported_version`    | 400  |
| 上游响应缺少必需字段              | `missing_field`          | 502  |

`detail` 中带有上下文（如上游状态码、缺失字段名、建议的 `retry_after`）。

## 重试与熔断规则

**重试**（`RetryPolicy`，默认 `max_attempts=3, base_delay=0.1s, max_delay=2s`）：

- 只重试*临时性*失败：超时、连接失败、上游 5xx。
- 上游 4xx、熔断打开、限流、版本/字段问题**不重试**，立即返回。
- 退避为 full-jitter 指数退避：`uniform(0, min(max_delay, base * 2^(n-1)))`。
- 全部失败后抛出 `retries_exhausted`，`detail.last_error` 标明最后一次失败的类别。

**熔断**（`CircuitBreaker`，默认 `failure_threshold=5, recovery_timeout=30s,
half_open_max_probes=1`）：

- CLOSED：正常放行；连续失败达到阈值 → OPEN。
- OPEN：直接快速失败（`circuit_open`），不触碰上游；`recovery_timeout` 后下一次
  请求进入 HALF_OPEN。
- HALF_OPEN：放行最多 `half_open_max_probes` 个并发探针，其余拒绝；探针成功 →
  CLOSED，探针失败 → 重新 OPEN 并重启恢复计时。
- 所有状态迁移由 `asyncio.Lock` 串行化，并发下不会出现撕裂状态；恢复时机由
  可注入时钟控制（测试中用 `FakeClock` 精确推进）。
- 一次逻辑调用（含其内部重试）只计一次成功/失败。

**限流**（`TokenBucketRateLimiter`，默认 50/s、burst 100）：非阻塞获取令牌，
拿不到立即返回 `rate_limited`，不排队、不堆积。

## 幂等

- 每个写请求必须携带 `Idempotency-Key` 请求头。
- 相同键 + 相同载荷：并发重复提交共享同一次执行（single-flight），之后的重复
  请求直接回放首次结果——上游副作用**恰好一次**。
- 相同键 + 不同载荷：`409 idempotency_conflict`，绝不静默返回无关结果。
- 执行失败不缓存，同键可安全重试。

## 版本兼容

支持上游 `v1`、`v2` 两个版本（`UpstreamAdapter.supported_versions`）：

| 版本 | 请求体                                            | 响应体                                       |
|------|---------------------------------------------------|----------------------------------------------|
| v1   | `{"order_id", "amount"}`                          | `{"status", "transaction_id"}`               |
| v2   | `{"order": {"id", "amount"}, "currency": "CNY"}`  | `{"result": {"status", "txId"}}`             |

两个版本都归一为 `{"status", "transaction_id"}` 返回给调用方。请求未知版本
→ `400 unsupported_version`；上游响应缺少必需字段 → `502 missing_field`
（`detail.field` 指明缺失字段），**绝不静默取默认值**。

## 运行与验证

```bash
uv sync                                        # 安装依赖
uv run pytest                                  # 跑全部单测（67 个用例，-v 可看到每个用例）
UPSTREAM_BASE_URL=http://127.0.0.1:9000 uv run uvicorn main:app
```

调用示例：

```bash
curl -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"order_id": "o-42", "amount": 9.5, "version": "v1"}'
```

测试全部通过进程内 `httpx.MockTransport` 模拟上游（超时、连接失败、4xx/5xx、
非法 JSON、缺字段、v1/v2 两种响应），不依赖任何外部服务；并发边界
（熔断并发计数、半开探针竞争、重复提交 single-flight、令牌桶并发）均有用例覆盖。
