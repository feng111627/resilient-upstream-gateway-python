"""pytest 全局配置：让每个用例的业务归属与通过情况在日志里一目了然。

运行 `uv run pytest tests/ -v` 时：
- 每个用例结束打印一行：[业务域] PASS/FAIL 用例名；
- 会话结束按业务域汇总通过/失败数。
"""

from __future__ import annotations

import pytest

# 测试文件 -> 业务域
AREA_BY_MODULE = {
    "test_errors": "错误归一化",
    "test_circuit_breaker": "熔断器",
    "test_client": "单上游客户端(重试/限流/幂等/版本)",
    "test_app": "HTTP入口(统一错误体/多上游路由)",
    "test_concurrency": "并发名额(舱壁)",
    "test_registry": "多上游隔离与健康",
    "test_semantics": "调用语义(幂等冲突/版本适配/非法响应)",
}

_outcomes: dict[str, dict[str, int]] = {}


def _area(item: pytest.Item) -> str:
    module = item.module.__name__.rsplit(".", 1)[-1]
    return AREA_BY_MODULE.get(module, module)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        return
    area = _area(item)
    passed = report.outcome == "passed"
    bucket = _outcomes.setdefault(area, {"passed": 0, "failed": 0})
    bucket["passed" if passed else "failed"] += 1
    mark = "PASS" if passed else "FAIL"
    print(f"\n[用例][{area}] {mark} {item.name}")


def pytest_terminal_summary(terminalreporter) -> None:
    terminalreporter.write_sep("=", "业务域覆盖汇总")
    for area in sorted(_outcomes):
        stat = _outcomes[area]
        total = stat["passed"] + stat["failed"]
        terminalreporter.write_line(
            f"[{area}] {stat['passed']}/{total} 通过"
            + (f"，{stat['failed']} 失败" if stat["failed"] else "")
        )
