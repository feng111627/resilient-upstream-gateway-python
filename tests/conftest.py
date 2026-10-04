"""pytest 公共配置：每个测试文件（业务模块）开始时打印一行业务说明。

横幅在 pytest_runtest_logstart 阶段通过终端报告器输出（与逐用例进度行同一通道，
不受 stdout capture 影响）。配合 `pytest -v` 可清楚看到覆盖了哪些业务以及
每个用例的通过情况。
"""

from __future__ import annotations

# 测试文件 -> 业务说明，按业务域分组展示
_BUSINESS_TITLES = {
    "test_errors.py": "业务：错误归一化（底层异常分类，不透出原始异常）",
    "test_circuit_breaker.py": "业务：熔断器三态与并发安全",
    "test_bulkhead.py": "业务：每上游并发隔离舱（名额、排队、快速失败、必定释放）",
    "test_client.py": "业务：单上游组合客户端端到端（重试/熔断/限流/幂等/版本/非法JSON）",
    "test_multi_upstream.py": "业务：多上游登记、故障隔离、配额隔离、分别健康检查",
    "test_idempotency_conflict.py": "业务：幂等同键换内容冲突",
    "test_request_versioning.py": "业务：分版本请求组装与校验",
    "test_app.py": "业务：HTTP 层统一错误体/头/多上游路由/健康端点",
}

_shown: set[str] = set()


def pytest_report_header(config):
    # 此 hook 一定能拿到 config；缓存终端报告器供 logstart 阶段输出横幅
    pytest_report_header.reporter = config.pluginmanager.get_plugin("terminalreporter")


def pytest_runtest_logstart(nodeid, location):
    path = location[0].replace("\\", "/").split("/")[-1]
    title = _BUSINESS_TITLES.get(path)
    reporter = getattr(pytest_report_header, "reporter", None)
    if title is None or path in _shown or reporter is None:
        return
    _shown.add(path)
    reporter.write_line("")
    reporter.write_line(f"===== {title} =====")
