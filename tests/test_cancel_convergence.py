"""取消请求的确认、超时收敛与终态裁决测试。

以固定时钟模拟三种到达顺序：
1. 取消先到、工作者在时限内确认 -> cancelled，释放运行配额；
2. 取消先到、工作者失联 -> 恢复程序在确认超时/租约过期后代为结束；
3. 取消与合格成绩竞争到达（两种交错次序）-> 只保留一个有依据的终态。

另含排队取消、重复操作幂等与接口/历史一致性校验。
所有用例都通过 client fixture 把数据库指向临时路径。
"""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, init_db

TEMPLATE = {
    "code": "solver-a",
    "name": "方程求解模板",
    "algorithm": "solver-a",
    "parameter_schema": {
        "iterations": {"type": "integer", "required": True, "minimum": 1, "maximum": 10000},
        "tolerance": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "mode": {"type": "string", "required": True, "choices": ["fast", "accurate"]},
    },
    "default_parameters": {"tolerance": 0.001},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}

BASE_TIME = datetime(2026, 9, 29, 8, 0, 0, tzinfo=UTC)


def submit_payload(key: str, *, user: str = "student-1", priority: int = 50) -> dict:
    return {
        "template_code": "solver-a",
        "project_code": "project-a",
        "requested_by": user,
        "parameters": {"iterations": 100, "mode": "accurate"},
        "priority": priority,
        "idempotency_key": key,
    }


def _service() -> tuple[ComputeOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(BASE_TIME)
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "administrator")
    return service, clock


def _running_task(service: ComputeOperationsService, key: str, user: str = "student-1", lease: int = 60):
    service.set_quota({"subject_type": "user", "subject_key": user, "max_queued": 5, "max_running": 1, "daily_submissions": 50}, "administrator")
    task = service.submit(submit_payload(key, user=user))
    claimed = service.claim("worker-1", ["solver-a"], lease)
    assert claimed and claimed["id"] == task["id"]
    return task


CONCLUSION_KEYS = (
    "status", "requested_by", "acknowledged_by", "effective_at",
    "released_quota", "request_status", "task_effective_at", "finished_at",
)


def _history_conclusion(service: ComputeOperationsService, task_id: int) -> dict:
    """从任务详情与取消请求历史中复原终态结论。"""
    details = service.get_task(task_id)
    assert details["cancel_requests"], "历史中必须留有取消请求记录"
    request = details["cancel_requests"][-1]
    return {
        "status": details["status"],
        "requested_by": request["requested_by"],
        "acknowledged_by": request["acknowledged_by"],
        "effective_at": request["effective_at"],
        "released_quota": request["released_quota"],
        "request_status": request["status"],
        "task_effective_at": details["cancel_effective_at"],
        "finished_at": details["finished_at"],
    }


def _response_conclusion(outcome: dict) -> dict:
    cancellation = outcome["cancellation"]
    return {
        "status": outcome["status"],
        "requested_by": cancellation["requested_by"],
        "acknowledged_by": cancellation["acknowledged_by"],
        "effective_at": cancellation["effective_at"],
        "released_quota": cancellation["released_quota"],
        "request_status": cancellation["request_status"],
        "task_effective_at": outcome["cancel_effective_at"],
        "finished_at": outcome["finished_at"],
    }


def test_order_one_worker_acknowledges_cancel(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-order-1")

    requested = service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-key-1", ack_timeout_seconds=30)
    assert requested["status"] == "cancel_requested"
    assert requested["cancellation"]["requested_by"] == "duty-teacher"
    assert requested["cancellation"]["deadline_at"] == "2026-09-29T08:00:30+00:00"
    assert requested["cancellation"]["request_status"] == "pending"
    assert requested["released_quota"] == ""  # 运行配额在确认前不释放
    # 取消挂起期间，普通回报（心跳）必须停止
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-1", 60)

    clock.advance(seconds=5)
    outcome = service.acknowledge_cancel(task["id"], "worker-1")
    conclusion = _response_conclusion(outcome)
    assert conclusion == {
        "status": "cancelled",
        "requested_by": "duty-teacher",
        "acknowledged_by": "worker-1",
        "effective_at": "2026-09-29T08:00:05+00:00",
        "released_quota": "running",
        "request_status": "accepted",
        "task_effective_at": "2026-09-29T08:00:05+00:00",
        "finished_at": "2026-09-29T08:00:05+00:00",
    }
    # 接口即时结论与历史记录复原结论必须一致
    assert _history_conclusion(service, task["id"]) == conclusion

    details = service.get_task(task["id"])
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_acknowledged"]
    # 运行配额已释放：同一用户可以再领取新任务
    service.submit(submit_payload("cancel-order-1-next", user="student-1"))
    assert service.claim("worker-1", ["solver-a"], 60) is not None


def test_order_two_worker_lost_recovery_converges_at_deadline(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-order-2", lease=600)
    service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-key-2", ack_timeout_seconds=30)

    # 工作者始终不应答；确认超时前恢复程序不介入
    clock.advance(seconds=29)
    assert service.recover_expired()["cancelled"] == []
    assert service.get_task(task["id"])["status"] == "cancel_requested"

    clock.advance(seconds=1)
    recovered = service.recover_expired()
    assert recovered["cancelled"] == [task["id"]]
    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    request = details["cancel_requests"][-1]
    assert request["acknowledged_by"] == "recovery-worker"
    assert request["effective_at"] == "2026-09-29T08:00:30+00:00"
    assert request["released_quota"] == "running"
    assert details["interventions"][-1]["action"] == "cancel_timeout_recovery"
    assert _history_conclusion(service, task["id"])["acknowledged_by"] == "recovery-worker"

    # 迟到的工作者确认不能改写终态
    with pytest.raises(ConflictError):
        service.acknowledge_cancel(task["id"], "worker-1")
    # 恢复程序重复运行不产生新结果
    clock.advance(seconds=10)
    assert service.recover_expired()["cancelled"] == []
    assert len(service.get_task(task["id"])["interventions"]) == 2


def test_order_two_lease_expires_before_ack_deadline(client: TestClient):
    service, clock = _service()
    # 租约 10 秒、确认时限 60 秒：先失联（租约过期）即由恢复程序代为结束
    task = _running_task(service, "cancel-order-2b", lease=10)
    service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-key-2b", ack_timeout_seconds=60)
    clock.advance(seconds=11)
    assert service.recover_expired()["cancelled"] == [task["id"]]
    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    assert details["cancel_requests"][-1]["acknowledged_by"] == "recovery-worker"
    assert "失联" in details["interventions"][-1]["reason"]


def test_order_three_cancel_before_qualified_result(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-order-3a")
    service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-key-3a")
    clock.advance(seconds=1)
    # 合格成绩晚于取消到达：不得覆盖取消依据，回报被拒收但留有记录
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", {"score": 95}, {}, True)
    details = service.get_task(task["id"])
    assert details["status"] == "cancel_requested"  # 终态尚待确认，取消依据保留
    assert details["results"] == []  # 迟到成绩不落库为正式结果版本
    rejection = details["interventions"][-1]
    assert rejection["action"] == "result_rejected"
    assert json.loads(rejection["after_json"])["late_report"]["outcome"] == "qualified_result"

    outcome = service.acknowledge_cancel(task["id"], "worker-1")
    assert outcome["status"] == "cancelled"
    assert _history_conclusion(service, task["id"]) == _response_conclusion(outcome)


def test_order_three_qualified_result_before_cancel(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-order-3b")
    clock.advance(seconds=1)
    completed = service.complete(task["id"], "worker-1", {"score": 96}, {}, True)
    assert completed["status"] == "succeeded"
    assert completed["current_result_version"] == 1
    # 取消晚于合格成绩到达：成绩终态保留，取消被拒绝
    with pytest.raises(ConflictError):
        service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-key-3b")
    details = service.get_task(task["id"])
    assert details["status"] == "succeeded"
    assert len(details["results"]) == 1
    assert details["cancel_requests"] == []


def test_unqualified_result_fails_without_result_version(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-order-3c")
    clock.advance(seconds=1)
    outcome = service.complete(task["id"], "worker-1", {"score": 40}, {}, False)
    assert outcome["status"] == "failed"
    assert service.get_task(task["id"])["results"] == []


def test_queued_cancel_releases_queued_quota_immediately(client: TestClient):
    service, clock = _service()
    service.set_quota({"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 10}, "administrator")
    task = service.submit(submit_payload("queued-cancel-1", user="limited"))
    outcome = service.cancel(task["id"], "duty-teacher", "学员撤回申请", request_key="queued-cancel-key")
    assert outcome["status"] == "cancelled"
    assert outcome["cancellation"]["released_quota"] == "queued"
    assert outcome["cancel_effective_at"] == outcome["finished_at"] == "2026-09-29T08:00:00+00:00"
    assert _history_conclusion(service, task["id"]) == _response_conclusion(outcome)
    # 排队名额恢复：再次提交不被配额拦截
    again = service.submit(submit_payload("queued-cancel-2", user="limited"))
    assert again["status"] == "queued"


def test_repeated_operations_return_first_outcome(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-repeat")

    first = service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="repeat-key", ack_timeout_seconds=30)
    # 重复发起（同一幂等键）回到首次处理结果，而不是报"已存在"
    repeated = service.cancel(task["id"], "duty-teacher", "学员临时申请退出（重复点击）", request_key="repeat-key")
    assert repeated["status"] == first["status"] == "cancel_requested"
    assert repeated["cancellation"]["request_id"] == first["cancellation"]["request_id"]

    clock.advance(seconds=5)
    ack = service.acknowledge_cancel(task["id"], "worker-1")
    # 工作者重复确认：回到首次确认结果
    clock.advance(seconds=5)
    ack_again = service.acknowledge_cancel(task["id"], "worker-1")
    assert ack_again["status"] == ack["status"] == "cancelled"
    assert ack_again["cancellation"]["acknowledged_at"] == ack["cancellation"]["acknowledged_at"] == "2026-09-29T08:00:05+00:00"
    assert ack_again["cancellation"]["effective_at"] == ack["cancellation"]["effective_at"]

    # 取消幂等键在确认后重放，仍返回首次终态（不随时钟推移改变生效时刻）
    replay = service.cancel(task["id"], "duty-teacher", "重放", request_key="repeat-key")
    assert replay["status"] == "cancelled"
    assert replay["cancellation"]["effective_at"] == "2026-09-29T08:00:05+00:00"
    assert replay["cancellation"]["released_quota"] == "running"

    details = service.get_task(task["id"])
    # 只有一次 cancel 干预、一次确认干预
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_acknowledged"]


def test_cancel_then_retry_allows_new_run(client: TestClient):
    service, clock = _service()
    task = _running_task(service, "cancel-retry")
    service.cancel(task["id"], "duty-teacher", "学员临时申请退出", request_key="cancel-retry-key")
    clock.advance(seconds=2)
    service.acknowledge_cancel(task["id"], "worker-1")
    retried = service.retry(task["id"], "duty-teacher", "误操作恢复", priority=80)
    assert retried["status"] == "queued"
    assert retried["cancel_requested_at"] == ""
    claimed = service.claim("worker-2", ["solver-a"], 60)
    assert claimed and claimed["id"] == task["id"]


def test_http_api_and_history_give_same_conclusion(client: TestClient):
    """走 HTTP 接口完成完整流程，校验接口与历史记录结论一致。"""
    client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    created = client.post("/api/compute/tasks", json=submit_payload("http-cancel-1"))
    task_id = created.json()["id"]
    client.post("/api/compute/tasks/claim", json={"worker_id": "worker-1", "capabilities": ["solver-a"], "lease_seconds": 60})

    cancel_response = client.post(
        f"/api/compute/tasks/{task_id}/cancel",
        json={"actor": "duty-teacher", "reason": "学员临时申请退出", "idempotency_key": "http-cancel-key", "ack_timeout_seconds": 30},
    )
    assert cancel_response.status_code == 200, cancel_response.text
    assert cancel_response.json()["status"] == "cancel_requested"

    # 取消后普通回报被拒
    blocked_heartbeat = client.post(f"/api/compute/tasks/{task_id}/heartbeat", json={"worker_id": "worker-1", "capabilities": [], "lease_seconds": 60})
    assert blocked_heartbeat.status_code == 409

    ack_response = client.post(f"/api/compute/tasks/{task_id}/cancel/acknowledge", json={"worker_id": "worker-1"})
    assert ack_response.status_code == 200, ack_response.text
    ack_body = ack_response.json()
    assert ack_body["status"] == "cancelled"

    details = client.get(f"/api/compute/task-details/{task_id}").json()
    request = details["cancel_requests"][-1]
    assert details["status"] == ack_body["status"] == "cancelled"
    assert request["requested_by"] == ack_body["cancellation"]["requested_by"] == "duty-teacher"
    assert request["acknowledged_by"] == ack_body["cancellation"]["acknowledged_by"] == "worker-1"
    assert request["effective_at"] == ack_body["cancellation"]["effective_at"] == details["cancel_effective_at"]
    assert request["released_quota"] == "running"
