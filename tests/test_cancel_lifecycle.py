from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import close_connection, get_connection, init_db


TEMPLATE = {
    "code": "safety-lab",
    "name": "安全操作实训模板",
    "algorithm": "safety-drill",
    "parameter_schema": {
        "rounds": {"type": "integer", "required": True, "minimum": 1, "maximum": 100},
    },
    "default_parameters": {},
    "max_runtime_seconds": 600,
    "max_attempts": 2,
}

T0 = datetime(2026, 9, 29, 8, 0, 0, tzinfo=UTC)
RESULT = {"passed": True, "score": 92}
METRICS = {"seconds": 41}


def submit_payload(key: str, *, user: str = "trainee-1") -> dict:
    return {
        "template_code": "safety-lab",
        "project_code": "class-a",
        "requested_by": user,
        "parameters": {"rounds": 3},
        "priority": 50,
        "idempotency_key": key,
    }


@pytest.fixture()
def service(tmp_path, monkeypatch):
    monkeypatch.setenv("TOWNSHIP_DATABASE_PATH", str(tmp_path / "cancel-lifecycle.db"))
    close_connection()
    init_db()
    clock = FrozenClock(T0)
    svc = ComputeOperationsService(get_connection(), clock)
    svc.create_template(TEMPLATE, "administrator")
    svc.set_quota(
        {"subject_type": "user", "subject_key": "trainee-1", "max_queued": 5, "max_running": 1, "daily_submissions": 50},
        "administrator",
    )
    yield svc
    close_connection()


def running_task(service: ComputeOperationsService, *, key: str = "seat-0001", worker: str = "worker-1") -> dict:
    service.submit(submit_payload(key))
    claimed = service.claim(worker, ["safety-drill"], 300)
    assert claimed is not None and claimed["status"] == "running"
    return claimed


def released_running_slot(resolution: dict) -> None:
    assert resolution["released_quota"] == {
        "quota_type": "running_slot",
        "subject_type": "user",
        "subject_key": "trainee-1",
    }


def test_worker_confirm_stops_reports_and_releases_seat(service):
    task = running_task(service)

    cancel = service.cancel(task["id"], "duty-teacher", "学员临时申请退出")
    assert cancel["status"] == "cancel_requested"
    pending = cancel["cancel_resolution"]
    assert pending["status"] == "pending"
    assert pending["requested_by"] == "duty-teacher"
    assert pending["confirmed_by"] == ""
    assert pending["effective_at"] is None

    service.clock.advance(seconds=20)
    # 确认前普通回报通道已关闭：成绩、心跳都被拒绝（成绩严格晚于取消到达）。
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", RESULT, METRICS)
    with pytest.raises(ConflictError):
        service.heartbeat(task["id"], "worker-1", 300)

    # 只有持有租约的工作者可以确认。
    with pytest.raises(ConflictError):
        service.confirm_cancel(task["id"], "other-worker")

    confirmed = service.confirm_cancel(task["id"], "worker-1")
    resolution = confirmed["cancel_resolution"]
    assert confirmed["status"] == "cancelled"
    assert resolution["status"] == "confirmed"
    assert resolution["resolution_path"] == "worker_confirmed"
    assert resolution["requested_by"] == "duty-teacher"
    assert resolution["confirmed_by"] == "worker-1"
    assert resolution["confirmed_at"] == confirmed["finished_at"] == resolution["effective_at"]
    released_running_slot(resolution)

    # 运行名额确实释放。
    states = service.repository.count_user_states("trainee-1")
    assert states.get("running", 0) == 0 and states.get("cancel_requested", 0) == 0

    # 重复确认回到首次处理结果，不产生新的历史行。
    repeated = service.confirm_cancel(task["id"], "worker-1")
    assert repeated["cancel_resolution"]["cancel_request_id"] == resolution["cancel_request_id"]
    assert repeated["cancel_resolution"]["effective_at"] == resolution["effective_at"]
    details = service.get_task(task["id"])
    assert [row["action"] for row in details["interventions"]] == ["cancel", "result_reject", "cancel_confirm"]

    # 取消生效后的迟到成绩不再受理，结论与首次一致。
    with pytest.raises(ConflictError):
        service.complete(task["id"], "worker-1", RESULT, METRICS)


def test_lost_worker_is_closed_by_recovery_after_timeout(service):
    task = running_task(service)
    cancel = service.cancel(task["id"], "duty-teacher", "学员临时申请退出", confirm_timeout_seconds=120)
    assert cancel["status"] == "cancel_requested"

    service.clock.advance(seconds=119)
    assert service.recover_expired()["cancel_timeouts"] == []
    assert service.get_task(task["id"])["status"] == "cancel_requested"

    service.clock.advance(seconds=2)
    result = service.recover_expired("recovery-worker")
    assert result["cancel_timeouts"] == [task["id"]]

    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    resolution = details["cancel_resolution"]
    assert resolution["status"] == "timeout_closed"
    assert resolution["resolution_path"] == "recovery_timeout"
    assert resolution["requested_by"] == "duty-teacher"
    assert resolution["confirmed_by"] == "recovery-worker"
    assert resolution["confirmed_at"] == resolution["effective_at"] == details["finished_at"]
    released_running_slot(resolution)

    # 重复恢复是幂等的：名额不会被二次处理，历史不增长。
    assert service.recover_expired("recovery-worker")["cancel_timeouts"] == []
    assert [row["action"] for row in service.get_task(task["id"])["interventions"]] == ["cancel", "cancel_timeout"]


def test_result_strictly_first_rejects_cancel_and_keeps_one_terminal_state(service):
    task = running_task(service)
    service.clock.advance(seconds=10)
    completed = service.complete(task["id"], "worker-1", RESULT, METRICS)
    assert completed["status"] == "succeeded"

    service.clock.advance(seconds=5)
    with pytest.raises(ConflictError) as exc:
        service.cancel(task["id"], "duty-teacher", "学员临时申请退出")
    resolution = exc.value.context["cancel_resolution"]
    assert resolution["status"] == "rejected"
    assert resolution["resolution_path"] == "result_first"
    assert resolution["requested_by"] == "duty-teacher"
    assert resolution["confirmed_by"] == "worker-1"

    details = service.get_task(task["id"])
    assert details["status"] == "succeeded"
    assert len(details["results"]) == 1
    assert details["cancel_resolution"]["status"] == "rejected"

    # 重复取消回到同一被否决结论，不新增结果版本。
    with pytest.raises(ConflictError) as again:
        service.cancel(task["id"], "duty-teacher", "学员临时申请退出")
    assert again.value.context["cancel_resolution"]["cancel_request_id"] == resolution["cancel_request_id"]
    assert len(service.get_task(task["id"])["results"]) == 1


def test_cancel_strictly_first_rejects_later_qualified_result(service):
    task = running_task(service)
    service.cancel(task["id"], "duty-teacher", "学员临时申请退出")

    service.clock.advance(seconds=5)
    with pytest.raises(ConflictError) as exc:
        service.complete(task["id"], "worker-1", RESULT, METRICS)
    assert exc.value.context["cancel_resolution"]["status"] == "pending"

    # 成绩未落库，任务仍停在待确认；随后确认收敛为取消。
    details = service.get_task(task["id"])
    assert details["status"] == "cancel_requested"
    assert details["results"] == []
    confirmed = service.confirm_cancel(task["id"], "worker-1")
    assert confirmed["status"] == "cancelled"
    assert confirmed["cancel_resolution"]["resolution_path"] == "worker_confirmed"


@pytest.mark.parametrize("order", ["cancel_first", "result_first"])
def test_simultaneous_arrival_converges_to_single_cancel_terminal_state(service, order):
    task = running_task(service, key=f"tie-{order}")

    if order == "cancel_first":
        pending = service.cancel(task["id"], "duty-teacher", "学员临时申请退出")
        assert pending["status"] == "cancel_requested"
        with pytest.raises(ConflictError) as exc:
            service.complete(task["id"], "worker-1", RESULT, METRICS)  # 同一固定时钟时刻到达
        resolution = exc.value.context["cancel_resolution"]
    else:
        service.complete(task["id"], "worker-1", RESULT, METRICS)  # 同一固定时钟时刻到达
        settled = service.cancel(task["id"], "duty-teacher", "学员临时申请退出")
        resolution = settled["cancel_resolution"]

    # 两种调用顺序必须收敛到同一个有依据的终态：取消，且不残留结果版本。
    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    assert details["results"] == []
    assert details["current_result_version"] is None
    assert resolution["status"] == "confirmed"
    assert resolution["resolution_path"] == "simultaneous_cancel_priority"
    assert resolution["requested_by"] == "duty-teacher"
    assert resolution["confirmed_by"] == "system"
    assert resolution["effective_at"] == details["finished_at"]
    released_running_slot(resolution)
    # 接口内联结论与历史记录给出相同结论。
    assert details["cancel_resolution"]["cancel_request_id"] == resolution["cancel_request_id"]
    assert details["cancel_requests"][-1]["resolution_path"] == "simultaneous_cancel_priority"


def test_api_response_and_history_give_the_same_reviewable_conclusion(client):
    response = client.post("/api/compute/templates?actor=administrator", json=TEMPLATE)
    assert response.status_code == 201, response.text
    submitted = client.post("/api/compute/tasks", json=submit_payload("api-seat-001")).json()
    claimed = client.post(
        "/api/compute/tasks/claim",
        json={"worker_id": "worker-9", "capabilities": ["safety-drill"], "lease_seconds": 300},
    ).json()["task"]
    assert claimed["id"] == submitted["id"]

    cancelled_request = client.post(
        f"/api/compute/tasks/{submitted['id']}/cancel",
        json={"actor": "duty-teacher", "reason": "学员临时申请退出", "confirm_timeout_seconds": 120},
    )
    assert cancelled_request.status_code == 200
    api_resolution = cancelled_request.json()["cancel_resolution"]
    assert api_resolution["requested_by"] == "duty-teacher"

    confirmed = client.post(f"/api/compute/tasks/{submitted['id']}/cancel/confirm", json={"worker_id": "worker-9"})
    assert confirmed.status_code == 200
    final_resolution = confirmed.json()["cancel_resolution"]
    assert confirmed.json()["status"] == "cancelled"

    history = client.get(f"/api/compute/task-details/{submitted['id']}").json()
    # 接口返回与 task-details 历史记录中的终态结论逐字段一致。
    assert history["status"] == "cancelled"
    assert history["cancel_resolution"] == final_resolution
    assert [row["action"] for row in history["interventions"]] == ["cancel", "cancel_confirm"]

    # 重复确认与重复取消都回到首次处理结果。
    again = client.post(f"/api/compute/tasks/{submitted['id']}/cancel/confirm", json={"worker_id": "worker-9"})
    assert again.status_code == 200 and again.json()["cancel_resolution"] == final_resolution
    duplicate_cancel = client.post(
        f"/api/compute/tasks/{submitted['id']}/cancel",
        json={"actor": "duty-teacher", "reason": "学员临时申请退出", "confirm_timeout_seconds": 120},
    )
    assert duplicate_cancel.status_code == 200
    assert duplicate_cancel.json()["cancel_resolution"]["cancel_request_id"] == final_resolution["cancel_request_id"]
    assert client.get(f"/api/compute/task-details/{submitted['id']}").json()["interventions"].__len__() == 2
