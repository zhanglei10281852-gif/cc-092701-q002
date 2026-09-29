from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理计算模板、配额、任务租约、结果版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["cancel_requests"] = [self._resolution_payload(item) for item in self.repository.list_cancel_requests(task_id)]
        if result["cancel_requests"]:
            result["cancel_resolution"] = result["cancel_requests"][-1]
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    @staticmethod
    def _blocking_cancel(repository: ComputeRepository, task: sqlite3.Row) -> sqlite3.Row | None:
        """返回当前关闭普通回报通道的取消请求（待确认或已据此结束）。"""
        if task["status"] not in {"cancel_requested", "cancelled"}:
            return None
        request_row = repository.latest_cancel_request(task["id"])
        if request_row is not None and request_row["status"] != "rejected":
            return request_row
        return None

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            blocking = self._blocking_cancel(repository, task)
            if blocking is not None:
                raise ConflictError(
                    "任务处于取消流程，普通回报通道已关闭",
                    context={"cancel_resolution": self._resolution_payload(blocking)},
                )
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        rejection: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            blocking = self._blocking_cancel(repository, task)
            if blocking is not None:
                if task["status"] == "cancel_requested" and blocking["requested_at"] == now:
                    # 取消请求与合格成绩同一固定时钟时刻到达：与“先成绩后取消”收敛到同一终态。
                    _, settled = self._settle_simultaneous_cancel(
                        connection, repository, task, request_row=blocking,
                        requested_by=blocking["requested_by"], reason=blocking["reason"], now=now,
                        trigger_actor=worker_id, trigger_action="result_tie_break",
                    )
                    rejection = ConflictError(
                        "取消与合格成绩同时到达，按取消优先规则终态为取消",
                        context={"cancel_resolution": self._resolution_payload(settled)},
                    )
                elif task["status"] == "cancel_requested":
                    # 取消请求严格先到：普通回报通道关闭，成绩留痕但不产生结果版本。
                    repository.add_intervention(
                        task_id=task_id, actor=worker_id, action="result_reject",
                        reason="合格成绩晚于取消请求到达，终态以取消为准",
                        before=dict(task), after=dict(task), batch_key="", now=now,
                    )
                    rejection = ConflictError(
                        "任务已在取消流程中，合格成绩被拒绝，终态以取消为准",
                        context={"cancel_resolution": self._resolution_payload(blocking)},
                    )
                else:
                    rejection = ConflictError(
                        "任务已取消，合格成绩不再受理",
                        context={"cancel_resolution": self._resolution_payload(blocking)},
                    )
            else:
                if task["status"] != "running" or task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
                )
                connection.execute(
                    "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (version, now, now, task_id),
                )
                task = repository.task_by_id(task_id)
        if rejection is not None:
            raise rejection
        return dict(task)

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        rejection: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            blocking = self._blocking_cancel(repository, task)
            if blocking is not None:
                if task["status"] == "cancel_requested":
                    repository.add_intervention(
                        task_id=task_id, actor=worker_id, action="fail_reject",
                        reason="失败回报晚于取消请求到达，终态以取消为准",
                        before=dict(task), after=dict(task), batch_key="", now=now,
                    )
                rejection = ConflictError(
                    "任务已在取消流程中，失败回报被拒绝，终态以取消为准",
                    context={"cancel_resolution": self._resolution_payload(blocking)},
                )
            else:
                if task["status"] != "running" or task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
                status = "queued" if can_retry else "failed"
                delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
                available = to_storage(now_value + timedelta(seconds=delay))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, available, error_code, message[:2000], None if can_retry else now, now, task_id),
                )
                task = repository.task_by_id(task_id)
        if rejection is not None:
            raise rejection
        return dict(task)

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "", confirm_timeout_seconds: int = 120) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        result: dict[str, Any] = {}
        rejection: ConflictError | None = None
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            existing = repository.latest_cancel_request(task_id)
            if existing is not None:
                # 重复操作回到首次处理结果；任务被重试后旧章节不再拦截新请求。
                if existing["status"] == "pending" and task["status"] == "cancel_requested":
                    result = self._with_resolution(dict(task), existing)
                elif existing["status"] in {"confirmed", "timeout_closed"} and task["status"] == "cancelled":
                    result = self._with_resolution(dict(task), existing)
                elif existing["status"] == "rejected" and task["status"] == "succeeded":
                    rejection = ConflictError(
                        "合格成绩已先于取消生效，终态以成绩为准",
                        context={"cancel_resolution": self._resolution_payload(existing)},
                    )
            if not result and rejection is None:
                if task["status"] == "succeeded":
                    finished_at = task["finished_at"] or ""
                    if finished_at < now:
                        # 成绩严格先于取消生效：登记被否决的取消请求，保留成功这一唯一终态。
                        rejected_row = self._record_cancel_rejected(connection, repository, task, actor, reason, now)
                        rejection = ConflictError(
                            "合格成绩已先于取消生效，终态以成绩为准",
                            context={"cancel_resolution": self._resolution_payload(rejected_row)},
                        )
                    else:
                        # 同一时刻到达：按“取消优先”立即仲裁为取消，成绩留痕但不再作为终态。
                        result = self._tie_break_cancel_priority(connection, repository, task, actor, reason, now)
                elif task["status"] in {"queued", "running"}:
                    result = self._open_cancel(connection, repository, task, actor, reason, batch_key, now_value, now, confirm_timeout_seconds)
                else:
                    raise ConflictError("当前任务状态不允许取消")
        if rejection is not None:
            raise rejection
        return result

    def _open_cancel(
        self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row,
        actor: str, reason: str, batch_key: str, now_value: datetime, now: str, confirm_timeout_seconds: int,
    ) -> dict[str, Any]:
        before = dict(task)
        if task["status"] == "queued":
            connection.execute(
                "UPDATE compute_tasks SET status='cancelled',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task["id"]),
            )
            request_row = repository.create_cancel_request(
                task_id=task["id"], requested_by=actor, reason=reason, requested_at=now, confirm_deadline=now, now=now,
            )
            repository.resolve_cancel_request(
                request_id=request_row["id"], status="confirmed", resolution_path="immediate",
                confirmed_by="", confirmed_at=None, effective_at=now,
                released_quota_type="queued_slot", released_subject_type="user",
                released_subject_key=task["requested_by"], now=now,
            )
        else:
            deadline = to_storage(now_value + timedelta(seconds=confirm_timeout_seconds))
            connection.execute(
                "UPDATE compute_tasks SET status='cancel_requested',updated_at=?,version=version+1 WHERE id=?",
                (now, task["id"]),
            )
            repository.create_cancel_request(
                task_id=task["id"], requested_by=actor, reason=reason, requested_at=now,
                confirm_deadline=deadline, now=now,
            )
        after = dict(repository.task_by_id(task["id"]))
        repository.add_intervention(task_id=task["id"], actor=actor, action="cancel", reason=reason, before=before, after=after, batch_key=batch_key, now=now)
        return self._with_resolution(after, repository.latest_cancel_request(task["id"]))

    def confirm_cancel(self, task_id: int, worker_id: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            request_row = repository.latest_cancel_request(task_id)
            if request_row is None:
                raise ConflictError("该任务没有取消请求可供确认")
            if request_row["status"] != "pending":
                if request_row["status"] == "rejected":
                    raise ConflictError(
                        "取消请求已被合格成绩否决",
                        context={"cancel_resolution": self._resolution_payload(request_row)},
                    )
                return self._with_resolution(repository.task_by_id(task_id), request_row)
            if task["status"] != "cancel_requested" or task["lease_owner"] != worker_id:
                raise ConflictError("取消请求只能由持有该任务的工作者确认")
            before = dict(task)
            connection.execute(
                "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, task_id),
            )
            repository.resolve_cancel_request(
                request_id=request_row["id"], status="confirmed", resolution_path="worker_confirmed",
                confirmed_by=worker_id, confirmed_at=now, effective_at=now,
                released_quota_type="running_slot", released_subject_type="user",
                released_subject_key=task["requested_by"], now=now,
            )
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(
                task_id=task_id, actor=worker_id, action="cancel_confirm", reason="工作者确认取消，停止后续普通回报",
                before=before, after=after, batch_key="", now=now,
            )
            return self._with_resolution(after, repository.latest_cancel_request(task_id))

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute("UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, task["id"]))
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancel_timeouts: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute("SELECT * FROM compute_tasks WHERE status='running' AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for task in rows:
                before = dict(task)
                if int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                connection.execute(
                    "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, task["id"]),
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
            # 取消确认超时：工作者失联或未在截止前确认，恢复程序代为结束并释放运行配额。
            for request_row in repository.cancel_requests_due(now):
                task = repository.task_by_id(request_row["task_id"])
                if task is None:
                    continue
                before = dict(task)
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task["id"]),
                )
                repository.resolve_cancel_request(
                    request_id=request_row["id"], status="timeout_closed", resolution_path="recovery_timeout",
                    confirmed_by=actor, confirmed_at=now, effective_at=now,
                    released_quota_type="running_slot", released_subject_type="user",
                    released_subject_key=task["requested_by"], now=now,
                )
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(
                    task_id=task["id"], actor=actor, action="cancel_timeout",
                    reason="工作者未在截止时间前确认取消，恢复程序代为结束",
                    before=before, after=after, batch_key="", now=now,
                )
                cancel_timeouts.append(int(task["id"]))
        return {"recovered": recovered, "exhausted": exhausted, "cancel_timeouts": cancel_timeouts}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def _settle_simultaneous_cancel(
        self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, *,
        request_row: sqlite3.Row | None, requested_by: str, reason: str, now: str,
        trigger_actor: str, trigger_action: str,
    ) -> tuple[dict[str, Any], sqlite3.Row]:
        # 成绩与取消在同一固定时钟时刻到达：既定规则“取消优先”，终态唯一收敛为取消。
        before = dict(task)
        # 同刻到达但未在仲裁中胜出的成绩不作为结果版本保留，确保只剩一个有依据的终态。
        connection.execute("DELETE FROM compute_results WHERE task_id=?", (task["id"],))
        connection.execute(
            "UPDATE compute_tasks SET status='cancelled',current_result_version=NULL,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
            (now, now, task["id"]),
        )
        if request_row is None:
            created = repository.create_cancel_request(
                task_id=task["id"], requested_by=requested_by, reason=reason, requested_at=now, confirm_deadline=now, now=now,
            )
            request_id = created["id"]
        else:
            # 待确认请求在同刻仲裁中立即生效：截止时间统一收敛为生效时刻，保证两种到达顺序逐字一致。
            connection.execute("UPDATE compute_cancel_requests SET confirm_deadline=? WHERE id=?", (now, request_row["id"]))
            request_id = request_row["id"]
        repository.resolve_cancel_request(
            request_id=request_id, status="confirmed", resolution_path="simultaneous_cancel_priority",
            confirmed_by="system", confirmed_at=now, effective_at=now,
            released_quota_type="running_slot", released_subject_type="user",
            released_subject_key=task["requested_by"], now=now,
        )
        after = dict(repository.task_by_id(task["id"]))
        repository.add_intervention(
            task_id=task["id"], actor=trigger_actor, action=trigger_action,
            reason="取消与合格成绩同时到达，按取消优先规则终态收敛为取消",
            before=before, after=after, batch_key="", now=now,
        )
        return after, repository.latest_cancel_request(task["id"])

    def _tie_break_cancel_priority(
        self, connection: sqlite3.Connection, repository: ComputeRepository,
        task: sqlite3.Row, actor: str, reason: str, now: str,
    ) -> dict[str, Any]:
        after, request_row = self._settle_simultaneous_cancel(
            connection, repository, task, request_row=None, requested_by=actor, reason=reason, now=now,
            trigger_actor=actor, trigger_action="cancel_tie_break",
        )
        return self._with_resolution(after, request_row)

    def _record_cancel_rejected(
        self, connection: sqlite3.Connection, repository: ComputeRepository,
        task: sqlite3.Row, actor: str, reason: str, now: str,
    ) -> sqlite3.Row:
        request_row = repository.create_cancel_request(
            task_id=task["id"], requested_by=actor, reason=reason, requested_at=now, confirm_deadline=now, now=now,
        )
        winner = connection.execute(
            "SELECT created_by FROM compute_results WHERE task_id=? ORDER BY version DESC LIMIT 1", (task["id"],),
        ).fetchone()
        confirmed_by = winner["created_by"] if winner is not None else "worker"
        repository.reject_cancel_request(request_id=request_row["id"], resolution_path="result_first", confirmed_by=confirmed_by, now=now)
        request_row = repository.latest_cancel_request(task["id"])
        repository.add_intervention(
            task_id=task["id"], actor=actor, action="cancel_reject",
            reason="合格成绩已先生效，取消请求被否决，终态以成绩为准",
            before=dict(task), after=dict(task), batch_key="", now=now,
        )
        return request_row

    @staticmethod
    def _resolution_payload(request_row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
        if request_row is None:
            return None
        item = dict(request_row)
        return {
            "cancel_request_id": item["id"],
            "requested_by": item["requested_by"],
            "requested_at": item["requested_at"],
            "confirm_deadline": item["confirm_deadline"],
            "status": item["status"],
            "resolution_path": item["resolution_path"],
            "confirmed_by": item["confirmed_by"],
            "confirmed_at": item["confirmed_at"],
            "effective_at": item["effective_at"],
            "released_quota": {
                "quota_type": item["released_quota_type"],
                "subject_type": item["released_subject_type"],
                "subject_key": item["released_subject_key"],
            },
        }

    def _with_resolution(self, task: dict[str, Any] | sqlite3.Row, request_row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any]:
        result = dict(task)
        result["cancel_resolution"] = self._resolution_payload(request_row)
        return result

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
