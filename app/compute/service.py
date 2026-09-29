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


CANCEL_ACK_TIMEOUT_SECONDS = 30


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
        result["cancel_requests"] = [
            dict(row) for row in self.connection.execute(
                "SELECT * FROM compute_cancel_requests WHERE task_id=? ORDER BY id", (task_id,)
            ).fetchall()
        ]
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

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] == "cancel_requested":
                raise ConflictError("任务已收到取消请求，工作者应确认取消而不是继续回报")
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(ComputeRepository(connection).task_by_id(task_id))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], passed: bool) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] == "cancel_requested":
                # 取消请求已先行持久化：终态只能保留一个，迟到回报不得覆盖取消依据。
                self._reject_late_report(connection, repository, task, worker_id, now, qualified=passed)
                connection.commit()  # 先落库拒收依据，再以冲突结束本次回报
                raise ConflictError("任务已收到取消请求，合格成绩晚于取消到达，终态保持为已取消")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            if not passed:
                connection.execute(
                    "UPDATE compute_tasks SET status='failed',lease_owner='',lease_expires_at='',last_error_code='not_qualified',last_error_message='实训成绩不合格',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, task_id),
                )
                return dict(repository.task_by_id(task_id))
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"result": result, "metrics": metrics}), worker_id, now),
            )
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            return dict(repository.task_by_id(task_id))

    @staticmethod
    def _reject_late_report(connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, worker_id: str, now: str, *, qualified: bool) -> None:
        outcome = "qualified_result" if qualified else "unqualified_result"
        after = dict(task)
        after["late_report"] = {"worker_id": worker_id, "outcome": outcome, "rejected_at": now}
        repository.add_intervention(
            task_id=task["id"], actor=worker_id, action="result_rejected",
            reason=f"取消请求({task['cancel_requested_at']})先于成绩回报到达，{outcome} 不改变终态",
            before=dict(task), after=after, batch_key="", now=now,
        )

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] == "cancel_requested":
                raise ConflictError("任务已收到取消请求，失败回报不再受理，终态以取消确认为准")
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
            return dict(repository.task_by_id(task_id))

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "", request_key: str = "", ack_timeout_seconds: int = CANCEL_ACK_TIMEOUT_SECONDS) -> dict[str, Any]:
        """发起取消。排队任务立即释放排队配额；运行中任务进入 cancel_requested，
        等待持有工作者确认或由恢复程序在确认超时后代为结束。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if request_key:
                existing = repository.cancel_request_by_idempotency(request_key)
                if existing is not None:
                    return self._cancel_outcome(repository, int(existing["task_id"]))
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            if task["status"] == "cancel_requested":
                raise ConflictError("取消请求已存在，等待工作者确认或超时收敛")
            if task["status"] in {"cancelled", "succeeded", "failed"}:
                raise ConflictError("任务已处于终态，不能重复取消")
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("当前任务状态不允许取消")
            if task["status"] == "queued":
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',cancel_requested_by=?,cancel_requested_at=?,cancel_ack_deadline='',cancel_acknowledged_by='',cancel_acknowledged_at='',cancel_effective_at=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (actor, now, now, now, now, task_id),
                )
                request = repository.create_cancel_request(task_id=task_id, idempotency_key=request_key, requested_by=actor, requested_at=now, deadline_at=None, now=now)
                repository.settle_cancel_request(
                    int(request["id"]), status="accepted", acknowledged_by="", acknowledged_at=now,
                    effective_at=now, released_quota="queued", note="排队任务直接取消，立即释放排队配额", now=now,
                )
                released = "queued"
            else:
                deadline_text = to_storage(now_value + timedelta(seconds=ack_timeout_seconds))
                connection.execute(
                    "UPDATE compute_tasks SET status='cancel_requested',cancel_requested_by=?,cancel_requested_at=?,cancel_ack_deadline=?,updated_at=?,version=version+1 WHERE id=?",
                    (actor, now, deadline_text, now, task_id),
                )
                repository.create_cancel_request(task_id=task_id, idempotency_key=request_key, requested_by=actor, requested_at=now, deadline_at=deadline_text, now=now)
                released = ""
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action="cancel", reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            outcome = self._cancel_outcome(repository, task_id)
            outcome["released_quota"] = released
            return outcome

    def acknowledge_cancel(self, task_id: int, worker_id: str, request_key: str = "") -> dict[str, Any]:
        """工作者确认取消：停止后续普通回报，立即释放运行配额并收敛为 cancelled。"""
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if request_key:
                existing = repository.cancel_request_by_idempotency(request_key)
                if existing is not None:
                    return self._cancel_outcome(repository, int(existing["task_id"]))
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            request = repository.cancel_request(task_id)
            if task["status"] == "cancelled" and request is not None and request["status"] == "accepted":
                if request["acknowledged_by"] == worker_id:
                    return self._cancel_outcome(repository, task_id)
                confirmer = request["acknowledged_by"] or "系统（排队阶段直接取消）"
                raise ConflictError(f"取消已由 {confirmer} 结束")
            if task["status"] != "cancel_requested":
                raise ConflictError("任务当前没有待确认的取消请求")
            if task["lease_owner"] and task["lease_owner"] != worker_id:
                raise ConflictError("只有持有租约的工作者可以确认取消")
            self._settle_cancellation(
                connection, repository, task, request, status="accepted",
                acknowledged_by=worker_id, acknowledged_at=now, effective_at=now,
                released_quota="running", note="工作者确认取消，运行配额立即释放",
                action="cancel_acknowledged", actor=worker_id,
                reason="工作者确认取消请求", now=now,
            )
            return self._cancel_outcome(repository, task_id)

    @staticmethod
    def _settle_cancellation(connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, request: sqlite3.Row | None, *, status: str, acknowledged_by: str, acknowledged_at: str, effective_at: str | None, released_quota: str, note: str, action: str, actor: str, reason: str, now: str) -> None:
        before = dict(task)
        connection.execute(
            "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',cancel_acknowledged_by=?,cancel_acknowledged_at=?,cancel_effective_at=?,finished_at=COALESCE(finished_at,?),updated_at=?,version=version+1 WHERE id=?",
            (acknowledged_by, acknowledged_at, effective_at or "", effective_at, now, task["id"]),
        )
        if request is not None and request["status"] == "pending":
            repository.settle_cancel_request(
                int(request["id"]), status=status, acknowledged_by=acknowledged_by,
                acknowledged_at=acknowledged_at, effective_at=effective_at,
                released_quota=released_quota, note=note, now=now,
            )
        after = dict(repository.task_by_id(task["id"]))
        repository.add_intervention(task_id=task["id"], actor=actor, action=action, reason=reason, before=before, after=after, batch_key="", now=now)

    @staticmethod
    def _cancel_outcome(repository: ComputeRepository, task_id: int) -> dict[str, Any]:
        task = dict(repository.task_by_id(task_id))
        request = repository.cancel_request(task_id)
        if request is not None:
            request = dict(request)
            task["cancellation"] = {
                "request_id": request["id"],
                "requested_by": request["requested_by"],
                "requested_at": request["requested_at"],
                "deadline_at": request["deadline_at"],
                "request_status": request["status"],
                "acknowledged_by": request["acknowledged_by"],
                "acknowledged_at": request["acknowledged_at"],
                "effective_at": request["effective_at"],
                "released_quota": request["released_quota"],
                "note": request["note"],
            }
        return task

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute(
                "UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',cancel_requested_by='',cancel_requested_at='',cancel_ack_deadline='',cancel_acknowledged_by='',cancel_acknowledged_at='',cancel_effective_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?",
                (chosen, now, now, task["id"]),
            )
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
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            due_rows = connection.execute(
                "SELECT t.* FROM compute_tasks t WHERE t.status='cancel_requested' AND "
                "((t.cancel_ack_deadline<>'' AND t.cancel_ack_deadline<=?) OR (t.lease_expires_at<>'' AND t.lease_expires_at<?)) ORDER BY t.id",
                (now, now),
            ).fetchall()
            for task in due_rows:
                request = repository.cancel_request(int(task["id"]))
                deadline_due = bool(task["cancel_ack_deadline"]) and task["cancel_ack_deadline"] <= now
                if deadline_due:
                    note = "工作者未在确认时限内应答，恢复程序代为结束并释放运行配额"
                    reason_text = "取消确认超时，恢复程序代为结束"
                else:
                    note = "工作者租约过期且未确认取消，恢复程序代为结束并释放运行配额"
                    reason_text = "工作者失联，恢复程序代为结束取消"
                self._settle_cancellation(
                    connection, repository, task, request, status="accepted",
                    acknowledged_by=actor, acknowledged_at=now, effective_at=now,
                    released_quota="running", note=note,
                    action="cancel_timeout_recovery", actor=actor,
                    reason=reason_text, now=now,
                )
                cancelled.append(int(task["id"]))
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
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

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
