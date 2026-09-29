# 职业教学任务运营服务

这是一个面向职业院校教务团队、授课教师和课程管理员的 Python 后端服务，用于管理课程任务模板、学员提交、执行队列、教师工作者、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

课程任务运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。

## 取消请求的确认与超时收敛

学员退出等取消请求具备明确的确认与超时收敛路径：

- `POST /api/compute/tasks/{id}/cancel`：值班老师发起取消。排队任务立即转为 `cancelled` 并释放排队配额；运行中任务转为 `cancel_requested`，记录发起人、发起时刻与确认截止时刻（`ack_timeout_seconds`，默认 30 秒），运行配额在确认前保持占用。
- `POST /api/compute/tasks/{id}/cancel/acknowledge`：持有租约的工作者确认取消，立即释放运行配额并收敛为 `cancelled`。取消挂起后心跳、成功/失败回报均被拒绝（停止后续普通回报）。
- `POST /api/compute/recovery/expired-leases`：工作者在确认时限内未应答（确认超时）或租约已过期（失联）时，恢复程序代为结束，终态仍为 `cancelled`，运行配额释放，确认者记录为 `recovery-worker`。
- 取消与合格成绩竞争时以先持久化者为准：取消先到则迟到成绩落 `result_rejected` 干预记录但不生成结果版本，终态保持取消；成绩先到则取消请求被拒，合格终态保留。
- 取消请求支持 `idempotency_key`，重复发起、重复确认均返回首次处理结果（含首次生效时刻与释放的配额），不产生新的干预记录。

`GET /api/compute/task-details/{id}` 的 `cancel_requests` 与接口响应中的 `cancellation` 字段给出相同结论：谁发起（`requested_by`/`requested_at`）、谁确认（`acknowledged_by`/`acknowledged_at`）、何时生效（`effective_at`）、释放了哪项配额（`released_quota`：`queued` 或 `running`）。

