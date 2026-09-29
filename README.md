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

运行中的实训任务被申请退出时，取消不会立即静默生效，而是进入可复核的两阶段流程：

1. **发起**：`POST /api/compute/tasks/{id}/cancel` 由值班老师发起。排队任务立即取消并释放排队名额；运行中任务进入 `cancel_requested`，普通回报通道（成绩、失败、心跳）随即关闭，任务在 `compute_cancel_requests` 中登记发起人、原因与确认截止时间（`confirm_timeout_seconds`，默认 120 秒）。
2. **工作者确认**：`POST /api/compute/tasks/{id}/cancel/confirm` 仅允许持有租约的工作者调用，确认后任务变为 `cancelled`、释放该用户的运行名额，记录确认者与生效时间。
3. **超时收敛**：工作者失联或未在截止前确认时，`POST /api/compute/recovery/expired-leases` 代为结束，终态仍为 `cancelled`，释放运行名额，并以 `recovery_timeout` 路径留痕，名额不会再被长期占用。

终态唯一且有依据：取消请求与合格成绩竞争时，以固定时钟比较生效时刻——严格先到者成为唯一终态；同一时刻到达按“取消优先”收敛，未落定为终态的成绩不保留结果版本。取消、确认与迟到回报均幂等，重复操作回到首次处理结果（同一 `cancel_request_id`）。

接口返回与 `GET /api/compute/task-details/{id}` 历史记录共用同一份结论（`cancel_resolution`），可区分**谁发起**（`requested_by`）、**谁确认**（`confirmed_by`，恢复程序代为结束时为恢复执行者）、**何时生效**（`effective_at`）以及**释放了哪项配额**（`released_quota`：`queued_slot` 或 `running_slot`，含配额主体）。收敛路径取值：`immediate`、`worker_confirmed`、`recovery_timeout`、`simultaneous_cancel_priority`、`result_first`（成绩先生效，取消被否决）。
