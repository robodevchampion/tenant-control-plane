import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from cp.db.repositories import ProcessedUpdateRepository, TaskRepository, TenantRepository
from cp.domain.enums import TaskStatus
from cp.domain.state_machine import TASK_TRANSITIONS, TENANT_ON_TASK_OUTCOME
from function.consumer.contracts import WorkerUpdate

logger = logging.getLogger(__name__)


class UnknownTaskUpdate(Exception):
    def __init__(self, task_id: UUID) -> None:
        super().__init__(f"Worker update references unknown task {task_id}")
        self.task_id = task_id


async def apply_worker_update(session: AsyncSession, update: WorkerUpdate) -> str:
    now = datetime.now(UTC)
    async with session.begin():
        is_new = await ProcessedUpdateRepository(session).record_if_new(
            update.update_id,
            update.task_id,
            now,
        )
        if not is_new:
            return "duplicate"

        task_repository = TaskRepository(session)
        task = await task_repository.get_for_update(update.task_id)
        if task is None:
            raise UnknownTaskUpdate(update.task_id)

        next_status = TaskStatus(update.status)
        if next_status not in TASK_TRANSITIONS[task.status]:
            logger.info(
                "Ignoring stale worker update %s for task %s in status %s",
                update.update_id,
                task.id,
                task.status.value,
            )
            return "stale"

        await task_repository.apply_worker_update(task, next_status, update.error, now)

        if next_status in (TaskStatus.DONE, TaskStatus.FAILED):
            expected_status, resulting_status = TENANT_ON_TASK_OUTCOME[(task.type, next_status)]
            updated = await TenantRepository(session).apply_task_outcome(
                task.tenant_id,
                expected_status,
                resulting_status,
                now,
            )
            if not updated:
                logger.warning(
                    "Task %s finished, but tenant %s was not in expected status %s",
                    task.id,
                    task.tenant_id,
                    expected_status.value,
                )
        return "applied"
