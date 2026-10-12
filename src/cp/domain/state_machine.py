from cp.domain.enums import TaskStatus, TaskType, TenantStatus

TENANT_ACTION_GUARDS: dict[str, tuple[frozenset[TenantStatus], TenantStatus, TaskType]] = {
    "patch": (frozenset({TenantStatus.ACTIVE}), TenantStatus.UPDATING, TaskType.UPDATE),
    "delete": (
        frozenset({TenantStatus.ACTIVE, TenantStatus.FAILED}),
        TenantStatus.DESTROYING,
        TaskType.DESTROY,
    ),
}

TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.ACCEPTED: frozenset({TaskStatus.IN_PROGRESS}),
    TaskStatus.IN_PROGRESS: frozenset({TaskStatus.DONE, TaskStatus.FAILED}),
    TaskStatus.DONE: frozenset(),
    TaskStatus.FAILED: frozenset(),
}

TENANT_ON_TASK_OUTCOME: dict[tuple[TaskType, TaskStatus], tuple[TenantStatus, TenantStatus]] = {
    (TaskType.DEPLOY, TaskStatus.DONE): (TenantStatus.PROVISIONING, TenantStatus.ACTIVE),
    (TaskType.UPDATE, TaskStatus.DONE): (TenantStatus.UPDATING, TenantStatus.ACTIVE),
    (TaskType.DESTROY, TaskStatus.DONE): (TenantStatus.DESTROYING, TenantStatus.DESTROYED),
    (TaskType.DEPLOY, TaskStatus.FAILED): (TenantStatus.PROVISIONING, TenantStatus.FAILED),
    (TaskType.UPDATE, TaskStatus.FAILED): (TenantStatus.UPDATING, TenantStatus.FAILED),
    (TaskType.DESTROY, TaskStatus.FAILED): (TenantStatus.DESTROYING, TenantStatus.FAILED),
}
