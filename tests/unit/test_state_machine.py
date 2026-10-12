from cp.domain.enums import TaskStatus, TaskType, TenantStatus
from cp.domain.state_machine import (
    TASK_TRANSITIONS,
    TENANT_ACTION_GUARDS,
    TENANT_ON_TASK_OUTCOME,
)


def test_tenant_action_guards_match_supported_operations() -> None:
    assert TENANT_ACTION_GUARDS["patch"] == (
        frozenset({TenantStatus.ACTIVE}),
        TenantStatus.UPDATING,
        TaskType.UPDATE,
    )
    assert TENANT_ACTION_GUARDS["delete"][0] == frozenset(
        {TenantStatus.ACTIVE, TenantStatus.FAILED}
    )


def test_task_transitions_have_only_expected_successors() -> None:
    assert TASK_TRANSITIONS == {
        TaskStatus.ACCEPTED: frozenset({TaskStatus.IN_PROGRESS}),
        TaskStatus.IN_PROGRESS: frozenset({TaskStatus.DONE, TaskStatus.FAILED}),
        TaskStatus.DONE: frozenset(),
        TaskStatus.FAILED: frozenset(),
    }


def test_terminal_outcomes_cover_all_task_types() -> None:
    assert len(TENANT_ON_TASK_OUTCOME) == 6
    assert TENANT_ON_TASK_OUTCOME[(TaskType.DESTROY, TaskStatus.DONE)] == (
        TenantStatus.DESTROYING,
        TenantStatus.DESTROYED,
    )
