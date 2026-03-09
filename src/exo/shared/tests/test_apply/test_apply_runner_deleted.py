from exo.shared.apply import apply_runner_deleted, apply_runner_status_updated
from exo.shared.models.model_cards import ModelId
from exo.shared.tests.conftest import get_pipeline_shard_metadata
from exo.shared.types.common import NodeId
from exo.shared.types.events import RunnerDeleted, RunnerStatusUpdated
from exo.shared.types.state import State
from exo.shared.types.worker.instances import InstanceId, MlxRingInstance
from exo.shared.types.worker.runners import RunnerId, RunnerIdle, ShardAssignments


def test_apply_runner_deleted_removes_runner():
    runner_id = RunnerId()
    state = State(runners={runner_id: RunnerIdle()})

    new_state = apply_runner_deleted(RunnerDeleted(runner_id=runner_id), state)

    assert runner_id not in new_state.runners


def test_apply_runner_deleted_noop_when_runner_absent():
    runner_id = RunnerId()
    state = State()

    new_state = apply_runner_deleted(RunnerDeleted(runner_id=runner_id), state)

    assert new_state is state


def test_apply_runner_status_updated_ignores_orphaned_runner():
    runner_id = RunnerId()
    state = State()

    new_state = apply_runner_status_updated(
        RunnerStatusUpdated(runner_id=runner_id, runner_status=RunnerIdle()), state
    )

    assert new_state is state
    assert runner_id not in new_state.runners


def test_apply_runner_status_updated_accepts_runner_with_instance():
    runner_id = RunnerId()
    node_id = NodeId("node-a")
    instance_id = InstanceId()
    instance = MlxRingInstance(
        instance_id=instance_id,
        shard_assignments=ShardAssignments(
            model_id=ModelId("test-model"),
            runner_to_shard={
                runner_id: get_pipeline_shard_metadata(
                    ModelId("test-model"), device_rank=0
                ),
            },
            node_to_runner={node_id: runner_id},
        ),
        hosts_by_node={},
        ephemeral_port=50000,
    )
    state = State(instances={instance_id: instance})

    new_state = apply_runner_status_updated(
        RunnerStatusUpdated(runner_id=runner_id, runner_status=RunnerIdle()), state
    )

    assert runner_id in new_state.runners
