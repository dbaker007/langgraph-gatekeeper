"""Domain Verification Suite: Programmatic State Mutation Firewalls.

This suite exercises the administrative state modification boundaries, verifying
that out-of-band updates to active checkpoints via update_state are strictly protected
by a programmatically enforced capability firewall layer (core/graph.py).
"""

import uuid

import pytest


def test_proxy_lock_blocks_unauthorized_state_mutations(secure_mutation_graph):
    """SCENARIO: Verifies that a user profile lacking the explicit 'mutate_state' claim

    is forcefully blocked at the boundary with a hard PermissionError exception.
    """
    malicious_config = {
        "configurable": {
            "thread_id": f"t_mutation_hack_{uuid.uuid4()}",
            "user_id": "malicious_actor",
            "user_claims": ["assign_analyst"],
        }
    }

    with pytest.raises(PermissionError) as exc_info:
        secure_mutation_graph.update_state(malicious_config, {"result_data": "HACKED"})

    assert "denied administrative state mutation" in str(exc_info.value)


def test_proxy_lock_allows_authorized_state_mutations(secure_mutation_graph):
    """SCENARIO: Verifies that an administrative service profile holding 'mutate_state'

    can cleanly inject parameter state modifications straight down to the checkpoint values.
    """
    thread_id = f"t_mutation_ok_{uuid.uuid4()}"
    authorized_config = {
        "configurable": {
            "thread_id": thread_id,
            "user_id": "trusted_system_service",
            "user_claims": ["mutate_state"],
        }
    }

    secure_mutation_graph.update_state(authorized_config, {"test_payload": "CLEARED"})

    snapshot = secure_mutation_graph.get_state(authorized_config)
    assert snapshot.values.get("test_payload") == "CLEARED"
