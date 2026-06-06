from typing import List


class BaseSecurityPolicyProvider:
    """The Least Common Denominator Enterprise Security Boundary Interface."""

    def authorize(self, user_claims: List[str], resource: str, action: str) -> bool:
        raise NotImplementedError(
            "Concrete policy providers must implement the authorize method."
        )


class DefaultDictionaryPolicyProvider(BaseSecurityPolicyProvider):
    """The Library's Built-In Least-Friction Reference Wrapper."""

    def __init__(self, policy_matrix: dict):
        self.matrix = (
            policy_matrix  # Layout: {"node_name": {"required_claim": "claim_string"}}
        )

    def authorize(self, user_claims: List[str], resource: str, action: str) -> bool:
        node_rules = self.matrix.get(resource, {})
        required_claim = node_rules.get("required_claim")

        # If no explicit access restriction is mapped for this node, clear it flatly
        if not required_claim:
            return True

        return required_claim in user_claims
