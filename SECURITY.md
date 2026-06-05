# LangGraph Gatekeeper Security Architecture

This framework implements a **Fail-Closed, Two-Class Permission-Based Access Control (PBAC)** boundary model to isolate graph execution states out-of-band.

## 🛡️ The Two-Class Permission Model

The framework separates authorization into two completely mutually exclusive actions based strictly on the public entry point called at runtime. 

### 1. Class 1: Baseline Static Node Entry Protection (`execute_graph`)
*   **Intent:** Controls whether a user context can cross the threshold of a code node for initial execution.
*   **Configuration:** Declared fluently at compilation via the chainable builder syntax.
*   **Enforcement:** Evaluated immediately at the node gate whenever `active_action == "execute"`.
*   **Example:**
    ```python
    graph = compile_graph_with_authorization(workflow).enforce_entry("target_node", required_claim="assign_analyst")
    ```

### 2. Class 2: Dynamic Resumption Hurdle Protection (`resume`)
*   **Intent:** Controls whether a supervisor can unblock a frozen `interrupt()` state checkpoint.
*   **Configuration:** Declared immutably right inside the tool node code when the hurdle is dropped on Turn 1.
*   **Enforcement:** Evaluated strictly out-of-band against the SQLite ledger database when `active_action == "resume"`. The Class 1 entry check is completely bypassed on this turn, preventing role pollution or forced superset claims.
*   **Example:**
    ```python
    interrupt(routing_key, business_context, required_claim="verify_underwriter", payload)
    ```

## 🔒 Symmetrical Overwrite Protections (Anti-Spoofing)

To eliminate network-layer privilege escalation hacks, all public orchestration entry points perform a forceful, hardcoded overwrite of the context environment variables before starting the execution stream:
*   Calling `execute_graph()` clamps `active_action` strictly to `"execute"`.
*   Calling `resume()` clamps `active_action` strictly to `"resume"`.

An external actor hitting a REST gateway has zero ability to manipulate or spoof the active operational verb.
