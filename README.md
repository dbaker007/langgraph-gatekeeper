# LangGraph Gatekeeper

An enterprise-grade, zero-trust security architecture and automated orchestration framework built natively for **LangGraph** workflows. 

This library acts as an automated, domain-agnostic compilation firewall—enforcing fine-grained pre-execution access controls (`execute` vs. `approve` turns) and out-of-band administrative thread evictions (TTL/SLA monitors) without polluting business canvas nodes or forcing partners into restrictive workflow patterns.

---

## 🏗️ Core Architectural Features

* **Compilation-Time Firewall**: Injects a global pre-execution security closure across all workflow nodes at compile time (`compile_graph_with_authorization`). Access validation is deferred until the precise microsecond of a live runtime node turn.
* **Abstract Policy Interface**: Decouples identity verification from business schemas via a universal `.authorize(user_claims, resource, action)` boundary contract, supporting standard dictionaries or custom enterprise policy providers out-of-band.
* **ContextVar Enforcement**: Enforces security boundaries natively using LangGraph's global thread configuration `ContextVar` context containers (`get_config()`), guaranteeing bulletproof protection from step one—even for flat graphs with zero internal interrupts routing straight to `END`.
* **Transaction-Safe Resumption Loop**: Implements a delayed-deletion orchestrator lifecycle primitive (`resume()`) that prevents token destruction during unauthorized resumption attempts, leaving the thread perfectly retryable for elevated manager contexts.
* **Out-of-Band Administrative Monitor**: Features a standalone polling daemon tier (`monitor.py`) that scans independent SLA registries and forcefully triggers out-of-band thread evictions using pristine framework checkpoint state mutations.

---

## 🛠️ Local Development & Setup

This library package uses **`uv`** for deterministic dependency management and development testing.

### 1. Prerequisites
Ensure you have the Python 3.12 runtime and the `uv` package manager installed on your machine.

### 2. Synchronize the Development Environment
From the root of the framework repository, initialize the virtual environment and fetch core library dependencies:

```bash
uv sync
```

---

## 🧪 Verification & Testing Suite

A standardized `Makefile` is provided to keep workspace hygiene clean. Execute the verification script to purge scratch tables, clear bytecode cache paths, and run the modular testing components:

```bash
make clean test
```

### What the Suites Validate Natively:
1. **Security Interception (`tests/test_security.py`)**: Proves that unauthorized identities attempting to stream an initial node pass or resume a frozen thread are intercepted at the gate and aborted immediately with a `PermissionError`. Missing user claims fall back securely to anonymous context restrictions (`[]`).
2. **Orchestration Lifecycles (`tests/test_framework.py`)**: Confirms that valid transactions stream unhindered, that the custom `interrupt()` wrapper harvests business keys cleanly out-of-band, and that failed resumption retries preserve token lines on disk.
3. **Background Evictions (`tests/test_ttl.py`)**: Verifies that the automated polling cycle daemon parses relational checkpoint ancestry records to forcefully eject breached threads using M2M security permission clearing blocks.
