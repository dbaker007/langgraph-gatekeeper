# Logistics Engine

An enterprise-grade cargo routing and fleet dispatch orchestration microservice built on top of **LangGraph** and secured via the [LangGraph Gatekeeper](https://github.com/dbaker007/langgraph-gatekeeper) framework library. 

This service demonstrates a completely decoupled, domain-agnostic approach to agentic workflow security—enforcing zero-friction role-to-permission mapping and out-of-band SLA thread evictions at runtime.

---

## 🏗️ Architecture Blueprint

The microservice exposes a secure HTTP API portal over a parallel `StateGraph` workflow canvas. It implements a strict separation of concerns across three system layers:

1. **The Gateway Tier (`gateway.py`)**: A high-performance FastAPI web layer that intercepts incoming corporate traffic, simulates cryptographic token (JWT) validation, extracts the caller's high-level organizational roles, and maps them to execution threads.
2. **The Security Boundary (`workflow.py`)**: Implements an advanced `LogisticsSecurityProvider` class that implements the universal `.authorize()` contract. This class maps broad organizational roles (e.g., `junior_dispatcher`, `fleet_director`) to fine-grained system capability claims entirely out-of-band.
3. **The Staging Registry**: Leverages the parent framework's `execute_graph` and delayed-deletion `resume` orchestrators to cleanly intercept and cache volatile task tokens without exposing tuple or snapshot complexity to the application developer.

---

## 🛠️ Local Development & Setup

This repository uses **`uv`** for fast, deterministic dependency isolation and workspace synchronization.

### 1. Prerequisites
Ensure you have the Python 3.12 runtime and `uv` package manager installed on your machine.

### 2. Synchronize the Environment
From the root of the `logistics-engine` repository, run the workspace setup command. This will build your local virtual environment and dynamically mount the parallel `langgraph-gatekeeper` package via an editable local symlink path dependency:

```bash
uv sync
```

---

## 🧪 Verification & Testing

A localized `Makefile` is provided to keep development workflows uniform and clean. 

Execute the master target runner to wipe old database artifacts, clear bytecaches, and run both the business workflow state tests and the FastAPI client endpoint integration suite:

```bash
make clean test
```

### What the Suites Verify Natively:
* **Turn 1 (Dispatch Entrance)**: Validates that an authenticated caller context carrying a standard `junior_dispatcher` role successfully passes the front-door compilation firewall on step one, freezing gracefully at an inline `interrupt()` hurdle.
* **Turn 2 (Security Interception Failure)**: Confirms that if that same unauthorized dispatcher attempts a high-privilege route approval pass, the runtime firewall catches the breach, blocks execution, and preserves the active token cache row.
* **Turn 3 (Elevated Resumption Clearance)**: Verifies that when an elevated `fleet_director` context retries the transaction, the token resolves natively, the firewall clears the gate, and the workflow finishes cleanly.

---

## 🚀 Future Roadmap: LLM & Tool Integrations
* **Ollama Local LLM Connectivity**: Standardizing on the OpenAI-compatible API standard to execute Llama 3 / Mistral model reasoning pipelines locally on a private hardware sandbox.
* **Fine-Grained Tool Firewalls**: Wrapping LangGraph `Action Nodes` with compile-time security fences to validate individual tool-call permission headers out-of-band before tools compute data.
