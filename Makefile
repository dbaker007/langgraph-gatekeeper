.PHONY: all clean lint test help

# Default target sequences the entire validation matrix in strict order
all: clean lint test

# Runs the complete static analysis suite using ephemeral tool runner environments
lint:
	@echo "=== RUNNING RUFF STATIC CODE CHECKS ==="
	uv tool run ruff check src/ tests/
	@echo "=== RUNNING MYPY STATIC TYPE CHECKING ==="
	uv tool run mypy --ignore-missing-imports src/

# FIXED REGION: Preserves your isolated framework target selection filter exactly as written
test:
	@if [ -n "$(TEST_TARGET)" ]; then \
		echo "Running isolated framework target filter: -k '$(TEST_TARGET)'"; \
		uv run pytest tests/ -v -k "$(TEST_TARGET)"; \
	else \
		echo "Running full framework testing suite..."; \
		uv run pytest tests/ -v; \
	fi

# Wipes temporary test parameters, validation pools, and cache artifacts out-of-band
clean:
	@echo "=== RESEEDING BUILD CACHE POOLS ==="
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Self-documenting target matrix helper
help:
	@echo "Framework Automation Command Matrix:"
	@echo "  make lint  - Runs ruff code styles and mypy types via ephemeral caches."
	@echo "  make test  - Executes framework testing suite (Supports TEST_TARGET=\"pattern\")."
	@echo "  make clean - Purges all build parameters, compilation pools, and test artifacts."
	@echo "  make all   - Sequences clean, lint, and test loops into a single command pass."
