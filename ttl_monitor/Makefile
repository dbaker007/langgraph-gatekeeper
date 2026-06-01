.PHONY: sync test clean

# 1. Sync the workspace package dependencies into the monorepo environment
sync:
	uv sync --all-packages

# 2. Run the core infrastructure unit test suite with the python path set correctly
test:
	PYTHONPATH=. uv run pytest tests/test_monitor_core.py -v

# 3. Strip out old python bytecode files and execution caches
clean:
	rm -rf .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null
