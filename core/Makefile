.PHONY: test clean

test:
	PYTHONPATH=../../ uv run pytest tests/ -v

clean:
	rm -rf .pytest_cache/
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null
