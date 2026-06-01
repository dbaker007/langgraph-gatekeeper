.PHONY: test clean

test:
	uv run pytest tests/ -v

clean:
	find . -name "*.db" -type f -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
	find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null
