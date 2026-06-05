.PHONY: test clean

# =============================================================================
# MASTER TESTING UTILITIES
# =============================================================================
# This special pattern traps any arguments passed after the word "test" 
# and converts them straight into a targeted pytest filter flag string.
ifeq (test,$(firstword $(MAKECMDGOALS)))
  TEST_TARGET := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  $(eval $(TEST_TARGET):;@:)
endif

.PHONY: test
test:
	@if [ -n "$(TEST_TARGET)" ]; then \
		echo "Running isolated framework target filter: -k '$(TEST_TARGET)'"; \
		uv run pytest tests/ -v -k "$(TEST_TARGET)"; \
	else \
		echo "Running full framework testing suite..."; \
		uv run pytest tests/ -v; \
	fi

clean:
	find . -name "*.db" -type f -delete
	find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
	find . -name ".pytest_cache" -type d -exec rm -rf {} + 2>/dev/null
