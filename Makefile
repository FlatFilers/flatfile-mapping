.PHONY: test lint format typecheck coverage clean

# Path to the source directory
SRC_DIR=src/flatfile_mapping

# Run pytest for the project
test:
	@pytest

# Check coverage
coverage:
	@coverage run -m pytest && coverage report -m

# Run ruff for the project
lint:
	@ruff $(SRC_DIR)

# Run mypy for type checking
typecheck:
	@mypy $(SRC_DIR)

# Format the project using black
format:
	@black $(SRC_DIR)

# Combine all the checks: lint, typecheck, and test
allchecks: lint typecheck test

# Clean up any compiled Python files
clean:
	@find $(SRC_DIR) -name "*.pyc" -delete
	@find $(SRC_DIR) -name "__pycache__" -type d -delete