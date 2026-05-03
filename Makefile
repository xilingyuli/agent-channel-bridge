.PHONY: install run dev test lint clean

install:
	pip install -e .

run:
	python -m agent_channel_bridge

dev:
	pip install -e ".[dev]"

test:
	python -m pytest tests/ -v

lint:
	python -m ruff check src/
	python -m mypy src/

clean:
	rm -rf build/ dist/ *.egg-info/ __pycache__/
	rm -rf src/agent_channel_bridge/__pycache__/
	find . -name "*.pyc" -delete
