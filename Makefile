
lint:
	black --check .
	isort --check-only .
	flake8 .


format:
	black .
	isort .
