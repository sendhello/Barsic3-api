FROM python:3.12.1

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
# pip
ENV PIP_NO_CACHE_DIR=off
ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PIP_DEFAULT_TIMEOUT=100
# Poetry no venv
ENV POETRY_VIRTUALENVS_CREATE=false
# do not ask any interactive question
ENV POETRY_NO_INTERACTION=1
ENV APP_PATH="/opt/app"

WORKDIR $APP_PATH
COPY poetry.lock pyproject.toml ./
RUN pip install poetry && poetry install --no-dev
COPY . .

ENTRYPOINT sh barsic_web.sh
