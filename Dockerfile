FROM sendhello/barsic3-api:3.2.3

WORKDIR $APP_PATH

COPY poetry.lock pyproject.toml ./
RUN pip install poetry && poetry install
COPY . .

ENTRYPOINT sh barsic_web.sh
