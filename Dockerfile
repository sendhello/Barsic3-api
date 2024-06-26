FROM python:3.12-slim-bullseye

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

ENV DEBIAN_FRONTEND=noninteractive
ENV APP_PATH="/opt/app"

WORKDIR $APP_PATH

RUN apt-get update && apt-get install -y --no-install-recommends curl build-essential libpq-dev

# Установка драйвера Microsoft ODBC 18
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && /bin/bash -c 'source ~/.bashrc' \
    && apt-get install -y unixodbc-dev \
    && apt-get install -y libgssapi-krb5-2

# Понижения уровня безопасности для работы с MSSQL 2014
RUN apt-get update -yqq \
    && apt-get install -y --no-install-recommends openssl \
    && sed -i 's,^\(MinProtocol[ ]*=\).*,\1'TLSv1.0',g' /etc/ssl/openssl.cnf \
    && sed -i 's,^\(CipherString[ ]*=\).*,\1'DEFAULT@SECLEVEL=1',g' /etc/ssl/openssl.cnf\
    && rm -rf /var/lib/apt/lists/*

COPY poetry.lock pyproject.toml ./
RUN pip install poetry && poetry install --no-dev
COPY . .

ENTRYPOINT sh barsic_web.sh
