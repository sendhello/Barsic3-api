# Barsic3 API

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109.2-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.md)
[![Docker](https://img.shields.io/badge/docker-enabled-blue.svg)](https://www.docker.com/)
[![CodeQL](https://github.com/sendhello/Barsic3-api/actions/workflows/codeql.yml/badge.svg)](https://github.com/sendhello/Barsic3-api/actions/workflows/codeql.yml)

A comprehensive reporting service for extracting, transforming, and storing reports from the Datakrat Bars2 system. Built with FastAPI and designed for efficient data processing and report generation.

## Table of Contents

- [Features](#features)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Development](#development)
- [Environment Variables](#environment-variables)
- [Deployment](#deployment)
- [Security](#security)
- [Contributing](#contributing)
- [License](#license)
- [Authors](#authors)

## Features

- 📊 **Report Processing**: Extract and transform reports from Datakrat Bars2 system
- 🔗 **Google Integration**: Google Sheets and Drive API integration
- 💾 **Yandex Disk Storage**: Automated report backup to Yandex Disk
- 📈 **Excel Processing**: Advanced Excel file generation and manipulation
- 🤖 **Telegram Integration**: Bot notifications and interactions
- 🗄️ **Database Support**: PostgreSQL and MSSQL Server connectivity
- ⚡ **High Performance**: Async/await throughout with Redis caching
- 🐳 **Containerized**: Docker and Docker Compose ready
- 📋 **REST API**: Complete RESTful API for report management
- 🔧 **Configurable**: Flexible environment-based configuration

## Tech Stack

- **Backend Framework**: [FastAPI](https://fastapi.tiangolo.com/) 0.109.2
- **Language**: Python 3.11+
- **Database**: PostgreSQL with [SQLAlchemy](https://www.sqlalchemy.org/) (async)
- **Cache**: Redis 5.0+
- **Excel Processing**: OpenPyXL, lxml
- **Google APIs**: Google API Python Client
- **Telegram Bot**: Aiogram 3.4+
- **Cloud Storage**: Yandex Disk integration
- **Migration**: Alembic
- **Validation**: Pydantic
- **ASGI Server**: Uvicorn/Gunicorn
- **Containerization**: Docker & Docker Compose

## Architecture

The Barsic3 API serves as a data processing hub that connects multiple external systems and provides unified access to reporting functionality.

```
  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
  │   Web Clients   │    │  Mobile Apps    │    │  External APIs  │
  └─────────┬───────┘    └────────┬────────┘    └─────────┬───────┘
            │                     │                       │
            └─────────────────────┼───────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │     Barsic3 API           │
                    │   (FastAPI + Reports)     │
                    └─────────────┬─────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
    ┌─────────▼─────────┐ ┌───────▼────────┐ ┌────────▼─────────┐
    │   PostgreSQL      │ │     Redis      │ │   Datakrat Bars2 │
    │   (Reports Data)  │ │    (Cache)     │ │   (Source Data)  │
    └───────────────────┘ └────────────────┘ └──────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
    ┌─────────▼─────────┐ ┌───────▼────────┐ ┌────────▼─────────┐
    │   Google APIs     │ │  Yandex Disk   │ │   Telegram Bot   │
    │   (Sheets/Drive)  │ │   (Storage)    │ │ (Notifications)  │
    └───────────────────┘ └────────────────┘ └──────────────────┘
```

## Prerequisites

- **Python**: 3.11 or higher
- **Docker**: 20.10 or higher
- **Docker Compose**: 2.0 or higher
- **PostgreSQL**: 12 or higher (if running locally)
- **Redis**: 5.0 or higher (if running locally)

## Installation

### Option 1: Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Barsic3-api
   ```

2. **Build and start services**
   ```bash
   docker compose up --build
   ```

The service will be available at `http://localhost/barsic`

### Option 2: Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Barsic3-api
   ```

2. **Create virtual environment**
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies with Poetry**
   ```bash
   pip install poetry
   poetry install
   ```

4. **Set up environment variables**
   ```bash
   # Create .env file with required variables (see Environment Variables section)
   ```

5. **Start infrastructure services**
   ```bash
   docker compose -f docker-compose-dev.yml up -d
   ```

6. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

7. **Start the application**
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

## Usage

### Basic Report Operations

```bash
# Health check
curl -X GET "http://localhost/health"

# Get available report groups
curl -X GET "http://localhost/barsic/api/v1/report-groups"

# Generate a report
curl -X POST "http://localhost/barsic/api/v1/reports" \
  -H "Content-Type: application/json" \
  -d '{
    "report_type": "bars_summary",
    "date_from": "2025-01-01",
    "date_to": "2025-01-31"
  }'
```

## API Documentation

Once the service is running, you can access:

- **Swagger UI**: [http://localhost/barsic/api/v1/docs](http://localhost/barsic/api/v1/docs)
- **OpenAPI JSON**: [http://localhost/barsic/api/v1/openapi.json](http://localhost/barsic/api/v1/openapi.json)

### Main Endpoints

| Method | Endpoint                     | Description                    |
|--------|------------------------------|--------------------------------|
| `GET`  | `/api/v1/report-groups`      | List available report groups   |
| `GET`  | `/api/v1/report-settings`    | Get report configuration       |
| `POST` | `/api/v1/reports`            | Generate new report            |
| `GET`  | `/api/v1/reports/{id}`       | Get specific report            |
| `GET`  | `/api/v1/google-report-ids`  | Get Google Sheets report IDs   |
| `GET`  | `/api/v1/bars`               | Get bars data                  |
| `GET`  | `/api/v1/report-elements`    | Get report element definitions |

## Development

### Code Quality Tools

We use several tools to maintain code quality:

```bash
# Format code
black .

# Sort imports
isort .

# Lint code
flake8 .
```

### Database Operations

```bash
# Create new migration
alembic revision --autogenerate -m "Description of changes"

# Apply migrations
alembic upgrade head

# Downgrade migration
alembic downgrade -1
```

### Development Environment

For development with local database and external services:

```bash
# Start development environment
docker compose -f docker-compose-dev.yml up

# Start without the main application (for local development)
docker compose -f docker-compose-dev-without-app.yml up
```

## Environment Variables

| Variable                | Default     | Description                              |
|-------------------------|-------------|------------------------------------------|
| `DEBUG`                 | `False`     | Enable debug mode                        |
| `PROJECT_NAME`          | `Barsic`    | Service name (displayed in docs)         |
| `POSTGRES_HOST`         | `localhost` | PostgreSQL hostname                      |
| `POSTGRES_PORT`         | `5432`      | PostgreSQL port                          |
| `POSTGRES_DB`           | `barsic`    | Database name                            |
| `POSTGRES_USER`         | `app`       | PostgreSQL username                      |
| `POSTGRES_PASSWORD`     |             | PostgreSQL password                      |
| `REDIS_HOST`            | `redis`     | Redis hostname                           |
| `REDIS_PORT`            | `6379`      | Redis port                               |

### External Service Integration

The service integrates with several external systems:

- **Datakrat Bars2**: Source system for report data
- **Google APIs**: For Google Sheets and Drive integration
- **Yandex Disk**: For automated report storage and backup
- **Telegram Bot**: For notifications and status updates
- **MSSQL Server**: Additional database connectivity for legacy systems

### Setting Up External Services

#### Microsoft ODBC 18 (macOS)
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

#### Yandex Disk Token
Create a Yandex application and obtain a token following the instructions at:
https://yandex.ru/dev/disk/webdav/

## Deployment

### Docker Production Build

```bash
# Build production image
docker build -t barsic3-api:latest .

# Run with production settings
docker run -d \
  --name barsic3-api \
  -p 80:8000 \
  --env-file .env.prod \
  barsic3-api:latest
```

### System Requirements

- **Minimum**: 1 CPU, 1GB RAM
- **Recommended**: 2 CPU, 2GB RAM  
- **Storage**: 20GB for reports and database
- **Network**: Access to PostgreSQL, Redis, and external APIs

### Database Backup and Restore

```bash
# Create PostgreSQL backup
docker exec -it postgres pg_dump -U barsic -W barsic > /tmp/barsic.dump

# Restore from backup
docker exec -it postgres psql -U barsic -W barsic < /tmp/barsic.dump
```

## Security

Security considerations for the Barsic3 API include:

- **Input Validation**: Comprehensive validation using Pydantic
- **Database Security**: Minimal privilege database users
- **API Security**: Rate limiting and request validation
- **External API Security**: Secure token management for third-party services
- **Data Protection**: Encrypted storage of sensitive configuration

For detailed security information and vulnerability reporting, please see our [Security Policy](SECURITY.md).

**Important**: 
- Never commit secrets to version control
- Use strong, unique passwords and API keys
- Enable HTTPS in production environments
- Regularly update dependencies and security patches
- Secure external service credentials

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Run code quality checks (`flake8`, `black`, `isort`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

### Code Style Guidelines

- Follow PEP 8 style guide
- Use type hints for all functions
- Write docstrings for all public methods
- Use meaningful commit messages
- Add comprehensive tests for new features
- Update documentation for API changes

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](LICENSE.md) file for details.

Copyright 2025 Ivan Bazhenov

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Authors

- **Ivan Bazhenov** - *Initial work* - [@sendhello](https://github.com/sendhello)
  - Email: bazhenov.in@gmail.com

## Support

For support and questions:

- Create an issue on GitHub
- Check the [API documentation](http://localhost/barsic/api/v1/docs)
- Contact the maintainer via email
- Review existing issues for similar problems

---

**Built with ❤️ for efficient report processing using FastAPI and Python 3.11+**
