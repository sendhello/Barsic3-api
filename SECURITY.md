# Security Policy

## Supported Versions

We actively support security updates for the following versions of the Barsic3 API:

| Version | Supported          |
| ------- | ------------------ |
| 3.x     | :white_check_mark: |
| < 3.0   | :x:                |

## Reporting a Vulnerability

The Barsic3 API team takes security vulnerabilities seriously. We appreciate your efforts to responsibly disclose your findings.

### How to Report

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report security vulnerabilities by emailing:
- **Primary Contact**: [bazhenov.in@gmail.com](mailto:bazhenov.in@gmail.com)
- **Subject**: `[SECURITY] Barsic3 API Vulnerability Report`

### What to Include

When reporting a vulnerability, please include the following information:
- **Description**: A clear description of the vulnerability
- **Impact**: The potential impact and severity of the issue
- **Steps to Reproduce**: Detailed steps to reproduce the vulnerability
- **Proof of Concept**: If applicable, include a proof of concept
- **Suggested Fix**: If you have suggestions for how to fix the issue
- **Environment**: Version information and environment details

### Response Timeline

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: We will provide an initial assessment within 5 business days
- **Status Updates**: We will keep you informed of our progress throughout the investigation
- **Resolution**: We aim to resolve critical vulnerabilities within 30 days

### Security Best Practices

When using the Barsic3 API, we recommend following these security best practices:

#### Environment Configuration
- Use strong, unique passwords for database connections
- Never commit secrets to version control
- Use environment-specific configuration files
- Enable HTTPS in production environments
- Regularly rotate API keys and database passwords

#### Database Security
- Use dedicated database users with minimal privileges
- Enable database connection encryption
- Regularly update PostgreSQL and Redis versions
- Monitor database access logs
- Implement proper backup encryption

#### External Service Security
- Secure Google API credentials and tokens
- Protect Yandex Disk access tokens
- Use secure Telegram Bot tokens
- Implement OAuth best practices for external integrations
- Monitor API usage and rate limits

#### API Security
- Configure appropriate rate limits based on your use case
- Monitor for suspicious activity patterns
- Implement proper input validation
- Use HTTPS for all API communications
- Log and monitor API access patterns

#### Infrastructure Security
- Keep Docker images updated
- Use non-root users in containers
- Enable container security scanning
- Implement proper network segmentation
- Secure file upload and processing

### Known Security Considerations

- **File Processing**: Excel and report file processing requires careful input validation
- **External APIs**: Multiple third-party integrations require secure credential management
- **Database Access**: Both PostgreSQL and MSSQL connections need proper security
- **Redis Cache**: Session and cache data should be properly secured
- **Report Storage**: Generated reports may contain sensitive data requiring protection

### Security Features

The Barsic3 API includes several built-in security features:
- **Input Validation**: Comprehensive input validation using Pydantic
- **Database Security**: Minimal privilege database users
- **Secure File Processing**: Safe handling of Excel and report files
- **API Rate Limiting**: Configurable request throttling
- **Secure Headers**: Proper security headers in responses
- **Environment Isolation**: Clear separation between development and production
- **Audit Logging**: Comprehensive logging for security monitoring

### Responsible Disclosure

We follow responsible disclosure practices:
- We will work with you to understand and resolve the issue
- We will not take legal action against researchers who follow this policy
- We will credit researchers who report valid vulnerabilities (unless they prefer to remain anonymous)
- We may publish security advisories after issues are resolved

### Bug Bounty

Currently, we do not have a formal bug bounty program, but we greatly appreciate security researchers who help us improve the security of our service.

## Contact

For non-security related issues, please use GitHub issues or contact the maintainer through standard channels.

For urgent security matters outside of vulnerability reports, you can also reach out via:
- GitHub: [@sendhello](https://github.com/sendhello)
- Email: [bazhenov.in@gmail.com](mailto:bazhenov.in@gmail.com)

---

*Last updated: August 2025*