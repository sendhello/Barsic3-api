from enum import Enum


ANONYMOUS = "anonymous"

GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]


class Service(str, Enum):
    async_api = "async_api"
