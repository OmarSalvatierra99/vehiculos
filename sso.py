"""Shared SSO cookie helpers for the portfolio apps."""

from __future__ import annotations

import os
from time import time
from typing import Any

from flask import Request, Response, current_app, request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer


DEFAULT_COOKIE_NAME = "portfolio_sso"
DEFAULT_COOKIE_DOMAIN = ".omar-xyz.shop"
DEFAULT_MAX_AGE_SECONDS = 12 * 60 * 60
COOKIE_SALT = "portfolio-sso-v1"
DEFAULT_SHARED_SECRET = "3e8d47c51b8a6f861534abb68b3fb318582c061e988ae12911d535db889570e8"


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default) or "").strip()


def _secret() -> str:
    return _env("PORTFOLIO_SSO_SECRET", DEFAULT_SHARED_SECRET)


def cookie_name() -> str:
    return _env("PORTFOLIO_SSO_COOKIE_NAME", DEFAULT_COOKIE_NAME)


def cookie_domain() -> str:
    return _env("PORTFOLIO_SSO_COOKIE_DOMAIN", DEFAULT_COOKIE_DOMAIN)


def max_age_seconds() -> int:
    raw_value = _env("PORTFOLIO_SSO_MAX_AGE_SECONDS", str(DEFAULT_MAX_AGE_SECONDS))
    try:
        return max(60, int(raw_value))
    except ValueError:
        return DEFAULT_MAX_AGE_SECONDS


def _cookie_secure() -> bool:
    raw_value = _env("PORTFOLIO_SSO_COOKIE_SECURE", "1").lower()
    return raw_value not in {"0", "false", "no", "off"}


def _serializer() -> URLSafeTimedSerializer | None:
    secret = _secret()
    if not secret:
        return None
    return URLSafeTimedSerializer(secret_key=secret, salt=COOKIE_SALT)


def normalize_sso_username(username: Any) -> str:
    return str(username or "").strip().lower()


def read_sso_username(source_request: Request | None = None) -> str:
    serializer = _serializer()
    if serializer is None:
        return ""

    req = source_request or request
    token = req.cookies.get(cookie_name(), "")
    if not token:
        return ""

    try:
        payload = serializer.loads(token, max_age=max_age_seconds())
    except SignatureExpired:
        return ""
    except BadSignature:
        return ""

    if not isinstance(payload, dict) or payload.get("v") != 1:
        return ""
    return normalize_sso_username(payload.get("u"))


def set_sso_cookie(response: Response, username: Any) -> Response:
    serializer = _serializer()
    normalized = normalize_sso_username(username)
    if serializer is None or not normalized:
        current_app.logger.warning("PORTFOLIO_SSO_SECRET is not configured; SSO cookie not set")
        return response

    payload = {"v": 1, "u": normalized, "iat": int(time())}
    response.set_cookie(
        cookie_name(),
        serializer.dumps(payload),
        max_age=max_age_seconds(),
        path="/",
        domain=cookie_domain() or None,
        secure=_cookie_secure(),
        httponly=True,
        samesite="Lax",
    )
    return response


def clear_sso_cookie(response: Response) -> Response:
    name = cookie_name()
    domain = cookie_domain()
    response.delete_cookie(name, path="/", domain=domain or None)
    if domain:
        response.delete_cookie(name, path="/")
    return response
