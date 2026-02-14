from typing import Any, Dict

import httpx

from config.settings import settings


def login_api(timeout: float = 30.0) -> Dict[str, Any]:
    if not settings.api_login_url:
        raise ValueError("API_LOGIN_URL nao configurada no .env")
    if not settings.api_user or not settings.api_pass:
        raise ValueError("API_USER/API_PASS nao configurados no .env")

    payload = {
        "user": settings.api_user,
        "pass": settings.api_pass,
    }
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=timeout) as client:
        response = client.post(settings.api_login_url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

    if "token" not in data:
        raise ValueError("Resposta de login sem token")

    return data
