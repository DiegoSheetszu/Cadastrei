from typing import Any, Dict, Mapping

import httpx

from config.settings import settings


def _cfg_get(config: Mapping[str, Any] | None, key: str, default: str) -> str:
    if config is None:
        return str(default or "").strip()
    return str(config.get(key) or default or "").strip()


def login_api(timeout: float = 30.0, config: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    login_url = _cfg_get(config, "login_url", settings.api_login_url)
    user = _cfg_get(config, "usuario", settings.api_user)
    password = _cfg_get(config, "senha", settings.api_pass)

    if not login_url:
        raise ValueError("API_LOGIN_URL nao configurada no .env")
    if not user or not password:
        raise ValueError("API_USER/API_PASS nao configurados no .env")

    payload = {
        "user": user,
        "pass": password,
    }
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    with httpx.Client(timeout=timeout) as client:
        response = client.post(login_url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

    if "token" not in data:
        raise ValueError("Resposta de login sem token")

    return data
