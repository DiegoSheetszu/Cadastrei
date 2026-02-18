from typing import Any, Dict, Mapping
from urllib.parse import urlparse, urlunparse

import httpx

from config.settings import settings

_LOGIN_PATHS = (
    "/login",
    "/v1/login",
    "/api/login",
    "/api/v1/login",
    "/auth/login",
    "/v1/auth/login",
)


def _cfg_get(config: Mapping[str, Any] | None, key: str, default: str) -> str:
    if config is None:
        return str(default or "").strip()
    return str(config.get(key) or default or "").strip()


def _append_candidate(candidates: list[str], seen: set[str], value: str) -> None:
    url = str(value or "").strip()
    if not url:
        return
    key = url.lower()
    if key in seen:
        return
    seen.add(key)
    candidates.append(url)


def _with_path(url: str, path: str) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return None
    normalized_path = "/" + str(path or "").strip().lstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc, normalized_path, "", "", ""))


def _with_port(url: str, port: int, path: str) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.hostname:
        return None
    netloc = f"{parsed.hostname}:{int(port)}"
    normalized_path = "/" + str(path or "").strip().lstrip("/")
    return urlunparse((parsed.scheme, netloc, normalized_path, "", "", ""))


def _build_login_candidates(login_url: str, base_url: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    _append_candidate(candidates, seen, login_url)

    if base_url:
        for path in _LOGIN_PATHS:
            _append_candidate(candidates, seen, _with_path(base_url, path) or "")

    parsed_login = urlparse(login_url) if login_url else None
    if parsed_login and parsed_login.scheme and parsed_login.netloc:
        path = parsed_login.path or "/login"
        if path == "/":
            path = "/login"
        if not path.lower().endswith("/login"):
            _append_candidate(candidates, seen, _with_path(login_url, "/login") or "")
        for alt_path in _LOGIN_PATHS:
            if path.lower() == alt_path.lower():
                continue
            _append_candidate(candidates, seen, _with_path(login_url, alt_path) or "")

        if parsed_login.port in {8087, 8088}:
            alt_port = 8088 if parsed_login.port == 8087 else 8087
            for alt_path in _LOGIN_PATHS:
                _append_candidate(candidates, seen, _with_port(login_url, alt_port, alt_path) or "")

    parsed_base = urlparse(base_url) if base_url else None
    if parsed_base and parsed_base.scheme and parsed_base.hostname and parsed_base.port in {8087, 8088}:
        alt_port = 8088 if parsed_base.port == 8087 else 8087
        for alt_path in _LOGIN_PATHS:
            _append_candidate(candidates, seen, _with_port(base_url, alt_port, alt_path) or "")

    return candidates


def _extract_token(payload: Any) -> str:
    if not isinstance(payload, Mapping):
        return ""
    for key in ("token", "access_token", "jwt", "id_token"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def login_api(timeout: float = 30.0, config: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    login_url = _cfg_get(config, "login_url", settings.api_login_url)
    base_url = _cfg_get(config, "base_url", settings.api_base_url)
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

    candidates = _build_login_candidates(login_url, base_url)
    if not candidates:
        raise ValueError("Nao foi possivel montar URL de login para autenticacao.")

    erros: list[str] = []
    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for candidate in candidates:
            try:
                response = client.post(candidate, json=payload, headers=headers)
            except Exception as exc:
                erros.append(f"{candidate} -> erro de conexao: {exc}")
                continue

            status_code = int(response.status_code)
            if status_code in {404, 405}:
                erros.append(f"{candidate} -> HTTP {status_code}")
                continue

            if status_code in {401, 403}:
                erros.append(f"{candidate} -> HTTP {status_code} (credenciais rejeitadas)")
                continue

            if status_code >= 400:
                erros.append(f"{candidate} -> HTTP {status_code}")
                continue

            try:
                data = response.json()
            except Exception:
                erros.append(f"{candidate} -> resposta nao-JSON (HTTP {status_code})")
                continue

            token = _extract_token(data)
            if not token:
                erros.append(f"{candidate} -> resposta sem token")
                continue

            normalized = dict(data)
            normalized.setdefault("token", token)
            normalized["_login_url"] = candidate
            return normalized

    resumo = " | ".join(erros[:5]) if erros else "sem detalhes"
    raise ValueError(f"Nao foi possivel autenticar. Tentativas: {resumo}")
