from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

from Cadastro_API.login import login_api
from config.settings import settings


@dataclass
class ApiResponse:
    status_code: int
    json_data: Any
    text: str


class AtsApiClient:
    def __init__(self, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self._base_url = self._resolver_base_url()
        self._token: str | None = None
        self._client = httpx.Client(timeout=self.timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def authenticate(self, force: bool = False) -> str:
        if self._token and not force:
            return self._token

        auth_data = login_api(timeout=self.timeout_seconds)
        token = str(auth_data.get("token") or "").strip()
        if not token:
            raise ValueError("Resposta de login sem token valido.")
        self._token = token
        return token

    def post_json(
        self,
        endpoint_path: str,
        payload: dict[str, Any],
        *,
        retry_unauthorized: bool = True,
    ) -> ApiResponse:
        token = self.authenticate()
        response = self._request(endpoint_path, payload, token)

        if response.status_code == 401 and retry_unauthorized:
            token = self.authenticate(force=True)
            response = self._request(endpoint_path, payload, token)

        return self._parse_response(response)

    def _request(
        self,
        endpoint_path: str,
        payload: dict[str, Any],
        token: str,
    ) -> httpx.Response:
        endpoint = self._normalizar_endpoint(endpoint_path)
        url = urljoin(self._base_url, endpoint)
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        return self._client.post(url, json=payload, headers=headers)

    @staticmethod
    def _parse_response(response: httpx.Response) -> ApiResponse:
        text = (response.text or "").strip()
        try:
            json_data = response.json()
        except Exception:
            json_data = None
        return ApiResponse(
            status_code=int(response.status_code),
            json_data=json_data,
            text=text,
        )

    @staticmethod
    def _normalizar_endpoint(value: str) -> str:
        endpoint = str(value or "").strip()
        if not endpoint:
            raise ValueError("Endpoint da API nao informado.")
        return endpoint if endpoint.startswith("/") else f"/{endpoint}"

    @staticmethod
    def _resolver_base_url() -> str:
        base_url = str(settings.api_base_url or "").strip()
        if base_url:
            return base_url.rstrip("/") + "/"

        login_url = str(settings.api_login_url or "").strip()
        if not login_url:
            raise ValueError("API_BASE_URL ou API_LOGIN_URL precisa estar configurado.")

        parsed = urlparse(login_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"API_LOGIN_URL invalida: {login_url!r}")

        path = (parsed.path or "").rstrip("/")
        if path.lower().endswith("/login"):
            path = path[:-6]

        return f"{parsed.scheme}://{parsed.netloc}{path}/"
