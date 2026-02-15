import json
import threading
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from config.settings import settings
from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos
from Ferramentas.montar_payload_afastamentos import montar_payload_afastamentos
from src.integradora.motorista_sync_service import MotoristaSyncService


class IntegracaoApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Integracao ATS - Execucao API")
        self.geometry("980x680")
        self.minsize(860, 560)

        self.engine_origem = None
        self.engine_destino = None
        self.database_origem_atual = None
        self.database_destino_atual = None
        self.token = None

        self.database_var = tk.StringVar(value=settings.source_database_dev)
        self.database_destino_var = tk.StringVar(value=settings.target_database)
        self.limit_var = tk.StringVar(value="1")
        self.status_var = tk.StringVar(value="Status: pronto")

        self._build_ui()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        top = ttk.Frame(self, padding=12)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(9, weight=1)

        ttk.Label(top, text="Origem:").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.database_var, width=24).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(top, text="Limite:").grid(row=0, column=2, sticky="w")
        ttk.Entry(top, textvariable=self.limit_var, width=8).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Label(top, text="Destino:").grid(row=0, column=4, sticky="w")
        ttk.Entry(top, textvariable=self.database_destino_var, width=18).grid(row=0, column=5, padx=(6, 14), sticky="w")

        ttk.Button(top, text="Login", command=lambda: self._run_async(self._login)).grid(row=0, column=6, padx=(0, 8))
        ttk.Button(top, text="Motoristas", command=lambda: self._run_async(self._executar_motoristas)).grid(row=0, column=7, padx=(0, 8))
        ttk.Button(top, text="Afastamentos", command=lambda: self._run_async(self._executar_afastamentos)).grid(row=0, column=8, padx=(0, 8))
        ttk.Button(top, text="Executar ambos", command=lambda: self._run_async(self._executar_ambos)).grid(row=0, column=9, sticky="w")

        ttk.Label(self, textvariable=self.status_var, padding=(12, 0, 12, 8)).grid(row=1, column=0, sticky="w")

        output = ttk.LabelFrame(self, text="Saida", padding=10)
        output.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 12))
        output.columnconfigure(0, weight=1)
        output.rowconfigure(0, weight=1)

        self.output_text = ScrolledText(output, wrap=tk.WORD, font=("Consolas", 10))
        self.output_text.grid(row=0, column=0, sticky="nsew")

        bottom = ttk.Frame(self, padding=(12, 0, 12, 12))
        bottom.grid(row=3, column=0, sticky="ew")
        ttk.Button(bottom, text="Limpar", command=self._clear).pack(side=tk.LEFT)

    def _run_async(self, target):
        thread = threading.Thread(target=self._safe_call, args=(target,), daemon=True)
        thread.start()

    def _safe_call(self, target):
        try:
            target()
        except Exception as exc:
            self._set_status(f"Status: erro - {exc}")
            self._log(f"ERRO: {exc}")

    def _clear(self) -> None:
        self.output_text.delete("1.0", tk.END)

    def _log(self, message: str) -> None:
        def _append():
            self.output_text.insert(tk.END, f"{message}\n")
            self.output_text.see(tk.END)

        self.after(0, _append)

    def _set_status(self, text: str) -> None:
        self.after(0, lambda: self.status_var.set(text))

    def _get_limit(self) -> int:
        try:
            value = int(self.limit_var.get().strip())
            return max(1, value)
        except Exception:
            return 1

    def _database_origem(self) -> str:
        return self.database_var.get().strip() or settings.source_database_dev

    def _database_destino(self) -> str:
        return self.database_destino_var.get().strip() or settings.target_database

    def _schema_origem(self) -> str:
        return settings.source_schema_for_database(self._database_origem())

    def _ensure_engine_origem(self):
        database = self._database_origem()
        if self.engine_origem is None or self.database_origem_atual != database:
            self.engine_origem = ativar_engine(database)
            self.database_origem_atual = database

    def _ensure_engine_destino(self):
        database = self._database_destino()
        if self.engine_destino is None or self.database_destino_atual != database:
            self.engine_destino = ativar_engine(database)
            self.database_destino_atual = database

    def _login(self):
        self._set_status("Status: autenticando...")
        auth = login_api()
        self.token = auth.get("token")
        exp = auth.get("exp")
        self._set_status("Status: autenticado")
        self._log(f"Login ok. Expira em: {exp}")

    def _executar_motoristas(self):
        self._set_status("Status: executando motoristas...")
        self._ensure_engine_origem()
        self._ensure_engine_destino()

        schema_origem = self._schema_origem()
        batch_size = self._get_limit()

        service = MotoristaSyncService(
            engine_origem=self.engine_origem,
            engine_destino=self.engine_destino,
            database_origem=self._database_origem(),
            schema_origem=schema_origem,
            schema_destino=settings.target_schema,
            tabela_destino=settings.target_motorista_table,
            batch_size=batch_size,
        )
        resultado = service.executar_ciclo()

        self._log(f"[Motoristas] Origem: {self._database_origem()} ({schema_origem})")
        self._log(f"[Motoristas] Destino: {self._database_destino()}.{settings.target_schema}.{settings.target_motorista_table}")
        self._log(f"[Motoristas] Alterados R034FUN: {resultado.alterados_fun}")
        self._log(f"[Motoristas] Alterados R034CPL: {resultado.alterados_cpl}")
        self._log(f"[Motoristas] NumCad processados: {resultado.numcads_processados}")
        self._log(f"[Motoristas] Registros origem: {resultado.registros_origem}")
        self._log(f"[Motoristas] Payloads validos: {resultado.payloads_validos}")
        self._log(f"[Motoristas] Eventos gerados: {resultado.eventos_gerados}")
        self._log(f"[Motoristas] Eventos inseridos: {resultado.eventos_inseridos}")
        self._set_status("Status: motoristas finalizado")

    def _executar_afastamentos(self):
        self._set_status("Status: executando afastamentos...")
        self._ensure_engine_origem()

        schema_origem = self._schema_origem()

        repo = RepositorioAfastamentos(self.engine_origem, schema_origem=schema_origem)
        registros = repo.buscar_dados_afastamentos(limit=self._get_limit())
        payload = montar_payload_afastamentos(registros)

        self._log(f"[Afastamentos] Origem: {self._database_origem()} ({schema_origem})")
        self._log(f"[Afastamentos] Banco: {len(registros)}")
        self._log(f"[Afastamentos] Payload: {len(payload)}")
        self._log(json.dumps(payload[:1], ensure_ascii=False, indent=2, default=str) if payload else "[Afastamentos] Sem payload")
        self._set_status("Status: afastamentos finalizado")

    def _executar_ambos(self):
        self._executar_motoristas()
        self._executar_afastamentos()


def iniciar_interface() -> None:
    app = IntegracaoApp()
    app.mainloop()
