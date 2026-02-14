import json
import threading
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from Consultas_dbo.afastamentos.afastamentos import RepositorioAfastamentos
from Consultas_dbo.cadastro_motoristas.cadastro_motoristas import RepositorioCadastroMotoristas
from Ferramentas.montar_payload_afastamentos import montar_payload_afastamentos
from Ferramentas.montar_payload_motoristas import montar_payload_motoristas


class IntegracaoApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Integracao ATS - Execucao API")
        self.geometry("980x680")
        self.minsize(860, 560)

        self.engine = None
        self.token = None

        self.database_var = tk.StringVar(value="Vetorh_Prod")
        self.limit_var = tk.StringVar(value="1")
        self.status_var = tk.StringVar(value="Status: pronto")

        self._build_ui()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        top = ttk.Frame(self, padding=12)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(7, weight=1)

        ttk.Label(top, text="Database:").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.database_var, width=24).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(top, text="Limite:").grid(row=0, column=2, sticky="w")
        ttk.Entry(top, textvariable=self.limit_var, width=8).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Button(top, text="Login", command=lambda: self._run_async(self._login)).grid(row=0, column=4, padx=(0, 8))
        ttk.Button(top, text="Motoristas", command=lambda: self._run_async(self._executar_motoristas)).grid(row=0, column=5, padx=(0, 8))
        ttk.Button(top, text="Afastamentos", command=lambda: self._run_async(self._executar_afastamentos)).grid(row=0, column=6, padx=(0, 8))
        ttk.Button(top, text="Executar ambos", command=lambda: self._run_async(self._executar_ambos)).grid(row=0, column=7, sticky="w")

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

    def _ensure_engine(self):
        if self.engine is None:
            self.engine = ativar_engine(self.database_var.get().strip() or "Vetorh_Prod")

    def _login(self):
        self._set_status("Status: autenticando...")
        auth = login_api()
        self.token = auth.get("token")
        exp = auth.get("exp")
        self._set_status("Status: autenticado")
        self._log(f"Login ok. Expira em: {exp}")

    def _executar_motoristas(self):
        self._set_status("Status: executando motoristas...")
        self._ensure_engine()

        repo = RepositorioCadastroMotoristas(self.engine)
        registros = repo.buscar_dados_cadastro_motoristas(limit=self._get_limit())
        payload = montar_payload_motoristas(registros)

        self._log(f"[Motoristas] Banco: {len(registros)}")
        self._log(f"[Motoristas] Payload: {len(payload)}")
        self._log(json.dumps(payload[:1], ensure_ascii=False, indent=2, default=str) if payload else "[Motoristas] Sem payload")
        self._set_status("Status: motoristas finalizado")

    def _executar_afastamentos(self):
        self._set_status("Status: executando afastamentos...")
        self._ensure_engine()

        repo = RepositorioAfastamentos(self.engine)
        registros = repo.buscar_dados_afastamentos(limit=self._get_limit())
        payload = montar_payload_afastamentos(registros)

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
