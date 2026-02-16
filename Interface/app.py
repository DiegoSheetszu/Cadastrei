import subprocess
import threading
import time
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from config.settings import settings
from src.integradora.afastamento_sync_service import AfastamentoSyncService
from src.integradora.motorista_sync_service import MotoristaSyncService


class IntegracaoApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Integracao ATS - Execucao API")
        self.geometry("980x680")
        self.minsize(860, 560)
        self.protocol("WM_DELETE_WINDOW", self._ao_fechar)

        self.engine_origem = None
        self.engine_destino = None
        self.database_origem_atual = None
        self.database_destino_atual = None
        self.token = None
        self.thread_servico_motoristas: threading.Thread | None = None
        self.thread_servico_afastamentos: threading.Thread | None = None
        self.stop_servico_motoristas: threading.Event | None = None
        self.stop_servico_afastamentos: threading.Event | None = None
        self._closing = False

        self.origens_opcoes = self._unique([settings.source_database_dev, settings.source_database_prod])
        self.destinos_opcoes = self._unique([settings.target_database])
        self.lote_opcoes = ["1", "5", "10", "20", "50", "100", "200", "500", "1000"]
        self.intervalo_opcoes = ["5", "10", "15", "30", "60", "120", "300"]
        self.win_service_motoristas_opcoes = self._unique(
            [settings.win_service_motoristas_dev, settings.win_service_motoristas_prod]
        )
        self.win_service_afastamentos_opcoes = self._unique(
            [settings.win_service_afastamentos_dev, settings.win_service_afastamentos_prod]
        )

        self.database_var = tk.StringVar(value=self.origens_opcoes[0])
        self.database_destino_var = tk.StringVar(value=self.destinos_opcoes[0])
        self.limit_var = tk.StringVar(value=self.lote_opcoes[0])
        self.ambiente_var = tk.StringVar(value=self._ambiente_por_database(self.database_var.get()))
        self.intervalo_motoristas_var = tk.StringVar(
            value=self._closest_option(str(settings.motorista_sync_interval_seconds), self.intervalo_opcoes)
        )
        self.intervalo_afastamentos_var = tk.StringVar(
            value=self._closest_option(str(settings.afastamento_sync_interval_seconds), self.intervalo_opcoes)
        )
        win_m, win_a = self._nomes_servicos_windows_por_ambiente(self.ambiente_var.get())
        self.win_service_motoristas_var = tk.StringVar(value=win_m)
        self.win_service_afastamentos_var = tk.StringVar(value=win_a)
        self.status_var = tk.StringVar(value="Status: pronto")
        self.servicos_status_var = tk.StringVar(value="Servicos: motoristas=OFF afastamentos=OFF")
        self.windows_services_status_var = tk.StringVar(value="Windows: motoristas=? afastamentos=?")

        self._build_ui()
        self.after(200, lambda: self._run_async(self._atualizar_status_windows_services))

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        top = ttk.Frame(self, padding=12)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(14, weight=1)

        ttk.Label(top, text="Ambiente:").grid(row=0, column=0, sticky="w")
        ambiente_combo = ttk.Combobox(
            top,
            textvariable=self.ambiente_var,
            values=("Homologacao", "Producao"),
            state="readonly",
            width=14,
        )
        ambiente_combo.grid(row=0, column=1, padx=(6, 8), sticky="w")
        ttk.Button(top, text="Aplicar ambiente", command=self._aplicar_ambiente).grid(
            row=0, column=2, padx=(0, 14), sticky="w"
        )

        ttk.Label(top, text="Origem:").grid(row=0, column=3, sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.database_var,
            values=self.origens_opcoes,
            state="readonly",
            width=16,
        ).grid(row=0, column=4, padx=(6, 14), sticky="w")

        ttk.Label(top, text="Destino:").grid(row=0, column=5, sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.database_destino_var,
            values=self.destinos_opcoes,
            state="readonly",
            width=14,
        ).grid(row=0, column=6, padx=(6, 14), sticky="w")

        ttk.Label(top, text="Lote:").grid(row=0, column=7, sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.limit_var,
            values=self.lote_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=8, padx=(6, 14), sticky="w")

        ttk.Button(top, text="Login", command=lambda: self._run_async(self._login)).grid(row=0, column=9, padx=(0, 8))
        ttk.Button(top, text="Motoristas", command=lambda: self._run_async(self._executar_motoristas)).grid(row=0, column=10, padx=(0, 8))
        ttk.Button(top, text="Afastamentos", command=lambda: self._run_async(self._executar_afastamentos)).grid(row=0, column=11, padx=(0, 8))
        ttk.Button(top, text="Executar ambos", command=lambda: self._run_async(self._executar_ambos)).grid(row=0, column=12, sticky="w")

        ttk.Label(top, text="Int. M(s):").grid(row=1, column=0, pady=(10, 0), sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.intervalo_motoristas_var,
            values=self.intervalo_opcoes,
            state="readonly",
            width=6,
        ).grid(row=1, column=1, pady=(10, 0), padx=(6, 14), sticky="w")
        ttk.Label(top, text="Int. A(s):").grid(row=1, column=2, pady=(10, 0), sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.intervalo_afastamentos_var,
            values=self.intervalo_opcoes,
            state="readonly",
            width=6,
        ).grid(row=1, column=3, pady=(10, 0), padx=(6, 14), sticky="w")

        ttk.Button(top, text="Iniciar M", command=lambda: self._run_async(self._iniciar_servico_motoristas)).grid(row=1, column=4, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Parar M", command=self._parar_servico_motoristas).grid(row=1, column=5, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Iniciar A", command=lambda: self._run_async(self._iniciar_servico_afastamentos)).grid(row=1, column=6, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Parar A", command=self._parar_servico_afastamentos).grid(row=1, column=7, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Iniciar ambos", command=lambda: self._run_async(self._iniciar_servicos)).grid(row=1, column=8, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Parar ambos", command=self._parar_servicos).grid(row=1, column=9, pady=(10, 0), padx=(0, 8))
        ttk.Label(top, textvariable=self.servicos_status_var).grid(row=1, column=10, columnspan=4, pady=(10, 0), sticky="w")

        ttk.Label(top, text="WinSvc M:").grid(row=2, column=0, pady=(10, 0), sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.win_service_motoristas_var,
            values=self.win_service_motoristas_opcoes,
            state="readonly",
            width=26,
        ).grid(row=2, column=1, columnspan=3, pady=(10, 0), padx=(6, 14), sticky="w")
        ttk.Label(top, text="WinSvc A:").grid(row=2, column=4, pady=(10, 0), sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.win_service_afastamentos_var,
            values=self.win_service_afastamentos_opcoes,
            state="readonly",
            width=26,
        ).grid(row=2, column=5, columnspan=3, pady=(10, 0), padx=(6, 14), sticky="w")
        ttk.Button(top, text="Status WinSvc", command=lambda: self._run_async(self._atualizar_status_windows_services)).grid(row=2, column=8, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Iniciar WinSvc", command=lambda: self._run_async(self._iniciar_windows_services)).grid(row=2, column=9, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Parar WinSvc", command=lambda: self._run_async(self._parar_windows_services)).grid(row=2, column=10, pady=(10, 0), padx=(0, 8))
        ttk.Button(top, text="Reiniciar WinSvc", command=lambda: self._run_async(self._reiniciar_windows_services)).grid(row=2, column=11, pady=(10, 0), padx=(0, 8))
        ttk.Label(top, textvariable=self.windows_services_status_var).grid(row=2, column=12, columnspan=3, pady=(10, 0), sticky="w")

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
        if self._closing:
            return

        def _append():
            self.output_text.insert(tk.END, f"{message}\n")
            self.output_text.see(tk.END)

        try:
            self.after(0, _append)
        except tk.TclError:
            pass

    def _set_status(self, text: str) -> None:
        if self._closing:
            return
        try:
            self.after(0, lambda: self.status_var.set(text))
        except tk.TclError:
            pass

    def _get_limit(self) -> int:
        try:
            value = int(self.limit_var.get().strip())
            return max(1, value)
        except Exception:
            return 1

    def _get_intervalo_motoristas(self) -> int:
        try:
            return max(1, int(self.intervalo_motoristas_var.get().strip()))
        except Exception:
            return max(1, int(settings.motorista_sync_interval_seconds))

    def _get_intervalo_afastamentos(self) -> int:
        try:
            return max(1, int(self.intervalo_afastamentos_var.get().strip()))
        except Exception:
            return max(1, int(settings.afastamento_sync_interval_seconds))

    def _database_origem(self) -> str:
        return self.database_var.get().strip() or settings.source_database_dev

    def _database_destino(self) -> str:
        return self.database_destino_var.get().strip() or settings.target_database

    def _schema_origem(self) -> str:
        return settings.source_schema_for_database(self._database_origem())

    @staticmethod
    def _unique(values: list[str]) -> list[str]:
        items: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            if text not in seen:
                seen.add(text)
                items.append(text)
        return items or [""]

    @staticmethod
    def _closest_option(value: str, options: list[str]) -> str:
        text = str(value or "").strip()
        if text in options:
            return text
        return options[0] if options else text

    def _ambiente_por_database(self, database: str) -> str:
        db = (database or "").strip().lower()
        if db == settings.source_database_prod.lower():
            return "Producao"
        return "Homologacao"

    def _database_por_ambiente(self, ambiente: str) -> str:
        if (ambiente or "").strip().lower() == "producao":
            return settings.source_database_prod
        return settings.source_database_dev

    def _nomes_servicos_windows_por_ambiente(self, ambiente: str) -> tuple[str, str]:
        if (ambiente or "").strip().lower() == "producao":
            return (
                settings.win_service_motoristas_prod,
                settings.win_service_afastamentos_prod,
            )
        return (
            settings.win_service_motoristas_dev,
            settings.win_service_afastamentos_dev,
        )

    def _aplicar_ambiente(self) -> None:
        if self._servicos_ativos():
            self._log("Pare os servicos continuos antes de trocar o ambiente.")
            return
        ambiente = self.ambiente_var.get()
        db_origem = self._database_por_ambiente(self.ambiente_var.get())
        svc_m, svc_a = self._nomes_servicos_windows_por_ambiente(ambiente)
        self.database_var.set(db_origem)
        if svc_m in self.win_service_motoristas_opcoes:
            self.win_service_motoristas_var.set(svc_m)
        if svc_a in self.win_service_afastamentos_opcoes:
            self.win_service_afastamentos_var.set(svc_a)
        if self.engine_origem is not None:
            try:
                self.engine_origem.dispose()
            except Exception:
                pass
        self.engine_origem = None
        self.database_origem_atual = None
        self._set_status(f"Status: ambiente aplicado ({ambiente})")
        self._log(f"Ambiente aplicado. Origem={db_origem}, WinSvc M={svc_m}, WinSvc A={svc_a}.")

    def _ensure_engine_origem(self):
        database = self._database_origem()
        if self.engine_origem is None or self.database_origem_atual != database:
            self.engine_origem = ativar_engine(database)
            self.database_origem_atual = database
        self.ambiente_var.set(self._ambiente_por_database(database))

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
        if self._motoristas_ativo():
            self._log("Servico continuo de motoristas esta ativo. Pare o servico para executar manualmente.")
            return
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
        if self._afastamentos_ativo():
            self._log("Servico continuo de afastamentos esta ativo. Pare o servico para executar manualmente.")
            return
        self._set_status("Status: executando afastamentos...")
        self._ensure_engine_origem()
        self._ensure_engine_destino()

        schema_origem = self._schema_origem()
        batch_size = self._get_limit()

        service = AfastamentoSyncService(
            engine_origem=self.engine_origem,
            engine_destino=self.engine_destino,
            database_origem=self._database_origem(),
            schema_origem=schema_origem,
            schema_destino=settings.target_schema,
            tabela_destino=settings.target_afastamento_table,
            batch_size=batch_size,
            data_inicio=settings.afastamento_sync_data_inicio,
        )
        resultado = service.executar_ciclo()

        self._log(f"[Afastamentos] Origem: {self._database_origem()} ({schema_origem})")
        self._log(f"[Afastamentos] Destino: {self._database_destino()}.{settings.target_schema}.{settings.target_afastamento_table}")
        self._log(f"[Afastamentos] Data inicio: {service.data_inicio.isoformat()}")
        self._log(f"[Afastamentos] Registros origem: {resultado.registros_origem}")
        self._log(f"[Afastamentos] Payloads validos: {resultado.payloads_validos}")
        self._log(f"[Afastamentos] Eventos gerados: {resultado.eventos_gerados}")
        self._log(f"[Afastamentos] Eventos inseridos: {resultado.eventos_inseridos}")
        self._log(f"[Afastamentos] Cursor reiniciado: {resultado.cursor_reiniciado}")
        self._set_status("Status: afastamentos finalizado")

    def _executar_ambos(self):
        self._executar_motoristas()
        self._executar_afastamentos()

    def _motoristas_ativo(self) -> bool:
        return self.thread_servico_motoristas is not None and self.thread_servico_motoristas.is_alive()

    def _afastamentos_ativo(self) -> bool:
        return self.thread_servico_afastamentos is not None and self.thread_servico_afastamentos.is_alive()

    def _servicos_ativos(self) -> bool:
        return self._motoristas_ativo() or self._afastamentos_ativo()

    def _atualizar_status_servicos(self) -> None:
        m_status = "ON" if self._motoristas_ativo() else "OFF"
        a_status = "ON" if self._afastamentos_ativo() else "OFF"
        if self._closing:
            return
        try:
            self.after(0, lambda: self.servicos_status_var.set(f"Servicos: motoristas={m_status} afastamentos={a_status}"))
        except tk.TclError:
            pass

    def _iniciar_servicos(self) -> None:
        self._iniciar_servico_motoristas()
        self._iniciar_servico_afastamentos()

    def _parar_servicos(self) -> None:
        self._parar_servico_motoristas()
        self._parar_servico_afastamentos()

    def _iniciar_servico_motoristas(self) -> None:
        if self._motoristas_ativo():
            self._log("Servico de motoristas ja esta em execucao.")
            return
        status_win, _ = self._status_windows_service(self._nome_win_svc_motoristas())
        if status_win == "RUNNING":
            self._log("WinSvc de motoristas esta RUNNING. Nao iniciarei servico local em paralelo.")
            return

        self._ensure_engine_origem()
        self._ensure_engine_destino()

        service = MotoristaSyncService(
            engine_origem=self.engine_origem,
            engine_destino=self.engine_destino,
            database_origem=self._database_origem(),
            schema_origem=self._schema_origem(),
            schema_destino=settings.target_schema,
            tabela_destino=settings.target_motorista_table,
            batch_size=self._get_limit(),
        )
        intervalo = self._get_intervalo_motoristas()
        stop_event = threading.Event()
        self.stop_servico_motoristas = stop_event

        def _run():
            self._log(
                f"[Servico Motoristas] Iniciado. origem={self._database_origem()} intervalo={intervalo}s lote={self._get_limit()}"
            )
            try:
                service.executar_continuo(
                    intervalo_segundos=intervalo,
                    logger=lambda m: self._log(f"[Servico Motoristas] {m}"),
                    stop_event=stop_event,
                )
            except Exception as exc:
                self._log(f"[Servico Motoristas] ERRO: {exc}")
            finally:
                self.thread_servico_motoristas = None
                self.stop_servico_motoristas = None
                self._log("[Servico Motoristas] Encerrado.")
                self._atualizar_status_servicos()

        self.thread_servico_motoristas = threading.Thread(target=_run, daemon=True)
        self.thread_servico_motoristas.start()
        self._set_status("Status: servico de motoristas ativo")
        self._atualizar_status_servicos()

    def _parar_servico_motoristas(self) -> None:
        if not self._motoristas_ativo():
            self._log("Servico de motoristas ja esta parado.")
            self._atualizar_status_servicos()
            return
        if self.stop_servico_motoristas is not None:
            self.stop_servico_motoristas.set()
        self._set_status("Status: parando servico de motoristas...")
        self._log("[Servico Motoristas] Sinal de parada enviado.")

    def _iniciar_servico_afastamentos(self) -> None:
        if self._afastamentos_ativo():
            self._log("Servico de afastamentos ja esta em execucao.")
            return
        status_win, _ = self._status_windows_service(self._nome_win_svc_afastamentos())
        if status_win == "RUNNING":
            self._log("WinSvc de afastamentos esta RUNNING. Nao iniciarei servico local em paralelo.")
            return

        self._ensure_engine_origem()
        self._ensure_engine_destino()

        service = AfastamentoSyncService(
            engine_origem=self.engine_origem,
            engine_destino=self.engine_destino,
            database_origem=self._database_origem(),
            schema_origem=self._schema_origem(),
            schema_destino=settings.target_schema,
            tabela_destino=settings.target_afastamento_table,
            batch_size=self._get_limit(),
            data_inicio=settings.afastamento_sync_data_inicio,
        )
        intervalo = self._get_intervalo_afastamentos()
        stop_event = threading.Event()
        self.stop_servico_afastamentos = stop_event

        def _run():
            self._log(
                f"[Servico Afastamentos] Iniciado. origem={self._database_origem()} intervalo={intervalo}s lote={self._get_limit()}"
            )
            try:
                service.executar_continuo(
                    intervalo_segundos=intervalo,
                    logger=lambda m: self._log(f"[Servico Afastamentos] {m}"),
                    stop_event=stop_event,
                )
            except Exception as exc:
                self._log(f"[Servico Afastamentos] ERRO: {exc}")
            finally:
                self.thread_servico_afastamentos = None
                self.stop_servico_afastamentos = None
                self._log("[Servico Afastamentos] Encerrado.")
                self._atualizar_status_servicos()

        self.thread_servico_afastamentos = threading.Thread(target=_run, daemon=True)
        self.thread_servico_afastamentos.start()
        self._set_status("Status: servico de afastamentos ativo")
        self._atualizar_status_servicos()

    def _parar_servico_afastamentos(self) -> None:
        if not self._afastamentos_ativo():
            self._log("Servico de afastamentos ja esta parado.")
            self._atualizar_status_servicos()
            return
        if self.stop_servico_afastamentos is not None:
            self.stop_servico_afastamentos.set()
        self._set_status("Status: parando servico de afastamentos...")
        self._log("[Servico Afastamentos] Sinal de parada enviado.")

    def _nome_win_svc_motoristas(self) -> str:
        return (self.win_service_motoristas_var.get() or "").strip()

    def _nome_win_svc_afastamentos(self) -> str:
        return (self.win_service_afastamentos_var.get() or "").strip()

    @staticmethod
    def _run_cmd(args: list[str]) -> tuple[int, str]:
        proc = subprocess.run(args, capture_output=True, text=True)
        out = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        return int(proc.returncode), out

    def _status_windows_service(self, service_name: str) -> tuple[str, str]:
        nome = (service_name or "").strip()
        if not nome:
            return "NOME_VAZIO", ""

        code, output = self._run_cmd(["sc", "query", nome])
        upper = output.upper()

        if "RUNNING" in upper:
            return "RUNNING", output
        if "STOPPED" in upper:
            return "STOPPED", output
        if "FAILED 1060" in upper or "DOES NOT EXIST" in upper:
            return "NAO_INSTALADO", output
        if code != 0:
            return "ERRO", output
        return "DESCONHECIDO", output

    def _aguardar_windows_service(self, service_name: str, esperado: str, timeout_segundos: int = 30) -> bool:
        limite = time.time() + max(1, timeout_segundos)
        while time.time() < limite:
            status, _ = self._status_windows_service(service_name)
            if status == esperado:
                return True
            time.sleep(1)
        return False

    def _acao_windows_service(self, service_name: str, action: str) -> bool:
        nome = (service_name or "").strip()
        if not nome:
            self._log(f"[WinSvc] Nome de servico vazio para acao {action}.")
            return False
        code, output = self._run_cmd(["sc", action, nome])
        if output:
            self._log(f"[WinSvc {nome}] {output}")
            upper = output.upper()
            if "FAILED 5" in upper or "ACCESS IS DENIED" in upper:
                self._log("[WinSvc] Permissao negada. Execute a interface como Administrador para controlar servicos Windows.")
        return code == 0

    def _atualizar_status_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        status_m, _ = self._status_windows_service(nome_m)
        status_a, _ = self._status_windows_service(nome_a)
        texto = f"Windows: motoristas={status_m} afastamentos={status_a}"
        if not self._closing:
            try:
                self.after(0, lambda: self.windows_services_status_var.set(texto))
            except tk.TclError:
                pass
        self._log(f"[WinSvc] Status - M({nome_m})={status_m} | A({nome_a})={status_a}")

    def _iniciar_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        self._acao_windows_service(nome_m, "start")
        self._acao_windows_service(nome_a, "start")
        time.sleep(1)
        self._atualizar_status_windows_services()

    def _parar_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        self._acao_windows_service(nome_m, "stop")
        self._acao_windows_service(nome_a, "stop")
        time.sleep(1)
        self._atualizar_status_windows_services()

    def _reiniciar_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()

        for nome in (nome_m, nome_a):
            if not nome:
                continue
            self._acao_windows_service(nome, "stop")
            self._aguardar_windows_service(nome, "STOPPED", timeout_segundos=20)
            self._acao_windows_service(nome, "start")

        time.sleep(1)
        self._atualizar_status_windows_services()

    def _ao_fechar(self) -> None:
        self._closing = True
        self._parar_servicos()
        self.after(150, self.destroy)


def iniciar_interface() -> None:
    app = IntegracaoApp()
    app.mainloop()
