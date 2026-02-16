
import re
import subprocess
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from config.settings import settings
from src.integradora.afastamento_sync_service import AfastamentoSyncService
from src.integradora.api_dispatch_service import ApiDispatchService
from src.integradora.motorista_sync_service import MotoristaSyncService


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class IntegracaoApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Integracao ATS")
        self.geometry("1280x820")
        self.minsize(1080, 700)
        self.protocol("WM_DELETE_WINDOW", self._ao_fechar)

        self.engine_origem: Engine | None = None
        self.engine_destino: Engine | None = None
        self.database_origem_atual: str | None = None
        self.database_destino_atual: str | None = None
        self.token: str | None = None

        self.thread_servico_motoristas: threading.Thread | None = None
        self.thread_servico_afastamentos: threading.Thread | None = None
        self.stop_servico_motoristas: threading.Event | None = None
        self.stop_servico_afastamentos: threading.Event | None = None

        self._cache_colunas: dict[str, dict[str, str]] = {}
        self._monitor_job: str | None = None
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
        self.win_service_api_motoristas_opcoes = self._unique(
            [settings.win_service_api_motoristas_dev, settings.win_service_api_motoristas_prod]
        )
        self.win_service_api_afastamentos_opcoes = self._unique(
            [settings.win_service_api_afastamentos_dev, settings.win_service_api_afastamentos_prod]
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
        self.intervalo_api_var = tk.StringVar(
            value=self._closest_option(str(settings.api_sync_interval_seconds), self.intervalo_opcoes)
        )
        self.batch_api_motoristas_var = tk.StringVar(
            value=self._closest_option(str(settings.api_sync_batch_size_motoristas), self.lote_opcoes)
        )
        self.batch_api_afastamentos_var = tk.StringVar(
            value=self._closest_option(str(settings.api_sync_batch_size_afastamentos), self.lote_opcoes)
        )

        sync_m, sync_a = self._nomes_servicos_windows_sync_por_ambiente(self.ambiente_var.get())
        api_m, api_a = self._nomes_servicos_windows_api_por_ambiente(self.ambiente_var.get())

        self.win_service_motoristas_var = tk.StringVar(value=sync_m)
        self.win_service_afastamentos_var = tk.StringVar(value=sync_a)
        self.win_service_api_motoristas_var = tk.StringVar(value=api_m)
        self.win_service_api_afastamentos_var = tk.StringVar(value=api_a)

        self.status_var = tk.StringVar(value="Status: pronto")
        self.servicos_status_var = tk.StringVar(value="Servicos locais: motoristas=OFF afastamentos=OFF")
        self.windows_services_status_var = tk.StringVar(value="Windows Sync: motoristas=? afastamentos=?")
        self.windows_api_services_status_var = tk.StringVar(value="Windows API: motoristas=? afastamentos=?")

        self.monitor_api_motoristas_var = tk.StringVar(value="Motoristas: aguardando atualizacao")
        self.monitor_api_afastamentos_var = tk.StringVar(value="Afastamentos: aguardando atualizacao")

        self.lista_tipo_var = tk.StringVar(value="Ambos")
        self.lista_status_var = tk.StringVar(value="Todos")
        self.lista_limite_var = tk.StringVar(value="100")
        self.lista_eventos_cache: list[dict[str, Any]] = []

        self._build_ui()
        self.after(300, lambda: self._run_async(self._atualizacao_inicial, channel="api"))
        self.after(1000, self._agendar_monitoramento_periodico)

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        top = ttk.Frame(self, padding=12)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(99, weight=1)

        ttk.Label(top, text="Ambiente:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.ambiente_var,
            values=("Homologacao", "Producao"),
            state="readonly",
            width=14,
        ).grid(row=0, column=1, padx=(6, 8), sticky="w")
        ttk.Button(top, text="Aplicar", command=self._aplicar_ambiente).grid(row=0, column=2, padx=(0, 14), sticky="w")

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

        ttk.Label(top, text="Lote sync:").grid(row=0, column=7, sticky="w")
        ttk.Combobox(
            top,
            textvariable=self.limit_var,
            values=self.lote_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=8, padx=(6, 14), sticky="w")

        ttk.Button(top, text="Login API", command=lambda: self._run_async(self._login, channel="api")).grid(
            row=0,
            column=9,
            padx=(0, 8),
        )
        ttk.Button(top, text="Atualizar monitor", command=lambda: self._run_async(self._atualizar_monitor_api, channel="api")).grid(
            row=0,
            column=10,
            padx=(0, 8),
        )

        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 8))

        self.tab_sync = ttk.Frame(self.notebook, padding=10)
        self.tab_api = ttk.Frame(self.notebook, padding=10)
        self.tab_lista = ttk.Frame(self.notebook, padding=10)

        self.notebook.add(self.tab_sync, text="Sincronizacao")
        self.notebook.add(self.tab_api, text="Monitor API")
        self.notebook.add(self.tab_lista, text="Lista Integracao")

        self._build_tab_sync()
        self._build_tab_api()
        self._build_tab_lista()

        ttk.Label(self, textvariable=self.status_var, padding=(12, 0, 12, 10)).grid(row=2, column=0, sticky="w")
    def _build_tab_sync(self) -> None:
        self.tab_sync.columnconfigure(0, weight=1)
        self.tab_sync.rowconfigure(3, weight=1)

        manual = ttk.LabelFrame(self.tab_sync, text="Execucao Manual", padding=10)
        manual.grid(row=0, column=0, sticky="ew")

        ttk.Button(manual, text="Motoristas", command=lambda: self._run_async(self._executar_motoristas)).grid(
            row=0,
            column=0,
            padx=(0, 8),
            pady=(0, 4),
        )
        ttk.Button(manual, text="Afastamentos", command=lambda: self._run_async(self._executar_afastamentos)).grid(
            row=0,
            column=1,
            padx=(0, 8),
            pady=(0, 4),
        )
        ttk.Button(manual, text="Executar ambos", command=lambda: self._run_async(self._executar_ambos)).grid(
            row=0,
            column=2,
            padx=(0, 8),
            pady=(0, 4),
        )

        continuo = ttk.LabelFrame(self.tab_sync, text="Servico Local Continuo", padding=10)
        continuo.grid(row=1, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(continuo, text="Int. M(s):").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            continuo,
            textvariable=self.intervalo_motoristas_var,
            values=self.intervalo_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(continuo, text="Int. A(s):").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            continuo,
            textvariable=self.intervalo_afastamentos_var,
            values=self.intervalo_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Button(
            continuo,
            text="Iniciar M",
            command=lambda: self._run_async(self._iniciar_servico_motoristas),
        ).grid(row=0, column=4, padx=(0, 8), sticky="w")
        ttk.Button(continuo, text="Parar M", command=self._parar_servico_motoristas).grid(row=0, column=5, padx=(0, 8), sticky="w")
        ttk.Button(
            continuo,
            text="Iniciar A",
            command=lambda: self._run_async(self._iniciar_servico_afastamentos),
        ).grid(row=0, column=6, padx=(0, 8), sticky="w")
        ttk.Button(continuo, text="Parar A", command=self._parar_servico_afastamentos).grid(row=0, column=7, padx=(0, 8), sticky="w")
        ttk.Button(continuo, text="Iniciar ambos", command=lambda: self._run_async(self._iniciar_servicos)).grid(
            row=0,
            column=8,
            padx=(0, 8),
            sticky="w",
        )
        ttk.Button(continuo, text="Parar ambos", command=self._parar_servicos).grid(row=0, column=9, padx=(0, 8), sticky="w")
        ttk.Label(continuo, textvariable=self.servicos_status_var).grid(row=0, column=10, padx=(8, 0), sticky="w")

        win = ttk.LabelFrame(self.tab_sync, text="Servicos Windows (Sync)", padding=10)
        win.grid(row=2, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(win, text="WinSvc M:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            win,
            textvariable=self.win_service_motoristas_var,
            values=self.win_service_motoristas_opcoes,
            state="readonly",
            width=28,
        ).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(win, text="WinSvc A:").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            win,
            textvariable=self.win_service_afastamentos_var,
            values=self.win_service_afastamentos_opcoes,
            state="readonly",
            width=28,
        ).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Button(win, text="Status", command=lambda: self._run_async(self._atualizar_status_windows_services)).grid(
            row=0,
            column=4,
            padx=(0, 8),
            sticky="w",
        )
        ttk.Button(win, text="Iniciar", command=lambda: self._run_async(self._iniciar_windows_services)).grid(
            row=0,
            column=5,
            padx=(0, 8),
            sticky="w",
        )
        ttk.Button(win, text="Parar", command=lambda: self._run_async(self._parar_windows_services)).grid(
            row=0,
            column=6,
            padx=(0, 8),
            sticky="w",
        )
        ttk.Button(win, text="Reiniciar", command=lambda: self._run_async(self._reiniciar_windows_services)).grid(
            row=0,
            column=7,
            padx=(0, 8),
            sticky="w",
        )
        ttk.Label(win, textvariable=self.windows_services_status_var).grid(row=0, column=8, padx=(8, 0), sticky="w")

        output = ttk.LabelFrame(self.tab_sync, text="Log de sincronizacao", padding=10)
        output.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        output.columnconfigure(0, weight=1)
        output.rowconfigure(0, weight=1)

        self.output_text_sync = ScrolledText(output, wrap=tk.WORD, font=("Consolas", 10))
        self.output_text_sync.grid(row=0, column=0, sticky="nsew")

        ttk.Button(output, text="Limpar log", command=lambda: self._clear_text(self.output_text_sync)).grid(
            row=1,
            column=0,
            sticky="w",
            pady=(8, 0),
        )

    def _build_tab_api(self) -> None:
        self.tab_api.columnconfigure(0, weight=1)
        self.tab_api.rowconfigure(3, weight=1)

        topo = ttk.LabelFrame(self.tab_api, text="Controle de envio API", padding=10)
        topo.grid(row=0, column=0, sticky="ew")

        ttk.Label(topo, text="Batch M:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            topo,
            textvariable=self.batch_api_motoristas_var,
            values=self.lote_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(topo, text="Batch A:").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            topo,
            textvariable=self.batch_api_afastamentos_var,
            values=self.lote_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Label(topo, text="Int. API(s):").grid(row=0, column=4, sticky="w")
        ttk.Combobox(
            topo,
            textvariable=self.intervalo_api_var,
            values=self.intervalo_opcoes,
            state="readonly",
            width=6,
        ).grid(row=0, column=5, padx=(6, 14), sticky="w")

        ttk.Button(
            topo,
            text="Executar API M",
            command=lambda: self._run_async(self._executar_api_motoristas, channel="api"),
        ).grid(row=0, column=6, padx=(0, 8), sticky="w")
        ttk.Button(
            topo,
            text="Executar API A",
            command=lambda: self._run_async(self._executar_api_afastamentos, channel="api"),
        ).grid(row=0, column=7, padx=(0, 8), sticky="w")
        ttk.Button(
            topo,
            text="Executar API ambos",
            command=lambda: self._run_async(self._executar_api_ambos, channel="api"),
        ).grid(row=0, column=8, padx=(0, 8), sticky="w")
        ttk.Button(
            topo,
            text="Atualizar indicadores",
            command=lambda: self._run_async(self._atualizar_monitor_api, channel="api"),
        ).grid(row=0, column=9, padx=(0, 8), sticky="w")

        win = ttk.LabelFrame(self.tab_api, text="Servicos Windows (API)", padding=10)
        win.grid(row=1, column=0, sticky="ew", pady=(8, 0))

        ttk.Label(win, text="WinSvc API M:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            win,
            textvariable=self.win_service_api_motoristas_var,
            values=self.win_service_api_motoristas_opcoes,
            state="readonly",
            width=28,
        ).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(win, text="WinSvc API A:").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            win,
            textvariable=self.win_service_api_afastamentos_var,
            values=self.win_service_api_afastamentos_opcoes,
            state="readonly",
            width=28,
        ).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Button(
            win,
            text="Status",
            command=lambda: self._run_async(self._atualizar_status_windows_services_api, channel="api"),
        ).grid(row=0, column=4, padx=(0, 8), sticky="w")
        ttk.Button(
            win,
            text="Iniciar",
            command=lambda: self._run_async(self._iniciar_windows_services_api, channel="api"),
        ).grid(row=0, column=5, padx=(0, 8), sticky="w")
        ttk.Button(
            win,
            text="Parar",
            command=lambda: self._run_async(self._parar_windows_services_api, channel="api"),
        ).grid(row=0, column=6, padx=(0, 8), sticky="w")
        ttk.Button(
            win,
            text="Reiniciar",
            command=lambda: self._run_async(self._reiniciar_windows_services_api, channel="api"),
        ).grid(row=0, column=7, padx=(0, 8), sticky="w")
        ttk.Label(win, textvariable=self.windows_api_services_status_var).grid(row=0, column=8, padx=(8, 0), sticky="w")

        resumo = ttk.Frame(self.tab_api)
        resumo.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        resumo.columnconfigure(0, weight=1)
        resumo.columnconfigure(1, weight=1)

        card_m = ttk.LabelFrame(resumo, text="Fila Motoristas", padding=10)
        card_m.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Label(card_m, textvariable=self.monitor_api_motoristas_var, justify=tk.LEFT).grid(row=0, column=0, sticky="w")

        card_a = ttk.LabelFrame(resumo, text="Fila Afastamentos", padding=10)
        card_a.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Label(card_a, textvariable=self.monitor_api_afastamentos_var, justify=tk.LEFT).grid(row=0, column=0, sticky="w")

        output = ttk.LabelFrame(self.tab_api, text="Log de envio API", padding=10)
        output.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        output.columnconfigure(0, weight=1)
        output.rowconfigure(0, weight=1)

        self.output_text_api = ScrolledText(output, wrap=tk.WORD, font=("Consolas", 10))
        self.output_text_api.grid(row=0, column=0, sticky="nsew")

        ttk.Button(output, text="Limpar log", command=lambda: self._clear_text(self.output_text_api)).grid(
            row=1,
            column=0,
            sticky="w",
            pady=(8, 0),
        )
    def _build_tab_lista(self) -> None:
        self.tab_lista.columnconfigure(0, weight=1)
        self.tab_lista.rowconfigure(1, weight=1)

        filtros = ttk.LabelFrame(self.tab_lista, text="Filtros", padding=10)
        filtros.grid(row=0, column=0, sticky="ew")

        ttk.Label(filtros, text="Tipo:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            filtros,
            textvariable=self.lista_tipo_var,
            values=("Ambos", "Motoristas", "Afastamentos"),
            state="readonly",
            width=14,
        ).grid(row=0, column=1, padx=(6, 14), sticky="w")

        ttk.Label(filtros, text="Status:").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            filtros,
            textvariable=self.lista_status_var,
            values=("Todos", "PENDENTE", "PROCESSANDO", "PROCESSADO", "ERRO"),
            state="readonly",
            width=14,
        ).grid(row=0, column=3, padx=(6, 14), sticky="w")

        ttk.Label(filtros, text="Limite:").grid(row=0, column=4, sticky="w")
        ttk.Combobox(
            filtros,
            textvariable=self.lista_limite_var,
            values=("50", "100", "200", "500", "1000"),
            state="readonly",
            width=8,
        ).grid(row=0, column=5, padx=(6, 14), sticky="w")

        ttk.Button(
            filtros,
            text="Atualizar lista",
            command=lambda: self._run_async(self._atualizar_lista_integracao, channel="api"),
        ).grid(row=0, column=6, padx=(0, 8), sticky="w")

        tabela_frame = ttk.LabelFrame(self.tab_lista, text="Eventos de integracao", padding=10)
        tabela_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        tabela_frame.columnconfigure(0, weight=1)
        tabela_frame.rowconfigure(0, weight=1)

        cols = (
            "tabela",
            "chave",
            "status",
            "tentativas",
            "evento",
            "criado_em",
            "atualizado_em",
            "proxima_tentativa",
            "processado_em",
            "http_status",
            "erro",
        )
        self.lista_tree = ttk.Treeview(tabela_frame, columns=cols, show="headings", height=15)
        self.lista_tree.grid(row=0, column=0, sticky="nsew")

        scroll_y = ttk.Scrollbar(tabela_frame, orient="vertical", command=self.lista_tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        self.lista_tree.configure(yscrollcommand=scroll_y.set)

        headers = {
            "tabela": "Tabela",
            "chave": "Chave",
            "status": "Status",
            "tentativas": "Tent.",
            "evento": "Evento",
            "criado_em": "Criado em",
            "atualizado_em": "Atualizado em",
            "proxima_tentativa": "Prox. tentativa",
            "processado_em": "Processado em",
            "http_status": "HTTP",
            "erro": "Ultimo erro",
        }
        widths = {
            "tabela": 110,
            "chave": 270,
            "status": 110,
            "tentativas": 70,
            "evento": 150,
            "criado_em": 140,
            "atualizado_em": 140,
            "proxima_tentativa": 140,
            "processado_em": 140,
            "http_status": 70,
            "erro": 340,
        }

        for col in cols:
            self.lista_tree.heading(col, text=headers[col])
            self.lista_tree.column(col, width=widths[col], anchor="w")

        self.lista_tree.bind("<<TreeviewSelect>>", self._on_lista_item_select)

        detalhes = ttk.LabelFrame(self.tab_lista, text="Detalhes do item", padding=10)
        detalhes.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        detalhes.columnconfigure(0, weight=1)
        detalhes.rowconfigure(0, weight=1)

        self.lista_detalhes_text = ScrolledText(detalhes, wrap=tk.WORD, font=("Consolas", 10), height=9)
        self.lista_detalhes_text.grid(row=0, column=0, sticky="nsew")

    def _run_async(self, target, *, channel: str = "sync") -> None:
        thread = threading.Thread(target=self._safe_call, args=(target, channel), daemon=True)
        thread.start()

    def _safe_call(self, target, channel: str) -> None:
        try:
            target()
        except Exception as exc:
            self._set_status(f"Status: erro - {exc}")
            if channel == "api":
                self._log_api(f"ERRO: {exc}")
            else:
                self._log_sync(f"ERRO: {exc}")

    def _clear_text(self, widget: ScrolledText) -> None:
        widget.delete("1.0", tk.END)

    def _append_log(self, widget: ScrolledText, message: str) -> None:
        if self._closing:
            return

        def _append() -> None:
            widget.insert(tk.END, f"{message}\n")
            widget.see(tk.END)

        try:
            self.after(0, _append)
        except tk.TclError:
            pass

    def _log_sync(self, message: str) -> None:
        self._append_log(self.output_text_sync, message)

    def _log_api(self, message: str) -> None:
        self._append_log(self.output_text_api, message)

    def _set_status(self, text_value: str) -> None:
        if self._closing:
            return
        try:
            self.after(0, lambda: self.status_var.set(text_value))
        except tk.TclError:
            pass

    def _get_limit(self) -> int:
        try:
            return max(1, int(self.limit_var.get().strip()))
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

    def _get_batch_api_motoristas(self) -> int:
        try:
            return max(1, int(self.batch_api_motoristas_var.get().strip()))
        except Exception:
            return max(1, int(settings.api_sync_batch_size_motoristas))

    def _get_batch_api_afastamentos(self) -> int:
        try:
            return max(1, int(self.batch_api_afastamentos_var.get().strip()))
        except Exception:
            return max(1, int(settings.api_sync_batch_size_afastamentos))

    def _get_intervalo_api(self) -> int:
        try:
            return max(1, int(self.intervalo_api_var.get().strip()))
        except Exception:
            return max(1, int(settings.api_sync_interval_seconds))

    def _get_lista_limite(self) -> int:
        try:
            return max(1, int(self.lista_limite_var.get().strip()))
        except Exception:
            return 100

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
            text_value = str(value or "").strip()
            if not text_value:
                continue
            if text_value not in seen:
                seen.add(text_value)
                items.append(text_value)
        return items or [""]

    @staticmethod
    def _closest_option(value: str, options: list[str]) -> str:
        text_value = str(value or "").strip()
        if text_value in options:
            return text_value
        return options[0] if options else text_value

    def _ambiente_por_database(self, database: str) -> str:
        db = (database or "").strip().lower()
        if db == settings.source_database_prod.lower():
            return "Producao"
        return "Homologacao"

    def _database_por_ambiente(self, ambiente: str) -> str:
        if (ambiente or "").strip().lower() == "producao":
            return settings.source_database_prod
        return settings.source_database_dev

    def _nomes_servicos_windows_sync_por_ambiente(self, ambiente: str) -> tuple[str, str]:
        if (ambiente or "").strip().lower() == "producao":
            return (settings.win_service_motoristas_prod, settings.win_service_afastamentos_prod)
        return (settings.win_service_motoristas_dev, settings.win_service_afastamentos_dev)

    def _nomes_servicos_windows_api_por_ambiente(self, ambiente: str) -> tuple[str, str]:
        if (ambiente or "").strip().lower() == "producao":
            return (settings.win_service_api_motoristas_prod, settings.win_service_api_afastamentos_prod)
        return (settings.win_service_api_motoristas_dev, settings.win_service_api_afastamentos_dev)

    def _aplicar_ambiente(self) -> None:
        if self._servicos_ativos():
            self._log_sync("Pare os servicos locais antes de trocar o ambiente.")
            return

        ambiente = self.ambiente_var.get()
        db_origem = self._database_por_ambiente(ambiente)
        sync_m, sync_a = self._nomes_servicos_windows_sync_por_ambiente(ambiente)
        api_m, api_a = self._nomes_servicos_windows_api_por_ambiente(ambiente)

        self.database_var.set(db_origem)
        self.win_service_motoristas_var.set(sync_m)
        self.win_service_afastamentos_var.set(sync_a)
        self.win_service_api_motoristas_var.set(api_m)
        self.win_service_api_afastamentos_var.set(api_a)

        if self.engine_origem is not None:
            try:
                self.engine_origem.dispose()
            except Exception:
                pass
        if self.engine_destino is not None:
            try:
                self.engine_destino.dispose()
            except Exception:
                pass

        self.engine_origem = None
        self.engine_destino = None
        self.database_origem_atual = None
        self.database_destino_atual = None
        self._cache_colunas.clear()

        self._set_status(f"Status: ambiente aplicado ({ambiente})")
        self._log_sync(f"Ambiente aplicado. Origem={db_origem}, WinSvc Sync M={sync_m}, A={sync_a}.")
        self._log_api(f"Ambiente aplicado. WinSvc API M={api_m}, A={api_a}.")
        self._run_async(self._atualizar_monitor_api, channel="api")

    def _ensure_engine_origem(self) -> None:
        database = self._database_origem()
        if self.engine_origem is None or self.database_origem_atual != database:
            self.engine_origem = ativar_engine(database)
            self.database_origem_atual = database
        self.ambiente_var.set(self._ambiente_por_database(database))

    def _ensure_engine_destino(self) -> None:
        database = self._database_destino()
        if self.engine_destino is None or self.database_destino_atual != database:
            self.engine_destino = ativar_engine(database)
            self.database_destino_atual = database
            self._cache_colunas.clear()

    def _login(self) -> None:
        self._set_status("Status: autenticando API...")
        auth = login_api()
        self.token = auth.get("token")
        exp = auth.get("exp")
        self._set_status("Status: autenticado")
        self._log_api(f"Login API ok. Expira em: {exp}")

    def _executar_motoristas(self) -> None:
        if self._motoristas_ativo():
            self._log_sync("Servico continuo de motoristas ativo. Pare o servico para executar manualmente.")
            return

        self._set_status("Status: executando sincronizacao de motoristas...")
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

        self._log_sync(f"[Motoristas] Origem: {self._database_origem()} ({schema_origem})")
        self._log_sync(
            f"[Motoristas] Destino: {self._database_destino()}.{settings.target_schema}.{settings.target_motorista_table}"
        )
        self._log_sync(f"[Motoristas] Alterados R034FUN: {resultado.alterados_fun}")
        self._log_sync(f"[Motoristas] Alterados R034CPL: {resultado.alterados_cpl}")
        self._log_sync(f"[Motoristas] NumCad processados: {resultado.numcads_processados}")
        self._log_sync(f"[Motoristas] Registros origem: {resultado.registros_origem}")
        self._log_sync(f"[Motoristas] Payloads validos: {resultado.payloads_validos}")
        self._log_sync(f"[Motoristas] Eventos gerados: {resultado.eventos_gerados}")
        self._log_sync(f"[Motoristas] Eventos inseridos: {resultado.eventos_inseridos}")
        self._set_status("Status: sincronizacao de motoristas finalizada")

    def _executar_afastamentos(self) -> None:
        if self._afastamentos_ativo():
            self._log_sync("Servico continuo de afastamentos ativo. Pare o servico para executar manualmente.")
            return

        self._set_status("Status: executando sincronizacao de afastamentos...")
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

        self._log_sync(f"[Afastamentos] Origem: {self._database_origem()} ({schema_origem})")
        self._log_sync(
            f"[Afastamentos] Destino: {self._database_destino()}.{settings.target_schema}.{settings.target_afastamento_table}"
        )
        self._log_sync(f"[Afastamentos] Data inicio: {service.data_inicio.isoformat()}")
        self._log_sync(f"[Afastamentos] Registros origem: {resultado.registros_origem}")
        self._log_sync(f"[Afastamentos] Payloads validos: {resultado.payloads_validos}")
        self._log_sync(f"[Afastamentos] Eventos gerados: {resultado.eventos_gerados}")
        self._log_sync(f"[Afastamentos] Eventos inseridos: {resultado.eventos_inseridos}")
        self._log_sync(f"[Afastamentos] Cursor reiniciado: {resultado.cursor_reiniciado}")
        self._set_status("Status: sincronizacao de afastamentos finalizada")

    def _executar_ambos(self) -> None:
        self._executar_motoristas()
        self._executar_afastamentos()

    def _executar_api_motoristas(self) -> None:
        self._executar_api(processar_motoristas=True, processar_afastamentos=False)

    def _executar_api_afastamentos(self) -> None:
        self._executar_api(processar_motoristas=False, processar_afastamentos=True)

    def _executar_api_ambos(self) -> None:
        self._executar_api(processar_motoristas=True, processar_afastamentos=True)
    def _executar_api(self, *, processar_motoristas: bool, processar_afastamentos: bool) -> None:
        self._set_status("Status: executando envio para API...")
        self._ensure_engine_destino()

        service = ApiDispatchService(
            engine_destino=self.engine_destino,
            schema_destino=settings.target_schema,
            tabela_motorista=settings.target_motorista_table,
            tabela_afastamento=settings.target_afastamento_table,
            endpoint_motorista=settings.api_motorista_endpoint,
            endpoint_afastamento=settings.api_afastamento_endpoint,
            batch_size_motoristas=self._get_batch_api_motoristas(),
            batch_size_afastamentos=self._get_batch_api_afastamentos(),
            max_tentativas=settings.api_sync_max_tentativas,
            lock_timeout_minutes=settings.api_sync_lock_timeout_minutes,
            retry_base_seconds=settings.api_sync_retry_base_seconds,
            retry_max_seconds=settings.api_sync_retry_max_seconds,
            api_timeout_seconds=settings.api_timeout_seconds,
            processar_motoristas=processar_motoristas,
            processar_afastamentos=processar_afastamentos,
        )

        try:
            resultado = service.executar_ciclo()
        finally:
            service.close()

        self._log_api(
            "[API] Ciclo concluido: "
            f"LockM={resultado.locks_liberados_motoristas} "
            f"LockA={resultado.locks_liberados_afastamentos} "
            f"CapM={resultado.motoristas_capturados} "
            f"OkM={resultado.motoristas_sucesso} "
            f"ErrM={resultado.motoristas_erro} "
            f"CapA={resultado.afastamentos_capturados} "
            f"OkA={resultado.afastamentos_sucesso} "
            f"ErrA={resultado.afastamentos_erro}"
        )
        self._set_status("Status: envio API finalizado")
        self._atualizar_monitor_api(log_line=False)
        self._atualizar_lista_integracao(log_line=False)

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
            self.after(0, lambda: self.servicos_status_var.set(f"Servicos locais: motoristas={m_status} afastamentos={a_status}"))
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
            self._log_sync("Servico de motoristas ja esta em execucao.")
            return

        status_win, _ = self._status_windows_service(self._nome_win_svc_motoristas())
        if status_win == "RUNNING":
            self._log_sync("WinSvc de motoristas esta RUNNING. Nao iniciarei servico local em paralelo.")
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

        def _run() -> None:
            self._log_sync(
                f"[Servico Motoristas] Iniciado. origem={self._database_origem()} intervalo={intervalo}s lote={self._get_limit()}"
            )
            try:
                service.executar_continuo(
                    intervalo_segundos=intervalo,
                    logger=lambda m: self._log_sync(f"[Servico Motoristas] {m}"),
                    stop_event=stop_event,
                )
            except Exception as exc:
                self._log_sync(f"[Servico Motoristas] ERRO: {exc}")
            finally:
                self.thread_servico_motoristas = None
                self.stop_servico_motoristas = None
                self._log_sync("[Servico Motoristas] Encerrado.")
                self._atualizar_status_servicos()

        self.thread_servico_motoristas = threading.Thread(target=_run, daemon=True)
        self.thread_servico_motoristas.start()
        self._set_status("Status: servico local de motoristas ativo")
        self._atualizar_status_servicos()

    def _parar_servico_motoristas(self) -> None:
        if not self._motoristas_ativo():
            self._log_sync("Servico de motoristas ja esta parado.")
            self._atualizar_status_servicos()
            return
        if self.stop_servico_motoristas is not None:
            self.stop_servico_motoristas.set()
        self._set_status("Status: parando servico local de motoristas...")
        self._log_sync("[Servico Motoristas] Sinal de parada enviado.")

    def _iniciar_servico_afastamentos(self) -> None:
        if self._afastamentos_ativo():
            self._log_sync("Servico de afastamentos ja esta em execucao.")
            return

        status_win, _ = self._status_windows_service(self._nome_win_svc_afastamentos())
        if status_win == "RUNNING":
            self._log_sync("WinSvc de afastamentos esta RUNNING. Nao iniciarei servico local em paralelo.")
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

        def _run() -> None:
            self._log_sync(
                f"[Servico Afastamentos] Iniciado. origem={self._database_origem()} intervalo={intervalo}s lote={self._get_limit()}"
            )
            try:
                service.executar_continuo(
                    intervalo_segundos=intervalo,
                    logger=lambda m: self._log_sync(f"[Servico Afastamentos] {m}"),
                    stop_event=stop_event,
                )
            except Exception as exc:
                self._log_sync(f"[Servico Afastamentos] ERRO: {exc}")
            finally:
                self.thread_servico_afastamentos = None
                self.stop_servico_afastamentos = None
                self._log_sync("[Servico Afastamentos] Encerrado.")
                self._atualizar_status_servicos()

        self.thread_servico_afastamentos = threading.Thread(target=_run, daemon=True)
        self.thread_servico_afastamentos.start()
        self._set_status("Status: servico local de afastamentos ativo")
        self._atualizar_status_servicos()

    def _parar_servico_afastamentos(self) -> None:
        if not self._afastamentos_ativo():
            self._log_sync("Servico de afastamentos ja esta parado.")
            self._atualizar_status_servicos()
            return
        if self.stop_servico_afastamentos is not None:
            self.stop_servico_afastamentos.set()
        self._set_status("Status: parando servico local de afastamentos...")
        self._log_sync("[Servico Afastamentos] Sinal de parada enviado.")
    def _nome_win_svc_motoristas(self) -> str:
        return (self.win_service_motoristas_var.get() or "").strip()

    def _nome_win_svc_afastamentos(self) -> str:
        return (self.win_service_afastamentos_var.get() or "").strip()

    def _nome_win_svc_api_motoristas(self) -> str:
        return (self.win_service_api_motoristas_var.get() or "").strip()

    def _nome_win_svc_api_afastamentos(self) -> str:
        return (self.win_service_api_afastamentos_var.get() or "").strip()

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
        if "PAUSED" in upper:
            return "PAUSED", output
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

    def _acao_windows_service(self, service_name: str, action: str, *, log_fn) -> bool:
        nome = (service_name or "").strip()
        if not nome:
            log_fn(f"[WinSvc] Nome de servico vazio para acao {action}.")
            return False

        code, output = self._run_cmd(["sc", action, nome])
        if output:
            log_fn(f"[WinSvc {nome}] {output}")
            upper = output.upper()
            if "FAILED 5" in upper or "ACCESS IS DENIED" in upper:
                log_fn("[WinSvc] Permissao negada. Execute a interface como Administrador.")
        return code == 0

    def _atualizar_status_windows_services(self, *, log_line: bool = True) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        status_m, _ = self._status_windows_service(nome_m)
        status_a, _ = self._status_windows_service(nome_a)
        texto = f"Windows Sync: motoristas={status_m} afastamentos={status_a}"

        if not self._closing:
            try:
                self.after(0, lambda: self.windows_services_status_var.set(texto))
            except tk.TclError:
                pass

        if log_line:
            self._log_sync(f"[WinSvc Sync] M({nome_m})={status_m} | A({nome_a})={status_a}")

    def _atualizar_status_windows_services_api(self, *, log_line: bool = True) -> None:
        nome_m = self._nome_win_svc_api_motoristas()
        nome_a = self._nome_win_svc_api_afastamentos()
        status_m, _ = self._status_windows_service(nome_m)
        status_a, _ = self._status_windows_service(nome_a)
        texto = f"Windows API: motoristas={status_m} afastamentos={status_a}"

        if not self._closing:
            try:
                self.after(0, lambda: self.windows_api_services_status_var.set(texto))
            except tk.TclError:
                pass

        if log_line:
            self._log_api(f"[WinSvc API] M({nome_m})={status_m} | A({nome_a})={status_a}")

    def _iniciar_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        self._acao_windows_service(nome_m, "start", log_fn=self._log_sync)
        self._acao_windows_service(nome_a, "start", log_fn=self._log_sync)
        time.sleep(1)
        self._atualizar_status_windows_services(log_line=True)

    def _parar_windows_services(self) -> None:
        nome_m = self._nome_win_svc_motoristas()
        nome_a = self._nome_win_svc_afastamentos()
        self._acao_windows_service(nome_m, "stop", log_fn=self._log_sync)
        self._acao_windows_service(nome_a, "stop", log_fn=self._log_sync)
        time.sleep(1)
        self._atualizar_status_windows_services(log_line=True)

    def _reiniciar_windows_services(self) -> None:
        for nome in (self._nome_win_svc_motoristas(), self._nome_win_svc_afastamentos()):
            if not nome:
                continue
            self._acao_windows_service(nome, "stop", log_fn=self._log_sync)
            self._aguardar_windows_service(nome, "STOPPED", timeout_segundos=20)
            self._acao_windows_service(nome, "start", log_fn=self._log_sync)
        time.sleep(1)
        self._atualizar_status_windows_services(log_line=True)

    def _iniciar_windows_services_api(self) -> None:
        nome_m = self._nome_win_svc_api_motoristas()
        nome_a = self._nome_win_svc_api_afastamentos()
        self._acao_windows_service(nome_m, "start", log_fn=self._log_api)
        self._acao_windows_service(nome_a, "start", log_fn=self._log_api)
        time.sleep(1)
        self._atualizar_status_windows_services_api(log_line=True)

    def _parar_windows_services_api(self) -> None:
        nome_m = self._nome_win_svc_api_motoristas()
        nome_a = self._nome_win_svc_api_afastamentos()
        self._acao_windows_service(nome_m, "stop", log_fn=self._log_api)
        self._acao_windows_service(nome_a, "stop", log_fn=self._log_api)
        time.sleep(1)
        self._atualizar_status_windows_services_api(log_line=True)

    def _reiniciar_windows_services_api(self) -> None:
        for nome in (self._nome_win_svc_api_motoristas(), self._nome_win_svc_api_afastamentos()):
            if not nome:
                continue
            self._acao_windows_service(nome, "stop", log_fn=self._log_api)
            self._aguardar_windows_service(nome, "STOPPED", timeout_segundos=20)
            self._acao_windows_service(nome, "start", log_fn=self._log_api)
        time.sleep(1)
        self._atualizar_status_windows_services_api(log_line=True)

    def _atualizacao_inicial(self) -> None:
        self._atualizar_status_windows_services(log_line=False)
        self._atualizar_status_windows_services_api(log_line=False)
        self._atualizar_monitor_api(log_line=False)
        self._atualizar_lista_integracao(log_line=False)

    def _agendar_monitoramento_periodico(self) -> None:
        if self._closing:
            return
        self._run_async(self._monitoramento_periodico, channel="api")
        self._monitor_job = self.after(15000, self._agendar_monitoramento_periodico)

    def _monitoramento_periodico(self) -> None:
        self._atualizar_status_windows_services(log_line=False)
        self._atualizar_status_windows_services_api(log_line=False)
        self._atualizar_monitor_api(log_line=False)

    def _atualizar_monitor_api(self, *, log_line: bool = True) -> None:
        self._ensure_engine_destino()

        resumo_motoristas = self._consultar_resumo_tabela(settings.target_motorista_table)
        resumo_afastamentos = self._consultar_resumo_tabela(settings.target_afastamento_table)

        texto_m = self._formatar_resumo("Motoristas", resumo_motoristas)
        texto_a = self._formatar_resumo("Afastamentos", resumo_afastamentos)

        if not self._closing:
            try:
                self.after(0, lambda: self.monitor_api_motoristas_var.set(texto_m))
                self.after(0, lambda: self.monitor_api_afastamentos_var.set(texto_a))
            except tk.TclError:
                pass

        if log_line:
            self._log_api(f"[Monitor API] {texto_m}")
            self._log_api(f"[Monitor API] {texto_a}")
            self._atualizar_status_windows_services_api(log_line=True)

    def _consultar_resumo_tabela(self, table_name: str) -> dict[str, Any]:
        resolved = self._resolver_colunas_tabela(
            table_name,
            optional_columns={
                "status": "Status",
                "tentativas": "Tentativas",
                "criado_em": "CriadoEm",
                "atualizado_em": "AtualizadoEm",
                "processado_em": "ProcessadoEm",
            },
        )

        schema = self._safe_identifier(settings.target_schema, "Schema")
        table = self._safe_identifier(table_name, "Tabela")

        select_parts = ["COUNT(1) AS total"]

        if "status" in resolved:
            for status_name in ("PENDENTE", "PROCESSANDO", "PROCESSADO", "ERRO"):
                alias = status_name.lower()
                select_parts.append(
                    f"SUM(CASE WHEN t.[{resolved['status']}] = '{status_name}' THEN 1 ELSE 0 END) AS [{alias}]"
                )

        if "tentativas" in resolved:
            select_parts.append(f"MAX(ISNULL(t.[{resolved['tentativas']}], 0)) AS max_tentativas")

        col_data = resolved.get("atualizado_em") or resolved.get("criado_em") or resolved.get("processado_em")
        if col_data:
            select_parts.append(f"MAX(t.[{col_data}]) AS ultima_data")

        sql = text(f"SELECT {', '.join(select_parts)} FROM [{schema}].[{table}] AS t")
        with self.engine_destino.connect() as conn:
            row = conn.execute(sql).mappings().one()

        return dict(row)

    @staticmethod
    def _formatar_resumo(prefixo: str, resumo: dict[str, Any]) -> str:
        total = int(resumo.get("total") or 0)
        pend = int(resumo.get("pendente") or 0)
        proc = int(resumo.get("processando") or 0)
        ok = int(resumo.get("processado") or 0)
        erro = int(resumo.get("erro") or 0)
        max_tent = int(resumo.get("max_tentativas") or 0)
        ultima = IntegracaoApp._format_datetime(resumo.get("ultima_data"))
        return (
            f"{prefixo}: total={total} pendente={pend} processando={proc} "
            f"processado={ok} erro={erro} max_tent={max_tent} ultima={ultima}"
        )
    def _atualizar_lista_integracao(self, *, log_line: bool = True) -> None:
        self._set_status("Status: carregando lista de integracao...")
        self._ensure_engine_destino()

        tipo = self.lista_tipo_var.get().strip() or "Ambos"
        status_filter = self.lista_status_var.get().strip() or "Todos"
        limite = self._get_lista_limite()

        eventos = self._consultar_lista_integracao(tipo=tipo, status_filter=status_filter, limite=limite)
        self.lista_eventos_cache = eventos

        def _render() -> None:
            self.lista_tree.delete(*self.lista_tree.get_children())
            for idx, evento in enumerate(eventos):
                self.lista_tree.insert(
                    "",
                    tk.END,
                    iid=str(idx),
                    values=(
                        evento.get("tabela"),
                        evento.get("chave"),
                        evento.get("status"),
                        evento.get("tentativas"),
                        evento.get("evento"),
                        evento.get("criado_em"),
                        evento.get("atualizado_em"),
                        evento.get("proxima_tentativa"),
                        evento.get("processado_em"),
                        evento.get("http_status"),
                        evento.get("erro"),
                    ),
                )

            self.lista_detalhes_text.delete("1.0", tk.END)
            if eventos:
                self.lista_tree.selection_set("0")
                self.lista_tree.focus("0")
                self._on_lista_item_select()

        if not self._closing:
            try:
                self.after(0, _render)
            except tk.TclError:
                pass

        self._set_status(f"Status: lista de integracao carregada ({len(eventos)} itens)")
        if log_line:
            self._log_api(f"[Lista] {len(eventos)} eventos carregados (tipo={tipo}, status={status_filter}, limite={limite}).")

    def _consultar_lista_integracao(self, *, tipo: str, status_filter: str, limite: int) -> list[dict[str, Any]]:
        alvos: list[tuple[str, str]] = []
        tipo_norm = (tipo or "").strip().lower()

        if tipo_norm in ("ambos", "motoristas"):
            alvos.append(("Motoristas", settings.target_motorista_table))
        if tipo_norm in ("ambos", "afastamentos"):
            alvos.append(("Afastamentos", settings.target_afastamento_table))

        eventos: list[dict[str, Any]] = []
        for tipo_label, tabela in alvos:
            eventos.extend(
                self._consultar_eventos_tabela(
                    tipo_label=tipo_label,
                    table_name=tabela,
                    status_filter=status_filter,
                    limite=limite,
                )
            )

        eventos.sort(key=lambda x: x.get("_sort_time") or datetime.min, reverse=True)
        return eventos[:limite]

    def _consultar_eventos_tabela(
        self,
        *,
        tipo_label: str,
        table_name: str,
        status_filter: str,
        limite: int,
    ) -> list[dict[str, Any]]:
        if tipo_label == "Motoristas":
            columns = {
                "id_de_origem": "IdDeOrigem",
                "numemp": "NumEmp",
                "numcad": "NumCad",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
                "status": "Status",
                "tentativas": "Tentativas",
                "criado_em": "CriadoEm",
                "atualizado_em": "AtualizadoEm",
                "proxima_tentativa_em": "ProximaTentativaEm",
                "processado_em": "ProcessadoEm",
                "http_status": "HttpStatus",
                "ultimo_erro": "UltimoErro",
                "resposta_resumo": "RespostaResumo",
                "payload_json": "PayloadJson",
            }
            key_aliases = ["id_de_origem", "numemp", "numcad", "evento_tipo", "versao_payload"]
        else:
            columns = {
                "numempresa": "NumeroDaEmpresa",
                "tipocolaborador": "TipoDeColaborador",
                "numorigem": "NumeroDeOrigemDoColaborador",
                "dataafastamento": "DataDoAfastamento",
                "situacao": "Situacao",
                "descricao_situacao": "DescricaoSituacao",
                "evento_tipo": "EventoTipo",
                "versao_payload": "VersaoPayload",
                "hash_payload": "HashPayload",
                "status": "Status",
                "tentativas": "Tentativas",
                "criado_em": "CriadoEm",
                "atualizado_em": "AtualizadoEm",
                "proxima_tentativa_em": "ProximaTentativaEm",
                "processado_em": "ProcessadoEm",
                "http_status": "HttpStatus",
                "ultimo_erro": "UltimoErro",
                "resposta_resumo": "RespostaResumo",
                "payload_json": "PayloadJson",
            }
            key_aliases = [
                "numempresa",
                "tipocolaborador",
                "numorigem",
                "dataafastamento",
                "situacao",
                "descricao_situacao",
                "evento_tipo",
            ]

        resolved = self._resolver_colunas_tabela(table_name, optional_columns=columns)

        select_parts: list[str] = []
        for alias in columns.keys():
            if alias in resolved:
                select_parts.append(f"t.[{resolved[alias]}] AS [{alias}]")
            else:
                select_parts.append(f"NULL AS [{alias}]")

        where_parts: list[str] = []
        params: dict[str, Any] = {"limite": max(1, int(limite))}

        status_norm = (status_filter or "Todos").strip().upper()
        if status_norm != "TODOS" and "status" in resolved:
            where_parts.append(f"t.[{resolved['status']}] = :status_filter")
            params["status_filter"] = status_norm

        order_col = resolved.get("atualizado_em") or resolved.get("criado_em") or resolved.get("processado_em")
        if not order_col:
            for alias in key_aliases:
                if alias in resolved:
                    order_col = resolved[alias]
                    break

        schema = self._safe_identifier(settings.target_schema, "Schema")
        table = self._safe_identifier(table_name, "Tabela")

        sql = (
            f"SELECT TOP (:limite) {', '.join(select_parts)} "
            f"FROM [{schema}].[{table}] AS t"
        )
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        if order_col:
            sql += f" ORDER BY t.[{order_col}] DESC"

        with self.engine_destino.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()

        eventos: list[dict[str, Any]] = []
        for row in rows:
            row_dict = dict(row)
            sort_time = (
                row_dict.get("atualizado_em")
                or row_dict.get("criado_em")
                or row_dict.get("processado_em")
                or row_dict.get("dataafastamento")
            )
            eventos.append(
                {
                    "tabela": tipo_label,
                    "chave": self._montar_chave_evento(tipo_label, row_dict),
                    "status": self._to_text(row_dict.get("status")),
                    "tentativas": self._to_text(row_dict.get("tentativas")),
                    "evento": self._to_text(row_dict.get("evento_tipo")),
                    "criado_em": self._format_datetime(row_dict.get("criado_em")),
                    "atualizado_em": self._format_datetime(row_dict.get("atualizado_em")),
                    "proxima_tentativa": self._format_datetime(row_dict.get("proxima_tentativa_em")),
                    "processado_em": self._format_datetime(row_dict.get("processado_em")),
                    "http_status": self._to_text(row_dict.get("http_status")),
                    "erro": self._to_text(row_dict.get("ultimo_erro"), max_len=180),
                    "_payload": row_dict.get("payload_json"),
                    "_resposta": row_dict.get("resposta_resumo"),
                    "_erro_full": row_dict.get("ultimo_erro"),
                    "_sort_time": sort_time if isinstance(sort_time, datetime) else datetime.min,
                }
            )

        return eventos
    def _on_lista_item_select(self, _event=None) -> None:
        selecionados = self.lista_tree.selection()
        if not selecionados:
            return

        try:
            idx = int(selecionados[0])
        except Exception:
            return

        if idx < 0 or idx >= len(self.lista_eventos_cache):
            return

        evento = self.lista_eventos_cache[idx]
        detalhes = [
            f"Tabela: {evento.get('tabela')}",
            f"Chave: {evento.get('chave')}",
            f"Status: {evento.get('status')}",
            f"Tentativas: {evento.get('tentativas')}",
            f"Evento: {evento.get('evento')}",
            f"Criado em: {evento.get('criado_em')}",
            f"Atualizado em: {evento.get('atualizado_em')}",
            f"Processado em: {evento.get('processado_em')}",
            f"Proxima tentativa: {evento.get('proxima_tentativa')}",
            f"HTTP: {evento.get('http_status')}",
            "",
            "Ultimo erro:",
            self._to_text(evento.get("_erro_full")),
            "",
            "Resumo de resposta:",
            self._to_text(evento.get("_resposta")),
            "",
            "Payload JSON:",
            self._to_text(evento.get("_payload")),
        ]

        self.lista_detalhes_text.delete("1.0", tk.END)
        self.lista_detalhes_text.insert(tk.END, "\n".join(detalhes))

    def _montar_chave_evento(self, tipo_label: str, row: dict[str, Any]) -> str:
        partes: list[str] = []
        if tipo_label == "Motoristas":
            if row.get("id_de_origem") is not None:
                partes.append(f"IdOrigem={row.get('id_de_origem')}")
            if row.get("numemp") is not None:
                partes.append(f"NumEmp={row.get('numemp')}")
            if row.get("numcad") is not None:
                partes.append(f"NumCad={row.get('numcad')}")
            if row.get("evento_tipo"):
                partes.append(f"Evento={row.get('evento_tipo')}")
            if row.get("versao_payload"):
                partes.append(f"Versao={row.get('versao_payload')}")
        else:
            if row.get("numempresa") is not None:
                partes.append(f"NumEmp={row.get('numempresa')}")
            if row.get("tipocolaborador") is not None:
                partes.append(f"TipCol={row.get('tipocolaborador')}")
            if row.get("numorigem") is not None:
                partes.append(f"Origem={row.get('numorigem')}")
            if row.get("dataafastamento") is not None:
                partes.append(f"Data={self._format_datetime(row.get('dataafastamento'))}")
            if row.get("situacao") is not None:
                partes.append(f"Sit={row.get('situacao')}")
            if row.get("descricao_situacao"):
                partes.append(f"Desc={row.get('descricao_situacao')}")
            if row.get("evento_tipo"):
                partes.append(f"Evento={row.get('evento_tipo')}")
        return " | ".join(partes) if partes else "-"

    def _resolver_colunas_tabela(
        self,
        table_name: str,
        *,
        required_columns: dict[str, str] | None = None,
        optional_columns: dict[str, str] | None = None,
    ) -> dict[str, str]:
        lookup = self._carregar_colunas_tabela(table_name)
        resolved: dict[str, str] = {}

        for alias, logical_name in (required_columns or {}).items():
            key = self._normalize_key(logical_name)
            if key not in lookup:
                raise ValueError(
                    f"Coluna obrigatoria nao encontrada em [{settings.target_schema}].[{table_name}]: {logical_name}"
                )
            resolved[alias] = lookup[key]

        for alias, logical_name in (optional_columns or {}).items():
            key = self._normalize_key(logical_name)
            if key in lookup:
                resolved[alias] = lookup[key]

        return resolved

    def _carregar_colunas_tabela(self, table_name: str) -> dict[str, str]:
        self._ensure_engine_destino()
        table = self._safe_identifier(table_name, "Tabela")
        cache_key = f"{self._database_destino().lower()}::{settings.target_schema.lower()}::{table.lower()}"

        if cache_key in self._cache_colunas:
            return self._cache_colunas[cache_key]

        with self.engine_destino.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT c.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS AS c
                    WHERE c.TABLE_SCHEMA = :schema
                    AND c.TABLE_NAME = :table_name
                    """
                ),
                {"schema": settings.target_schema, "table_name": table},
            ).scalars().all()

        if not rows:
            raise ValueError(f"Tabela nao encontrada: [{settings.target_schema}].[{table}]")

        mapped = {self._normalize_key(col): col for col in rows}
        self._cache_colunas[cache_key] = mapped
        return mapped

    @staticmethod
    def _normalize_key(value: str) -> str:
        return "".join(ch for ch in str(value).lower() if ch.isalnum())

    @staticmethod
    def _safe_identifier(value: str, label: str) -> str:
        normalized = (value or "").strip()
        if not _IDENTIFIER_RE.fullmatch(normalized):
            raise ValueError(f"{label} invalido: {value!r}")
        return normalized

    @staticmethod
    def _format_datetime(value: Any) -> str:
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if value is None:
            return "-"
        text_value = str(value).strip()
        return text_value if text_value else "-"

    @staticmethod
    def _to_text(value: Any, *, max_len: int | None = None) -> str:
        if value is None:
            return "-"
        text_value = str(value).strip()
        if not text_value:
            return "-"
        if max_len is not None and len(text_value) > max_len:
            return text_value[: max_len - 3] + "..."
        return text_value

    def _ao_fechar(self) -> None:
        self._closing = True
        if self._monitor_job is not None:
            try:
                self.after_cancel(self._monitor_job)
            except Exception:
                pass

        self._parar_servicos()

        if self.engine_origem is not None:
            try:
                self.engine_origem.dispose()
            except Exception:
                pass
        if self.engine_destino is not None:
            try:
                self.engine_destino.dispose()
            except Exception:
                pass

        self.after(180, self.destroy)


# compatibilidade com chamadas antigas
IntegracaoApp._log = IntegracaoApp._log_sync


def iniciar_interface() -> None:
    app = IntegracaoApp()
    app.mainloop()
