
import re
import subprocess
import threading
import time
import uuid
import tkinter as tk
from datetime import datetime
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
from typing import Any
try:
    import customtkinter as ctk
    HAS_CUSTOMTKINTER = True
except Exception:
    ctk = None
    HAS_CUSTOMTKINTER = False

from sqlalchemy import text
from sqlalchemy.engine import Engine

from Cadastro_API.login import login_api
from config.engine import ativar_engine
from config.integration_registry import IntegracaoClienteApi, IntegracaoEndpoint, IntegracaoRegistry
from config.settings import settings
from src.integradora.afastamento_sync_service import AfastamentoSyncService
from src.integradora.api_dispatch_service import ApiDispatchService
from src.integradora.motorista_sync_service import MotoristaSyncService


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BaseWindow = ctk.CTk if HAS_CUSTOMTKINTER else tk.Tk


class IntegracaoApp(BaseWindow):
    def __init__(self) -> None:
        super().__init__()
        if HAS_CUSTOMTKINTER:
            ctk.set_appearance_mode("light")
            ctk.set_default_color_theme("blue")
        self.option_add("*Font", "{Segoe UI} 10")
        self.title("Integrador de APIs")
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
        self._busy_sync = 0
        self._busy_api = 0
        self._busy_lock = threading.Lock()
        self._progress_job: str | None = None
        self._progress_active = False
        self.registry = IntegracaoRegistry()
        self.integracao_items: list[IntegracaoClienteApi] = []
        self.integracao_selected_id: str | None = None
        self.endpoint_selected_id: str | None = None
        self.current_endpoints: list[IntegracaoEndpoint] = []

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
            [
                settings.win_service_api_motoristas,
                settings.win_service_api_motoristas_dev,
                settings.win_service_api_motoristas_prod,
            ]
        )
        self.win_service_api_afastamentos_opcoes = self._unique(
            [
                settings.win_service_api_afastamentos,
                settings.win_service_api_afastamentos_dev,
                settings.win_service_api_afastamentos_prod,
            ]
        )
        self.endpoint_tipo_opcoes = ["motoristas", "afastamentos"]
        self.endpoint_tabela_opcoes = self._unique(
            [settings.target_motorista_table, settings.target_afastamento_table]
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
        self.progress_text_var = tk.StringVar(value="Progresso: ocioso")
        self.progress_value_var = tk.DoubleVar(value=0.0)
        self.sync_exec_status_var = tk.StringVar(value="SYNC: ocioso")
        self.api_exec_status_var = tk.StringVar(value="API: ocioso")
        self.servicos_status_var = tk.StringVar(value="Servicos locais: motoristas=OFF afastamentos=OFF")
        self.windows_services_status_var = tk.StringVar(value="Windows Sync: motoristas=? afastamentos=?")
        self.windows_api_services_status_var = tk.StringVar(value="Windows API: motoristas=? afastamentos=?")
        self.main_maximize_text_var = tk.StringVar(value="Maximizar")

        self.monitor_api_motoristas_var = tk.StringVar(value="Motoristas: aguardando atualizacao")
        self.monitor_api_afastamentos_var = tk.StringVar(value="Afastamentos: aguardando atualizacao")

        self.lista_tipo_var = tk.StringVar(value="Ambos")
        self.lista_status_var = tk.StringVar(value="Todos")
        self.lista_limite_var = tk.StringVar(value="100")
        self.lista_eventos_cache: list[dict[str, Any]] = []
        self.cliente_api_ativo_var = tk.StringVar(value="Cliente API ativo: padrao (.env)")
        self.api_cliente_switch_var = tk.StringVar(value="")
        self._api_cliente_opcoes: dict[str, str] = {}

        self.int_nome_var = tk.StringVar(value="")
        self.int_fornecedor_var = tk.StringVar(value="ATS_Log")
        self.int_login_url_var = tk.StringVar(value="")
        self.int_base_url_var = tk.StringVar(value="")
        self.int_usuario_var = tk.StringVar(value="")
        self.int_senha_var = tk.StringVar(value="")
        self.int_endpoint_tipo_var = tk.StringVar(value=self.endpoint_tipo_opcoes[0])
        self.int_endpoint_path_var = tk.StringVar(value="")
        self.int_endpoint_tabela_var = tk.StringVar(value=settings.target_motorista_table)
        self.int_endpoint_ativo_var = tk.BooleanVar(value=True)
        self.int_map_origem_var = tk.StringVar(value="")
        self.int_map_destino_var = tk.StringVar(value="")
        self.int_map_padrao_var = tk.StringVar(value="")
        self.int_map_transformacao_var = tk.StringVar(value="")
        self.int_map_obrigatorio_var = tk.BooleanVar(value=False)
        self.int_map_ativo_var = tk.BooleanVar(value=True)
        self.map_selected_index: int | None = None
        self.map_transformacoes_opcoes = [
            "",
            "str",
            "upper",
            "lower",
            "int",
            "float",
            "bool",
            "cpf_digits",
            "date_yyyy_mm_dd",
        ]
        self.int_timeout_var = tk.StringVar(value=str(settings.api_timeout_seconds))

        self._setup_styles()
        self._build_ui()
        self._garantir_cadastro_api_padrao()
        self._carregar_configs_integracao(log_line=False)
        self.after(300, lambda: self._run_async(self._atualizacao_inicial, channel="api"))
        self.after(1000, self._agendar_monitoramento_periodico)

    def _setup_styles(self) -> None:
        style = ttk.Style(self)
        for theme in ("vista", "xpnative", "clam", "default"):
            if theme in style.theme_names():
                style.theme_use(theme)
                break

        self.configure(background="#f3f5f7")
        style.configure("TFrame", background="#f3f5f7")
        style.configure("TLabel", foreground="#1f2937")
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"), foreground="#0f172a")
        style.configure("SubTitle.TLabel", font=("Segoe UI", 10), foreground="#475569")
        style.configure("TLabelframe", background="#ffffff", borderwidth=1, relief="solid")
        style.configure("TLabelframe.Label", font=("Segoe UI", 10, "bold"), foreground="#0f172a")
        style.configure("TButton", padding=(10, 6))
        style.configure("Primary.TButton", foreground="#ffffff")
        style.map("Primary.TButton", background=[("!disabled", "#0d6efd"), ("pressed", "#0b5ed7")])
        style.configure("TNotebook", background="#f3f5f7")
        style.configure("TNotebook.Tab", padding=(14, 8), font=("Segoe UI", 10, "bold"))
        style.configure("Treeview", rowheight=24, font=("Consolas", 10))
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        style.configure("StatusIdle.TLabel", foreground="#1b5e20")
        style.configure("StatusBusy.TLabel", foreground="#e65100")

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        header = ttk.Frame(self, padding=(12, 12, 12, 6))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="Integrador Multi-Cliente de APIs", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="Cadastre clientes, autenticação e endpoints para executar sincronização e envio com segurança.",
            style="SubTitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        if not HAS_CUSTOMTKINTER:
            ttk.Label(
                header,
                text="Modo visual padrão ativo (instale customtkinter para UI avançada).",
                style="SubTitle.TLabel",
            ).grid(row=2, column=0, sticky="w", pady=(2, 0))

        top = ttk.Frame(self, padding=12)
        top.grid(row=1, column=0, sticky="ew")
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

        ttk.Button(top, text="Login API", style="Primary.TButton", command=lambda: self._run_async(self._login, channel="api")).grid(
            row=0,
            column=9,
            padx=(0, 8),
        )
        ttk.Button(top, text="Atualizar monitor", command=lambda: self._run_async(self._atualizar_monitor_api, channel="api")).grid(
            row=0,
            column=10,
            padx=(0, 8),
        )
        self.sync_status_label = ttk.Label(top, textvariable=self.sync_exec_status_var, style="StatusIdle.TLabel")
        self.sync_status_label.grid(row=0, column=11, padx=(6, 8), sticky="w")
        self.api_status_label = ttk.Label(top, textvariable=self.api_exec_status_var, style="StatusIdle.TLabel")
        self.api_status_label.grid(row=0, column=12, padx=(0, 8), sticky="w")
        ttk.Label(top, textvariable=self.cliente_api_ativo_var).grid(row=0, column=13, padx=(10, 0), sticky="w")
        ttk.Button(top, textvariable=self.main_maximize_text_var, command=self._toggle_main_window).grid(
            row=0, column=14, padx=(10, 0), sticky="e"
        )

        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 8))

        self.tab_sync = ttk.Frame(self.notebook, padding=10)
        self.tab_api = ttk.Frame(self.notebook, padding=10)
        self.tab_lista = ttk.Frame(self.notebook, padding=10)
        self.tab_clientes = ttk.Frame(self.notebook, padding=10)

        self.notebook.add(self.tab_sync, text="Sincronizacao")
        self.notebook.add(self.tab_api, text="Monitor API")
        self.notebook.add(self.tab_lista, text="Lista Integracao")
        self.notebook.add(self.tab_clientes, text="Clientes/API (Auth + Endpoints)")

        self._build_tab_sync()
        self._build_tab_api()
        self._build_tab_lista()
        self._build_tab_clientes()

        ttk.Label(self, textvariable=self.status_var, padding=(12, 0, 12, 4)).grid(row=3, column=0, sticky="w")

        progresso = ttk.Frame(self, padding=(12, 0, 12, 10))
        progresso.grid(row=4, column=0, sticky="ew")
        progresso.columnconfigure(1, weight=1)
        ttk.Label(progresso, textvariable=self.progress_text_var).grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.progress_bar = ttk.Progressbar(
            progresso,
            orient="horizontal",
            mode="determinate",
            variable=self.progress_value_var,
            maximum=100.0,
        )
        self.progress_bar.grid(row=0, column=1, sticky="ew")
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
        self.tab_api.rowconfigure(4, weight=1)

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

        perfil = ttk.LabelFrame(self.tab_api, text="Cliente/API para envio", padding=10)
        perfil.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        ttk.Label(perfil, text="Perfil ativo:").grid(row=0, column=0, sticky="w")
        self.api_cliente_combo = ttk.Combobox(
            perfil,
            textvariable=self.api_cliente_switch_var,
            state="readonly",
            width=62,
        )
        self.api_cliente_combo.grid(row=0, column=1, padx=(6, 10), sticky="w")
        ttk.Button(
            perfil,
            text="Ativar perfil",
            command=lambda: self._run_async(self._ativar_cliente_api_por_selecao, channel="api"),
        ).grid(row=0, column=2, padx=(0, 8), sticky="w")
        ttk.Label(
            perfil,
            text="Use um perfil de homologacao/producao para alternar o destino da API sem trocar servico.",
            style="SubTitle.TLabel",
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 0))

        win = ttk.LabelFrame(self.tab_api, text="Servicos Windows (API)", padding=10)
        win.grid(row=2, column=0, sticky="ew", pady=(8, 0))

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
        resumo.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        resumo.columnconfigure(0, weight=1)
        resumo.columnconfigure(1, weight=1)

        card_m = ttk.LabelFrame(resumo, text="Fila Motoristas", padding=10)
        card_m.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Label(card_m, textvariable=self.monitor_api_motoristas_var, justify=tk.LEFT).grid(row=0, column=0, sticky="w")

        card_a = ttk.LabelFrame(resumo, text="Fila Afastamentos", padding=10)
        card_a.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Label(card_a, textvariable=self.monitor_api_afastamentos_var, justify=tk.LEFT).grid(row=0, column=0, sticky="w")

        output = ttk.LabelFrame(self.tab_api, text="Log de envio API", padding=10)
        output.grid(row=4, column=0, sticky="nsew", pady=(8, 0))
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

    def _build_tab_clientes(self) -> None:
        self.tab_clientes.columnconfigure(0, weight=1)
        self.tab_clientes.rowconfigure(1, weight=1)

        acoes = ttk.Frame(self.tab_clientes)
        acoes.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        acoes.columnconfigure(99, weight=1)
        ttk.Button(acoes, text="Novo cadastro", command=self._novo_config_integracao).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(acoes, text="Abrir cadastro", command=self._abrir_config_integracao_selecionada).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(acoes, text="Definir ativo", command=self._definir_config_ativa).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(acoes, text="Remover", command=self._remover_config_integracao).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(acoes, text="Recarregar", command=self._recarregar_configs_integracao).grid(row=0, column=4, padx=(0, 6))
        ttk.Label(
            acoes,
            text="Dica: clique duas vezes em um cliente para abrir detalhes, endpoints e testar login.",
            style="SubTitle.TLabel",
        ).grid(row=0, column=99, sticky="e")

        lista = ttk.LabelFrame(self.tab_clientes, text="Clientes/API cadastrados", padding=10)
        lista.grid(row=1, column=0, sticky="nsew")
        lista.columnconfigure(0, weight=1)
        lista.rowconfigure(0, weight=1)

        cols = ("nome", "fornecedor", "ativo", "login_url", "base_url", "usuario", "endpoint_m", "endpoint_a", "timeout")
        self.integracao_tree = ttk.Treeview(lista, columns=cols, show="headings", height=20)
        self.integracao_tree.grid(row=0, column=0, sticky="nsew")
        self.integracao_tree.bind("<<TreeviewSelect>>", self._on_integracao_item_select)
        self.integracao_tree.bind("<Double-1>", self._abrir_config_integracao_selecionada)

        scrollbar = ttk.Scrollbar(lista, orient="vertical", command=self.integracao_tree.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.integracao_tree.configure(yscrollcommand=scrollbar.set)

        headers = {
            "nome": "Cliente",
            "fornecedor": "Fornecedor",
            "ativo": "Ativo",
            "login_url": "Login URL",
            "base_url": "Base URL",
            "usuario": "Usuario",
            "endpoint_m": "Qtd Endpoints",
            "endpoint_a": "Tipos",
            "timeout": "Timeout",
        }
        widths = {
            "nome": 180,
            "fornecedor": 110,
            "ativo": 60,
            "login_url": 220,
            "base_url": 220,
            "usuario": 130,
            "endpoint_m": 130,
            "endpoint_a": 130,
            "timeout": 70,
        }
        for col in cols:
            self.integracao_tree.heading(col, text=headers[col])
            self.integracao_tree.column(col, width=widths[col], anchor="w")

    def _recarregar_configs_integracao(self) -> None:
        self._carregar_configs_integracao(log_line=True)

    def _novo_config_integracao(self) -> None:
        self.integracao_selected_id = None
        self._abrir_dialog_integracao(None)

    def _on_integracao_item_select(self, _event=None) -> None:
        selected = self.integracao_tree.selection()
        if not selected:
            return
        iid = selected[0]
        item = next((x for x in self.integracao_items if x.id == iid), None)
        if item is None:
            return

        self.integracao_selected_id = item.id

    def _abrir_config_integracao_selecionada(self, _event=None) -> None:
        if not self.integracao_selected_id:
            selected = self.integracao_tree.selection()
            if not selected:
                self._set_status("Status: selecione um cliente/API para abrir")
                return
            self.integracao_selected_id = selected[0]

        item = next((x for x in self.integracao_items if x.id == self.integracao_selected_id), None)
        if item is None:
            self._set_status("Status: cliente/API selecionado nao encontrado")
            return
        self._abrir_dialog_integracao(item)

    def _abrir_dialog_integracao(self, item: IntegracaoClienteApi | None) -> None:
        self.endpoint_selected_id = None
        self.current_endpoints = list((item.endpoints if item else []) or [])
        self.int_nome_var.set(item.nome if item else "")
        self.int_fornecedor_var.set(item.fornecedor if item else "ATS_Log")
        self.int_login_url_var.set(item.login_url if item else "")
        self.int_base_url_var.set(item.base_url if item else "")
        self.int_usuario_var.set(item.usuario if item else "")
        self.int_senha_var.set(item.senha if item else "")
        self.int_timeout_var.set(str((item.timeout_seconds if item else settings.api_timeout_seconds)))
        self._limpar_endpoint_form()
        self.integracao_selected_id = item.id if item else None

        dialog = tk.Toplevel(self)
        dialog.title("Cadastro de Integracao API" if item is None else f"Detalhes da Integracao: {item.nome}")
        dialog.geometry("1220x840")
        dialog.minsize(1080, 760)
        dialog.transient(self)
        dialog.grab_set()
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(1, weight=1)
        dialog.protocol("WM_DELETE_WINDOW", lambda: self._fechar_dialog_integracao(dialog))

        topo = ttk.Frame(dialog, padding=(12, 12, 12, 6))
        topo.grid(row=0, column=0, sticky="ew")
        topo.columnconfigure(0, weight=1)
        dialog_maximize_text_var = tk.StringVar(value="Maximizar")
        ttk.Label(topo, text="Configuracao da Integracao", style="Title.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            topo,
            text="Preencha autenticacao e endpoints; use 'Testar login' antes de salvar.",
            style="SubTitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))
        ttk.Button(
            topo,
            textvariable=dialog_maximize_text_var,
            command=lambda: self._toggle_window_maximize(dialog, dialog_maximize_text_var),
        ).grid(row=0, column=1, rowspan=2, sticky="e")

        corpo = ttk.Frame(dialog, padding=(12, 0, 12, 8))
        corpo.grid(row=1, column=0, sticky="nsew")
        corpo.columnconfigure(0, weight=1)
        corpo.rowconfigure(2, weight=1)

        bloco_cliente = ttk.LabelFrame(corpo, text="Identificacao do Cliente", padding=10)
        bloco_cliente.grid(row=0, column=0, sticky="ew")
        bloco_cliente.columnconfigure(1, weight=1)
        ttk.Label(bloco_cliente, text="Nome cliente:").grid(row=0, column=0, sticky="w")
        ttk.Entry(bloco_cliente, textvariable=self.int_nome_var).grid(row=0, column=1, sticky="ew", padx=(8, 10))
        ttk.Label(bloco_cliente, text="Fornecedor:").grid(row=0, column=2, sticky="w")
        ttk.Entry(bloco_cliente, textvariable=self.int_fornecedor_var, width=24).grid(row=0, column=3, sticky="w")

        bloco_auth = ttk.LabelFrame(corpo, text="Autenticacao da API", padding=10)
        bloco_auth.grid(row=1, column=0, sticky="ew", pady=(8, 8))
        bloco_auth.columnconfigure(1, weight=1)
        ttk.Label(bloco_auth, text="Login URL:").grid(row=0, column=0, sticky="w")
        ttk.Entry(bloco_auth, textvariable=self.int_login_url_var).grid(row=0, column=1, sticky="ew", padx=(8, 10))
        ttk.Label(bloco_auth, text="Base URL:").grid(row=1, column=0, sticky="w")
        ttk.Entry(bloco_auth, textvariable=self.int_base_url_var).grid(row=1, column=1, sticky="ew", padx=(8, 10), pady=(6, 0))
        ttk.Label(bloco_auth, text="Usuario:").grid(row=0, column=2, sticky="w")
        ttk.Entry(bloco_auth, textvariable=self.int_usuario_var, width=24).grid(row=0, column=3, sticky="w")
        ttk.Label(bloco_auth, text="Senha:").grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Entry(bloco_auth, textvariable=self.int_senha_var, width=24, show="*").grid(row=1, column=3, sticky="w", pady=(6, 0))
        ttk.Label(bloco_auth, text="Timeout (s):").grid(row=0, column=4, sticky="w", padx=(10, 0))
        ttk.Entry(bloco_auth, textvariable=self.int_timeout_var, width=8).grid(row=0, column=5, sticky="w")

        bloco_endpoint = ttk.LabelFrame(corpo, text="Endpoints da API", padding=10)
        bloco_endpoint.grid(row=2, column=0, sticky="nsew")
        bloco_endpoint.columnconfigure(0, weight=1)
        bloco_endpoint.rowconfigure(2, weight=1)
        bloco_endpoint.rowconfigure(3, weight=1)

        form_ep = ttk.Frame(bloco_endpoint)
        form_ep.grid(row=0, column=0, sticky="ew")
        form_ep.columnconfigure(1, weight=1)
        ttk.Label(form_ep, text="Tipo endpoint:").grid(row=0, column=0, sticky="w")
        tipo_combo = ttk.Combobox(
            form_ep,
            textvariable=self.int_endpoint_tipo_var,
            values=self.endpoint_tipo_opcoes,
            state="readonly",
        )
        tipo_combo.grid(row=0, column=1, sticky="ew", padx=(8, 8))
        tipo_combo.bind("<<ComboboxSelected>>", self._on_endpoint_tipo_change)
        ttk.Label(form_ep, text="Path endpoint:").grid(row=0, column=2, sticky="w")
        ttk.Entry(form_ep, textvariable=self.int_endpoint_path_var, width=34).grid(row=0, column=3, sticky="ew", padx=(8, 8))
        ttk.Label(form_ep, text="Tabela destino:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Combobox(
            form_ep,
            textvariable=self.int_endpoint_tabela_var,
            values=self.endpoint_tabela_opcoes,
            state="readonly",
        ).grid(row=1, column=1, sticky="ew", padx=(8, 8), pady=(6, 0))
        ttk.Checkbutton(form_ep, text="Ativo", variable=self.int_endpoint_ativo_var).grid(row=1, column=2, sticky="w", pady=(6, 0))

        botoes_ep = ttk.Frame(bloco_endpoint)
        botoes_ep.grid(row=1, column=0, sticky="ew", pady=(8, 8))
        ttk.Button(
            botoes_ep,
            text="Adicionar/Atualizar endpoint",
            command=lambda: self._executar_acao_dialog(self._upsert_endpoint_form),
        ).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(
            botoes_ep,
            text="Remover endpoint",
            command=lambda: self._executar_acao_dialog(self._remover_endpoint_form),
        ).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(
            botoes_ep,
            text="Limpar endpoint",
            command=lambda: self._executar_acao_dialog(self._limpar_endpoint_form),
        ).grid(row=0, column=2, padx=(0, 6))

        ep_cols = ("tipo", "endpoint", "tabela", "mapeamentos", "ativo")
        self.endpoint_tree = ttk.Treeview(bloco_endpoint, columns=ep_cols, show="headings", height=10)
        self.endpoint_tree.grid(row=2, column=0, sticky="nsew")
        self.endpoint_tree.bind("<<TreeviewSelect>>", self._on_endpoint_item_select)
        self.endpoint_tree.bind("<Double-1>", self._on_endpoint_item_select)
        for col, title, width in (
            ("tipo", "Tipo", 150),
            ("endpoint", "Endpoint", 340),
            ("tabela", "Tabela", 190),
            ("mapeamentos", "De-para", 90),
            ("ativo", "Ativo", 70),
        ):
            self.endpoint_tree.heading(col, text=title)
            self.endpoint_tree.column(col, width=width, anchor="w")
        self._render_endpoint_tree()

        bloco_mapa = ttk.LabelFrame(
            bloco_endpoint,
            text="De-para do Endpoint Selecionado (coluna da tabela -> campo do endpoint)",
            padding=10,
        )
        bloco_mapa.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        bloco_mapa.columnconfigure(0, weight=1)
        bloco_mapa.rowconfigure(3, weight=1)

        form_map = ttk.Frame(bloco_mapa)
        form_map.grid(row=0, column=0, sticky="ew")
        form_map.columnconfigure(1, weight=1)
        form_map.columnconfigure(3, weight=1)

        ttk.Label(form_map, text="Coluna origem:").grid(row=0, column=0, sticky="w")
        self.map_origem_combo = ttk.Combobox(
            form_map,
            textvariable=self.int_map_origem_var,
            state="normal",
            width=34,
        )
        self.map_origem_combo.grid(row=0, column=1, sticky="ew", padx=(8, 10))

        ttk.Label(form_map, text="Campo destino:").grid(row=0, column=2, sticky="w")
        ttk.Entry(form_map, textvariable=self.int_map_destino_var).grid(row=0, column=3, sticky="ew", padx=(8, 0))

        ttk.Label(form_map, text="Padrao:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(form_map, textvariable=self.int_map_padrao_var).grid(row=1, column=1, sticky="ew", padx=(8, 10), pady=(6, 0))

        ttk.Label(form_map, text="Transformacao:").grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Combobox(
            form_map,
            textvariable=self.int_map_transformacao_var,
            values=self.map_transformacoes_opcoes,
            state="readonly",
            width=24,
        ).grid(row=1, column=3, sticky="w", padx=(8, 0), pady=(6, 0))

        ttk.Checkbutton(form_map, text="Obrigatorio", variable=self.int_map_obrigatorio_var).grid(
            row=2, column=0, sticky="w", pady=(6, 0)
        )
        ttk.Checkbutton(form_map, text="Ativo", variable=self.int_map_ativo_var).grid(
            row=2, column=1, sticky="w", pady=(6, 0)
        )

        ttk.Label(
            bloco_mapa,
            text=(
                "Selecione um endpoint acima. A coluna origem gera automaticamente origem='colunas.<Coluna>'. "
                "Se preferir, voce pode digitar caminho manual."
            ),
            style="SubTitle.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(6, 4))

        botoes_mapa = ttk.Frame(bloco_mapa)
        botoes_mapa.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(
            botoes_mapa,
            text="Carregar colunas",
            command=lambda: self._executar_acao_dialog(self._recarregar_colunas_de_para_interativo),
        ).grid(
            row=0, column=0, padx=(0, 10)
        )
        ttk.Button(
            botoes_mapa,
            text="Adicionar/Atualizar de-para",
            command=lambda: self._executar_acao_dialog(self._upsert_de_para_form),
        ).grid(
            row=0, column=1, padx=(0, 6)
        )
        ttk.Button(
            botoes_mapa,
            text="Remover de-para",
            command=lambda: self._executar_acao_dialog(self._remover_de_para_form),
        ).grid(
            row=0, column=2, padx=(0, 6)
        )
        ttk.Button(
            botoes_mapa,
            text="Limpar de-para",
            command=lambda: self._executar_acao_dialog(self._limpar_de_para_form),
        ).grid(
            row=0, column=3, padx=(0, 6)
        )

        map_cols = ("origem", "destino", "obrigatorio", "ativo", "padrao", "transformacao")
        self.map_tree = ttk.Treeview(bloco_mapa, columns=map_cols, show="headings", height=9)
        self.map_tree.grid(row=3, column=0, sticky="nsew")
        self.map_tree.bind("<<TreeviewSelect>>", self._on_de_para_item_select)
        self.map_tree.bind("<Double-1>", self._on_de_para_item_select)
        for col, title, width in (
            ("origem", "Origem", 240),
            ("destino", "Destino", 240),
            ("obrigatorio", "Obrigatorio", 90),
            ("ativo", "Ativo", 70),
            ("padrao", "Padrao", 140),
            ("transformacao", "Transformacao", 130),
        ):
            self.map_tree.heading(col, text=title)
            self.map_tree.column(col, width=width, anchor="w")
        self._render_de_para_tree()
        self._atualizar_opcoes_colunas_de_para(log_line=False)

        rodape = ttk.Frame(dialog, padding=(12, 0, 12, 12))
        rodape.grid(row=2, column=0, sticky="ew")
        ttk.Button(rodape, text="Testar login", command=lambda: self._run_async(self._testar_login_config, channel="api")).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(rodape, text="Salvar", style="Primary.TButton", command=lambda: self._salvar_integracao_dialog(dialog)).grid(
            row=0, column=1, padx=(0, 6)
        )
        ttk.Button(rodape, text="Definir ativo", command=self._definir_config_ativa).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(rodape, text="Fechar", command=lambda: self._fechar_dialog_integracao(dialog)).grid(row=0, column=3)

    def _salvar_integracao_dialog(self, dialog: tk.Toplevel) -> None:
        try:
            self._salvar_config_integracao()
            self._carregar_configs_integracao(log_line=False)
            self._fechar_dialog_integracao(dialog)
        except Exception as exc:
            self._set_status(f"Status: erro ao salvar integracao - {exc}")
            self._log_api(str(exc), level="ERROR")

    def _executar_acao_dialog(self, acao) -> None:
        try:
            acao()
        except Exception as exc:
            self._set_status(f"Status: erro - {exc}")
            self._log_api(str(exc), level="WARN")

    def _fechar_dialog_integracao(self, dialog: tk.Toplevel) -> None:
        try:
            dialog.destroy()
        finally:
            if hasattr(self, "endpoint_tree"):
                try:
                    delattr(self, "endpoint_tree")
                except Exception:
                    pass
            if hasattr(self, "map_tree"):
                try:
                    delattr(self, "map_tree")
                except Exception:
                    pass
            if hasattr(self, "map_origem_combo"):
                try:
                    delattr(self, "map_origem_combo")
                except Exception:
                    pass

    @staticmethod
    def _window_esta_maximizada(window: tk.Misc) -> bool:
        try:
            if str(window.state()).lower() == "zoomed":
                return True
        except Exception:
            pass
        try:
            return bool(window.attributes("-zoomed"))
        except Exception:
            return False

    @staticmethod
    def _set_window_maximized(window: tk.Misc, maximizar: bool) -> None:
        try:
            window.state("zoomed" if maximizar else "normal")
            return
        except Exception:
            pass
        try:
            window.attributes("-zoomed", bool(maximizar))
        except Exception:
            return

    def _atualizar_rotulo_maximizacao(self, window: tk.Misc, rotulo: tk.StringVar | None) -> None:
        if rotulo is None:
            return
        rotulo.set("Restaurar" if self._window_esta_maximizada(window) else "Maximizar")

    def _toggle_window_maximize(self, window: tk.Misc, rotulo: tk.StringVar | None = None) -> None:
        maximizar = not self._window_esta_maximizada(window)
        self._set_window_maximized(window, maximizar)
        self._atualizar_rotulo_maximizacao(window, rotulo)

    def _toggle_main_window(self) -> None:
        self._toggle_window_maximize(self, self.main_maximize_text_var)

    def _config_from_form(self) -> IntegracaoClienteApi:
        nome = (self.int_nome_var.get() or "").strip()
        if not nome:
            raise ValueError("Informe o nome do cliente.")
        login_url = (self.int_login_url_var.get() or "").strip()
        if not login_url:
            raise ValueError("Informe o Login URL.")
        usuario = (self.int_usuario_var.get() or "").strip()
        senha = (self.int_senha_var.get() or "").strip()
        if not usuario or not senha:
            raise ValueError("Informe usuario e senha da API.")

        try:
            timeout_seconds = float((self.int_timeout_var.get() or "").strip() or settings.api_timeout_seconds)
        except Exception as exc:
            raise ValueError("Timeout invalido.") from exc
        if not self.current_endpoints:
            raise ValueError("Cadastre ao menos um endpoint para a integracao.")

        return IntegracaoClienteApi(
            id=str(self.integracao_selected_id or "").strip(),
            nome=nome,
            fornecedor=(self.int_fornecedor_var.get() or "ATS_Log").strip() or "ATS_Log",
            base_url=(self.int_base_url_var.get() or "").strip(),
            login_url=login_url,
            usuario=usuario,
            senha=senha,
            timeout_seconds=max(1.0, timeout_seconds),
            endpoints=list(self.current_endpoints),
        )

    def _salvar_config_integracao(self) -> None:
        cfg = self._config_from_form()
        saved = self.registry.upsert(cfg)
        self.integracao_selected_id = saved.id
        self._carregar_configs_integracao(log_line=True)
        self._log_api(f"Cliente/API salvo: {saved.nome}")

    def _remover_config_integracao(self) -> None:
        if not self.integracao_selected_id:
            self._set_status("Status: selecione um cliente/API para remover")
            return
        self.registry.delete(self.integracao_selected_id)
        self.integracao_selected_id = None
        self.current_endpoints = []
        self.endpoint_selected_id = None
        self._limpar_endpoint_form()
        self._carregar_configs_integracao(log_line=True)
        self._log_api("Cliente/API removido.")

    def _definir_config_ativa(self) -> None:
        if not self.integracao_selected_id:
            self._set_status("Status: selecione um cliente/API para ativar")
            return
        self.registry.set_active(self.integracao_selected_id)
        self._carregar_configs_integracao(log_line=True)
        self._log_api("Cliente/API ativo atualizado.")

    def _testar_login_config(self) -> None:
        nome = (self.int_nome_var.get() or "").strip() or "Sem nome"
        login_url = (self.int_login_url_var.get() or "").strip()
        usuario = (self.int_usuario_var.get() or "").strip()
        senha = (self.int_senha_var.get() or "").strip()
        if not login_url or not usuario or not senha:
            raise ValueError("Informe Login URL, usuario e senha para testar autenticacao.")
        timeout_seconds = float((self.int_timeout_var.get() or "").strip() or settings.api_timeout_seconds)
        cfg = IntegracaoClienteApi(
            id=str(self.integracao_selected_id or "").strip(),
            nome=nome,
            fornecedor=(self.int_fornecedor_var.get() or "ATS_Log").strip() or "ATS_Log",
            base_url=(self.int_base_url_var.get() or "").strip(),
            login_url=login_url,
            usuario=usuario,
            senha=senha,
            timeout_seconds=max(1.0, timeout_seconds),
            endpoints=[],
        )
        self._set_status(f"Status: testando login ({cfg.nome})...")
        auth = login_api(timeout=cfg.timeout_seconds, config=cfg.to_runtime_dict())
        exp = auth.get("exp")
        login_url_efetiva = auth.get("_login_url")
        self._set_status("Status: login testado com sucesso")
        if login_url_efetiva:
            self._log_api(f"Login ok para cliente '{cfg.nome}' em {login_url_efetiva}. Expira em: {exp}")
        else:
            self._log_api(f"Login ok para cliente '{cfg.nome}'. Expira em: {exp}")

    def _limpar_endpoint_form(self) -> None:
        self.endpoint_selected_id = None
        self.int_endpoint_tipo_var.set(self.endpoint_tipo_opcoes[0])
        self.int_endpoint_path_var.set("")
        self.int_endpoint_tabela_var.set(settings.target_motorista_table)
        self.int_endpoint_ativo_var.set(True)
        self._limpar_de_para_form()
        self._render_de_para_tree()
        self._atualizar_opcoes_colunas_de_para(log_line=False)

    def _upsert_endpoint_form(self) -> None:
        tipo = (self.int_endpoint_tipo_var.get() or "").strip()
        endpoint = (self.int_endpoint_path_var.get() or "").strip()
        tabela = (self.int_endpoint_tabela_var.get() or "").strip()
        if not tipo:
            raise ValueError("Informe o tipo do endpoint.")
        if not endpoint:
            raise ValueError("Informe o path do endpoint.")

        ep_id = str(self.endpoint_selected_id or "").strip()
        de_para_atual: list[dict[str, Any]] = []
        if ep_id:
            existente = next((x for x in self.current_endpoints if x.id == ep_id), None)
            if existente is not None:
                de_para_atual = [dict(item) for item in (existente.de_para or []) if isinstance(item, dict)]
        novo = IntegracaoEndpoint(
            id=ep_id or str(uuid.uuid4()),
            tipo=tipo,
            endpoint=endpoint,
            tabela_destino=tabela,
            ativo=bool(self.int_endpoint_ativo_var.get()),
            de_para=de_para_atual,
        )

        atualizado = False
        for idx, existing in enumerate(self.current_endpoints):
            if existing.id == novo.id:
                self.current_endpoints[idx] = novo
                atualizado = True
                break
        if not atualizado:
            self.current_endpoints.append(novo)

        self._render_endpoint_tree()
        self.endpoint_selected_id = novo.id
        self._limpar_de_para_form()
        self._atualizar_opcoes_colunas_de_para(log_line=False)
        self._render_de_para_tree()

    def _remover_endpoint_form(self) -> None:
        if not self.endpoint_selected_id:
            raise ValueError("Selecione um endpoint para remover.")
        self.current_endpoints = [ep for ep in self.current_endpoints if ep.id != self.endpoint_selected_id]
        self._render_endpoint_tree()
        self._limpar_endpoint_form()

    def _on_endpoint_item_select(self, _event=None) -> None:
        if not hasattr(self, "endpoint_tree") or not self.endpoint_tree.winfo_exists():
            return
        selected = self.endpoint_tree.selection()
        if not selected:
            return
        ep_id = selected[0]
        ep = next((x for x in self.current_endpoints if x.id == ep_id), None)
        if ep is None:
            return
        self.endpoint_selected_id = ep.id
        self.int_endpoint_tipo_var.set(ep.tipo)
        self.int_endpoint_path_var.set(ep.endpoint)
        self.int_endpoint_tabela_var.set(ep.tabela_destino)
        self.int_endpoint_ativo_var.set(ep.ativo)
        self._limpar_de_para_form()
        self._atualizar_opcoes_colunas_de_para(log_line=True)
        self._render_de_para_tree()

    def _render_endpoint_tree(self) -> None:
        if not hasattr(self, "endpoint_tree") or not self.endpoint_tree.winfo_exists():
            return
        self.endpoint_tree.delete(*self.endpoint_tree.get_children())
        for ep in self.current_endpoints:
            self.endpoint_tree.insert(
                "",
                tk.END,
                iid=ep.id,
                values=(
                    ep.tipo,
                    ep.endpoint,
                    ep.tabela_destino,
                    len([item for item in (ep.de_para or []) if isinstance(item, dict)]),
                    "Sim" if ep.ativo else "Nao",
                ),
            )
        if self.endpoint_selected_id and self.endpoint_tree.exists(self.endpoint_selected_id):
            self.endpoint_tree.selection_set(self.endpoint_selected_id)
            self.endpoint_tree.focus(self.endpoint_selected_id)

    def _endpoint_atual_selecionado(self) -> IntegracaoEndpoint | None:
        if not self.endpoint_selected_id:
            return None
        return next((ep for ep in self.current_endpoints if ep.id == self.endpoint_selected_id), None)

    def _on_endpoint_tipo_change(self, _event=None) -> None:
        tipo_norm = self._normalizar_tipo_endpoint(self.int_endpoint_tipo_var.get())
        if tipo_norm in {"afastamentos", "afastamento"}:
            self.int_endpoint_tabela_var.set(settings.target_afastamento_table)
        else:
            self.int_endpoint_tabela_var.set(settings.target_motorista_table)
        self._atualizar_opcoes_colunas_de_para(log_line=False)

    def _tabela_destino_endpoint(self, endpoint: IntegracaoEndpoint | None) -> str:
        if endpoint is None:
            return ""
        tabela = str(endpoint.tabela_destino or "").strip()
        if tabela:
            return tabela
        tipo_norm = self._normalizar_tipo_endpoint(endpoint.tipo)
        if tipo_norm in {"afastamentos", "afastamento"}:
            return settings.target_afastamento_table
        return settings.target_motorista_table

    def _carregar_opcoes_colunas_de_para(self, endpoint: IntegracaoEndpoint | None) -> list[str]:
        tabela = self._tabela_destino_endpoint(endpoint)
        if not tabela:
            return []
        lookup = self._carregar_colunas_tabela(tabela)
        return sorted(lookup.values())

    def _atualizar_opcoes_colunas_de_para(self, *, log_line: bool) -> list[str]:
        if not hasattr(self, "map_origem_combo") or not self.map_origem_combo.winfo_exists():
            return []
        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            self.map_origem_combo.configure(values=[])
            return []

        try:
            opcoes = self._carregar_opcoes_colunas_de_para(endpoint)
        except Exception as exc:
            self.map_origem_combo.configure(values=[])
            if log_line:
                raise
            opcoes = []
            self._log_api(f"Falha ao carregar colunas para de-para: {exc}", level="WARN")

        self.map_origem_combo.configure(values=opcoes)
        if opcoes and not str(self.int_map_origem_var.get() or "").strip():
            self.int_map_origem_var.set(opcoes[0])
        return opcoes

    def _recarregar_colunas_de_para_interativo(self) -> None:
        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            raise ValueError("Selecione um endpoint para carregar as colunas.")
        opcoes = self._atualizar_opcoes_colunas_de_para(log_line=True)
        tabela = self._tabela_destino_endpoint(endpoint)
        if not opcoes:
            raise ValueError(f"Nenhuma coluna encontrada em [{settings.target_schema}].[{tabela}]")
        self._log_api(
            f"Colunas carregadas para de-para: tabela=[{settings.target_schema}].[{tabela}] total={len(opcoes)}"
        )

    def _limpar_de_para_form(self) -> None:
        self.map_selected_index = None
        self.int_map_origem_var.set("")
        self.int_map_destino_var.set("")
        self.int_map_padrao_var.set("")
        self.int_map_transformacao_var.set("")
        self.int_map_obrigatorio_var.set(False)
        self.int_map_ativo_var.set(True)

    @staticmethod
    def _normalizar_origem_coluna(origem: str) -> str:
        text_value = str(origem or "").strip()
        if not text_value:
            return ""
        if text_value.lower().startswith("colunas."):
            return text_value
        return f"colunas.{text_value}"

    def _render_de_para_tree(self) -> None:
        if not hasattr(self, "map_tree") or not self.map_tree.winfo_exists():
            return
        self.map_tree.delete(*self.map_tree.get_children())

        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            return

        de_para = [item for item in (endpoint.de_para or []) if isinstance(item, dict)]
        for idx, regra in enumerate(de_para):
            origem = str(regra.get("origem") or "").strip()
            origem_view = origem.split(".", 1)[1] if origem.lower().startswith("colunas.") else origem
            self.map_tree.insert(
                "",
                tk.END,
                iid=str(idx),
                values=(
                    origem_view,
                    str(regra.get("destino") or "").strip(),
                    "Sim" if bool(regra.get("obrigatorio", False)) else "Nao",
                    "Sim" if bool(regra.get("ativo", True)) else "Nao",
                    str(regra.get("padrao") if "padrao" in regra else "").strip(),
                    str(regra.get("transformacao") or "").strip(),
                ),
            )

    def _on_de_para_item_select(self, _event=None) -> None:
        if not hasattr(self, "map_tree") or not self.map_tree.winfo_exists():
            return
        selected = self.map_tree.selection()
        if not selected:
            return
        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            return

        try:
            idx = int(selected[0])
        except Exception:
            return

        regras = [item for item in (endpoint.de_para or []) if isinstance(item, dict)]
        if idx < 0 or idx >= len(regras):
            return

        regra = regras[idx]
        origem = str(regra.get("origem") or "").strip()
        origem_view = origem.split(".", 1)[1] if origem.lower().startswith("colunas.") else origem
        self.map_selected_index = idx
        self.int_map_origem_var.set(origem_view)
        self.int_map_destino_var.set(str(regra.get("destino") or "").strip())
        self.int_map_padrao_var.set(str(regra.get("padrao") if "padrao" in regra else "").strip())
        self.int_map_transformacao_var.set(str(regra.get("transformacao") or "").strip())
        self.int_map_obrigatorio_var.set(bool(regra.get("obrigatorio", False)))
        self.int_map_ativo_var.set(bool(regra.get("ativo", True)))

    def _upsert_de_para_form(self) -> None:
        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            raise ValueError("Selecione um endpoint para configurar o de-para.")

        origem_input = (self.int_map_origem_var.get() or "").strip()
        destino = (self.int_map_destino_var.get() or "").strip()
        padrao = (self.int_map_padrao_var.get() or "").strip()
        if not origem_input and not padrao:
            raise ValueError("Informe origem ou um valor padrao.")
        if not destino:
            raise ValueError("Informe o campo de destino.")

        opcoes_colunas = self._atualizar_opcoes_colunas_de_para(log_line=True)
        origem = origem_input
        if origem_input and not origem_input.lower().startswith("colunas.") and "." not in origem_input and ":" not in origem_input:
            coluna_resolvida = origem_input
            for coluna in opcoes_colunas:
                if self._normalize_key(coluna) == self._normalize_key(origem_input):
                    coluna_resolvida = coluna
                    break
            origem = self._normalizar_origem_coluna(coluna_resolvida)

        regra: dict[str, Any] = {
            "origem": origem,
            "destino": destino,
            "obrigatorio": bool(self.int_map_obrigatorio_var.get()),
            "ativo": bool(self.int_map_ativo_var.get()),
        }

        if padrao:
            regra["padrao"] = padrao

        transformacao = (self.int_map_transformacao_var.get() or "").strip()
        if transformacao:
            regra["transformacao"] = transformacao

        regras = [dict(item) for item in (endpoint.de_para or []) if isinstance(item, dict)]
        if self.map_selected_index is not None and 0 <= self.map_selected_index < len(regras):
            regras[self.map_selected_index] = regra
        else:
            regras.append(regra)

        endpoint.de_para = regras
        self._render_endpoint_tree()
        self._render_de_para_tree()
        self._limpar_de_para_form()

    def _remover_de_para_form(self) -> None:
        endpoint = self._endpoint_atual_selecionado()
        if endpoint is None:
            raise ValueError("Selecione um endpoint para remover o de-para.")
        if self.map_selected_index is None:
            raise ValueError("Selecione um de-para para remover.")

        regras = [dict(item) for item in (endpoint.de_para or []) if isinstance(item, dict)]
        if 0 <= self.map_selected_index < len(regras):
            del regras[self.map_selected_index]
        endpoint.de_para = regras
        self._render_endpoint_tree()
        self._render_de_para_tree()
        self._limpar_de_para_form()

    def _atualizar_opcoes_cliente_api(self, *, active_id: str | None = None) -> None:
        opcoes: list[str] = []
        mapa: dict[str, str] = {}
        selecionado = ""

        for item in self.integracao_items:
            rotulo = f"{item.nome} [{item.id[:8]}]"
            opcoes.append(rotulo)
            mapa[rotulo] = item.id
            if active_id and item.id == active_id:
                selecionado = rotulo

        self._api_cliente_opcoes = mapa

        if hasattr(self, "api_cliente_combo") and self.api_cliente_combo.winfo_exists():
            self.api_cliente_combo.configure(values=opcoes)

        if selecionado:
            self.api_cliente_switch_var.set(selecionado)
        elif opcoes:
            atual = str(self.api_cliente_switch_var.get() or "").strip()
            if atual not in mapa:
                self.api_cliente_switch_var.set(opcoes[0])
        else:
            self.api_cliente_switch_var.set("")

    def _ativar_cliente_api_por_selecao(self) -> None:
        if not self.integracao_items:
            raise ValueError("Nao ha clientes/API cadastrados.")

        rotulo = str(self.api_cliente_switch_var.get() or "").strip()
        if not rotulo:
            raise ValueError("Selecione um perfil de cliente/API.")

        cliente_id = self._api_cliente_opcoes.get(rotulo)
        if not cliente_id:
            raise ValueError("Perfil selecionado nao encontrado.")

        self.registry.set_active(cliente_id)
        self._carregar_configs_integracao(log_line=True)
        self._set_status("Status: cliente/API ativo atualizado")
        self._log_api(
            "Cliente/API ativo alterado. "
            "Se os servicos API estiverem rodando, a troca entra no proximo ciclo."
        )

    def _carregar_configs_integracao(self, *, log_line: bool = False) -> None:
        self.integracao_items = self.registry.list_configs()
        active_id = self.registry.get_active_id()
        active = self.registry.get_active()
        label_ativo = f"Cliente API ativo: {active.nome}" if active else "Cliente API ativo: padrao (.env)"
        self.cliente_api_ativo_var.set(label_ativo)
        self._atualizar_opcoes_cliente_api(active_id=active_id)

        if not self.integracao_items and not self.integracao_selected_id:
            default_cfg = self.registry.default_config()
            self.int_nome_var.set(default_cfg.nome)
            self.int_fornecedor_var.set(default_cfg.fornecedor)
            self.int_login_url_var.set(default_cfg.login_url)
            self.int_base_url_var.set(default_cfg.base_url)
            self.int_usuario_var.set(default_cfg.usuario)
            self.int_senha_var.set(default_cfg.senha)
            self.current_endpoints = list(default_cfg.endpoints or [])
            self._render_endpoint_tree()
            self.int_timeout_var.set(str(default_cfg.timeout_seconds))

        if hasattr(self, "integracao_tree"):
            self.integracao_tree.delete(*self.integracao_tree.get_children())
            for item in self.integracao_items:
                ativo = "Sim" if item.id == active_id else "Nao"
                tipos = ", ".join(sorted({(ep.tipo or "").strip() for ep in item.endpoints if (ep.tipo or "").strip()}))
                self.integracao_tree.insert(
                    "",
                    tk.END,
                    iid=item.id,
                    values=(
                        item.nome,
                        item.fornecedor,
                        ativo,
                        item.login_url,
                        item.base_url,
                        item.usuario,
                        len(item.endpoints),
                        tipos,
                        item.timeout_seconds,
                    ),
                )

            if self.integracao_selected_id and self.integracao_tree.exists(self.integracao_selected_id):
                self.integracao_tree.selection_set(self.integracao_selected_id)
                self.integracao_tree.focus(self.integracao_selected_id)
                self._on_integracao_item_select()
            elif active_id and self.integracao_tree.exists(active_id):
                self.integracao_tree.selection_set(active_id)
                self.integracao_tree.focus(active_id)
                self._on_integracao_item_select()

        if log_line:
            self._log_api(f"Clientes/API carregados: {len(self.integracao_items)}. Ativo: {active.nome if active else 'padrao (.env)'}")

    def _config_api_ativa(self) -> dict[str, Any] | None:
        active = self.registry.get_active()
        return active.to_runtime_dict() if active else None

    def _garantir_cadastro_api_padrao(self) -> None:
        try:
            items = self.registry.list_configs()
            nomes_ats = {"ats (padrao .env)", "ats", "padrao .env"}
            default_cfg = self.registry.default_config()
            defaults_por_tipo: dict[str, list[dict[str, Any]]] = {}
            for ep in (default_cfg.endpoints or []):
                tipo_norm = self._normalizar_tipo_endpoint(ep.tipo)
                defaults_por_tipo[tipo_norm] = [dict(item) for item in (ep.de_para or []) if isinstance(item, dict)]

            ats_items = [
                x for x in items
                if str(x.nome or "").strip().lower() in nomes_ats
            ]
            if ats_items:
                for ats_cfg in ats_items:
                    changed = False
                    for ep in (ats_cfg.endpoints or []):
                        tipo_norm = self._normalizar_tipo_endpoint(ep.tipo)
                        if ep.de_para:
                            continue
                        default_map = defaults_por_tipo.get(tipo_norm) or []
                        if not default_map:
                            continue
                        ep.de_para = [dict(item) for item in default_map]
                        changed = True
                    if changed:
                        self.registry.upsert(ats_cfg)
                return

            saved = self.registry.upsert(default_cfg)
            if not self.registry.get_active_id():
                self.registry.set_active(saved.id)
        except Exception:
            return

    def _run_async(self, target, *, channel: str = "sync") -> None:
        with self._busy_lock:
            channel_busy = self._busy_api if channel == "api" else self._busy_sync
        if channel_busy > 0:
            if channel == "api":
                self._log_api("Acao ignorada: ja existe uma operacao API em andamento.", level="WARN")
            else:
                self._log_sync("Acao ignorada: ja existe uma operacao de sincronizacao em andamento.", level="WARN")
            return
        thread = threading.Thread(target=self._safe_call, args=(target, channel), daemon=True)
        thread.start()

    def _safe_call(self, target, channel: str) -> None:
        self._set_busy(channel, True)
        try:
            target()
        except Exception as exc:
            self._set_status(f"Status: erro - {exc}")
            if channel == "api":
                self._log_api(f"{exc}", level="ERROR")
            else:
                self._log_sync(f"{exc}", level="ERROR")
        finally:
            self._set_busy(channel, False)

    def _clear_text(self, widget: ScrolledText) -> None:
        widget.delete("1.0", tk.END)

    def _append_log(self, widget: ScrolledText, message: str, *, level: str = "INFO") -> None:
        if self._closing:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [{level}] {message}"

        def _append() -> None:
            widget.insert(tk.END, f"{line}\n")
            widget.see(tk.END)

        try:
            self.after(0, _append)
        except tk.TclError:
            pass

    def _log_sync(self, message: str, *, level: str = "INFO") -> None:
        self._append_log(self.output_text_sync, message, level=level)

    def _log_api(self, message: str, *, level: str = "INFO") -> None:
        self._append_log(self.output_text_api, message, level=level)

    def _set_busy(self, channel: str, busy: bool) -> None:
        with self._busy_lock:
            if channel == "api":
                self._busy_api = max(0, self._busy_api + (1 if busy else -1))
            else:
                self._busy_sync = max(0, self._busy_sync + (1 if busy else -1))
            is_busy = (self._busy_sync + self._busy_api) > 0

        sync_text = "SYNC: executando" if self._busy_sync > 0 else "SYNC: ocioso"
        api_text = "API: executando" if self._busy_api > 0 else "API: ocioso"
        sync_style = "StatusBusy.TLabel" if self._busy_sync > 0 else "StatusIdle.TLabel"
        api_style = "StatusBusy.TLabel" if self._busy_api > 0 else "StatusIdle.TLabel"
        try:
            self.after(0, lambda: self.sync_exec_status_var.set(sync_text))
            self.after(0, lambda: self.api_exec_status_var.set(api_text))
            self.after(0, lambda: self.sync_status_label.configure(style=sync_style))
            self.after(0, lambda: self.api_status_label.configure(style=api_style))
        except tk.TclError:
            pass

        self._apply_busy_ui(is_busy)
        self._update_progress(is_busy)

    def _apply_busy_ui(self, is_busy: bool) -> None:
        if self._closing:
            return

        def _apply() -> None:
            try:
                self.configure(cursor="watch" if is_busy else "")
            except Exception:
                pass

        try:
            self.after(0, _apply)
        except tk.TclError:
            pass

    def _update_progress(self, is_busy: bool) -> None:
        if self._closing:
            return
        try:
            self.after(0, lambda: self._update_progress_main_thread(is_busy))
        except tk.TclError:
            pass

    def _update_progress_main_thread(self, is_busy: bool) -> None:
        if self._closing:
            return

        if is_busy:
            if not self._progress_active:
                self._progress_active = True
                self.progress_text_var.set("Progresso: executando...")
                self.progress_value_var.set(8.0)
                self._pulse_progress()
            return

        if not self._progress_active:
            return

        self._progress_active = False
        if self._progress_job is not None:
            try:
                self.after_cancel(self._progress_job)
            except Exception:
                pass
            self._progress_job = None

        self.progress_value_var.set(100.0)
        self.progress_text_var.set("Progresso: concluido")
        self.after(500, self._reset_progress_idle)

    def _pulse_progress(self) -> None:
        if self._closing or not self._progress_active:
            return

        value = float(self.progress_value_var.get())
        value = value + 4.0 if value < 92.0 else 18.0
        self.progress_value_var.set(value)
        self._progress_job = self.after(120, self._pulse_progress)

    def _reset_progress_idle(self) -> None:
        if self._closing or self._progress_active:
            return
        self.progress_value_var.set(0.0)
        self.progress_text_var.set("Progresso: ocioso")

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
        unico_m = str(settings.win_service_api_motoristas or "").strip()
        unico_a = str(settings.win_service_api_afastamentos or "").strip()
        if (ambiente or "").strip().lower() == "producao":
            default_m = settings.win_service_api_motoristas_prod
            default_a = settings.win_service_api_afastamentos_prod
        else:
            default_m = settings.win_service_api_motoristas_dev
            default_a = settings.win_service_api_afastamentos_dev
        if unico_m or unico_a:
            return (unico_m or default_m, unico_a or default_a)
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
        cfg = self._config_api_ativa()
        timeout_seconds = float((cfg or {}).get("timeout_seconds") or settings.api_timeout_seconds)
        auth = login_api(timeout=timeout_seconds, config=cfg)
        self.token = auth.get("token")
        exp = auth.get("exp")
        self._set_status("Status: autenticado")
        nome_cliente = str((cfg or {}).get("nome") or "padrao (.env)")
        self._log_api(f"Login API ok ({nome_cliente}). Expira em: {exp}")

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
        self._executar_api(tipos_permitidos={"motoristas", "motorista"})

    def _executar_api_afastamentos(self) -> None:
        self._executar_api(tipos_permitidos={"afastamentos", "afastamento"})

    def _executar_api_ambos(self) -> None:
        self._executar_api(tipos_permitidos=set())

    def _executar_api(self, *, tipos_permitidos: set[str]) -> None:
        self._set_status("Status: executando envio para API...")
        self._ensure_engine_destino()
        cfg = self._config_api_ativa()
        timeout_seconds = float((cfg or {}).get("timeout_seconds") or settings.api_timeout_seconds)
        endpoints = self._listar_endpoints_ativos(cfg)
        if tipos_permitidos:
            endpoints = [ep for ep in endpoints if str(ep.get("tipo_normalizado") or "") in tipos_permitidos]

        if not endpoints:
            raise ValueError("Nenhum endpoint ativo compativel com o tipo selecionado.")

        total_ok_m = 0
        total_ok_a = 0
        total_err_m = 0
        total_err_a = 0
        endpoints_processados = 0

        for ep in endpoints:
            tipo_norm = str(ep.get("tipo_normalizado") or "")
            processa_m = tipo_norm in {"motoristas", "motorista"}
            processa_a = tipo_norm in {"afastamentos", "afastamento"}
            if not processa_m and not processa_a:
                self._log_api(
                    f"[API] Endpoint ignorado por tipo sem mapeamento: tipo={ep.get('tipo')} path={ep.get('endpoint')}",
                    level="WARN",
                )
                continue

            tabela_custom = str(ep.get("tabela_destino") or "").strip()
            tabela_motorista = tabela_custom if processa_m and tabela_custom else settings.target_motorista_table
            tabela_afastamento = tabela_custom if processa_a and tabela_custom else settings.target_afastamento_table

            service = ApiDispatchService(
                engine_destino=self.engine_destino,
                schema_destino=settings.target_schema,
                tabela_motorista=tabela_motorista,
                tabela_afastamento=tabela_afastamento,
                endpoint_motorista=str(ep.get("endpoint") or settings.api_motorista_endpoint),
                endpoint_afastamento=str(ep.get("endpoint") or settings.api_afastamento_endpoint),
                batch_size_motoristas=self._get_batch_api_motoristas(),
                batch_size_afastamentos=self._get_batch_api_afastamentos(),
                max_tentativas=settings.api_sync_max_tentativas,
                lock_timeout_minutes=settings.api_sync_lock_timeout_minutes,
                retry_base_seconds=settings.api_sync_retry_base_seconds,
                retry_max_seconds=settings.api_sync_retry_max_seconds,
                api_timeout_seconds=timeout_seconds,
                processar_motoristas=processa_m,
                processar_afastamentos=processa_a,
                integration_config=cfg,
                payload_mapping=ep.get("de_para"),
            )

            try:
                resultado = service.executar_ciclo()
            finally:
                service.close()

            total_ok_m += int(resultado.motoristas_sucesso or 0)
            total_ok_a += int(resultado.afastamentos_sucesso or 0)
            total_err_m += int(resultado.motoristas_erro or 0)
            total_err_a += int(resultado.afastamentos_erro or 0)
            endpoints_processados += 1
            self._log_api(
                f"[API] Endpoint tipo={ep.get('tipo')} path={ep.get('endpoint')} "
                f"OkM={resultado.motoristas_sucesso} ErrM={resultado.motoristas_erro} "
                f"OkA={resultado.afastamentos_sucesso} ErrA={resultado.afastamentos_erro}"
            )

        if endpoints_processados == 0:
            raise ValueError("Nenhum endpoint com tipo mapeado para envio (motoristas/afastamentos).")
        self._log_api(f"[API] Consolidado: OkM={total_ok_m} ErrM={total_err_m} OkA={total_ok_a} ErrA={total_err_a}")
        self._set_status("Status: envio API finalizado")
        self._atualizar_monitor_api(log_line=False)
        self._atualizar_lista_integracao(log_line=False)

    @staticmethod
    def _normalizar_tipo_endpoint(value: str) -> str:
        text = str(value or "").strip().lower()
        if "afast" in text:
            return "afastamentos"
        if "motor" in text:
            return "motoristas"
        return text

    def _listar_endpoints_ativos(self, cfg: dict[str, Any] | None) -> list[dict[str, Any]]:
        runtime_cfg = cfg or {}
        endpoints = runtime_cfg.get("endpoints") or []
        result: list[dict[str, Any]] = []
        for ep in endpoints:
            if not isinstance(ep, dict):
                continue
            if not bool(ep.get("ativo", True)):
                continue
            endpoint_path = str(ep.get("endpoint") or "").strip()
            tipo = str(ep.get("tipo") or "").strip()
            if not endpoint_path or not tipo:
                continue
            result.append(
                {
                    "tipo": tipo,
                    "tipo_normalizado": self._normalizar_tipo_endpoint(tipo),
                    "endpoint": endpoint_path,
                    "tabela_destino": str(ep.get("tabela_destino") or "").strip(),
                    "de_para": [dict(item) for item in (ep.get("de_para") or []) if isinstance(item, dict)],
                }
            )

        if result:
            return result

        # Fallback para configuracao antiga/fixa
        return [
            {
                "tipo": "motoristas",
                "tipo_normalizado": "motoristas",
                "endpoint": str(settings.api_motorista_endpoint),
                "tabela_destino": str(settings.target_motorista_table),
                "de_para": [],
            },
            {
                "tipo": "afastamentos",
                "tipo_normalizado": "afastamentos",
                "endpoint": str(settings.api_afastamento_endpoint),
                "tabela_destino": str(settings.target_afastamento_table),
                "de_para": [],
            },
        ]

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
        if self._progress_job is not None:
            try:
                self.after_cancel(self._progress_job)
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
