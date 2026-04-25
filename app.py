import os
import traceback
from datetime import datetime
from io import StringIO

import dash
from dash import dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
from dash.dcc import send_data_frame

import pandas as pd
import plotly.express as px

import folium
from folium.plugins import MarkerCluster

from flask import request, session, redirect, url_for, render_template_string
from sqlalchemy import create_engine, text

from gerenciador_usuarios import (
    criar_tabelas_auth,
    criar_admin_inicial,
    autenticar_usuario,
    validar_sessao,
    encerrar_sessao,
    listar_usuarios,
    criar_usuario,
    listar_logs_acesso,
    ativar_usuario,
    desativar_usuario,
    resetar_senha,
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL não encontrada. Configure essa variável no Railway."
    )

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

ARQ_MAPA_HTML = "/tmp/mapa.html"
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Sao_Paulo")


# ============================================================
# INICIALIZAÇÃO DO BANCO
# ============================================================

def criar_tabela_pop_rua():
    sql = """
    CREATE TABLE IF NOT EXISTS pop_rua (
        id SERIAL PRIMARY KEY,
        titulo TEXT,
        url TEXT UNIQUE,
        municipio TEXT,
        uf VARCHAR(2),
        categoria TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        data_coleta TIMESTAMP ,
        data_publicacao TIMESTAMP NULL,
        query_origem TEXT,
        criado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_pop_rua_municipio ON pop_rua (municipio);
    CREATE INDEX IF NOT EXISTS ix_pop_rua_uf ON pop_rua (uf);
    CREATE INDEX IF NOT EXISTS ix_pop_rua_categoria ON pop_rua (categoria);
    CREATE INDEX IF NOT EXISTS ix_pop_rua_data_coleta ON pop_rua (data_coleta);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


def inicializar_banco():
    try:
        criar_tabelas_auth()
        criar_admin_inicial()
        criar_tabela_pop_rua()
        print("✅ Banco inicializado com sucesso.", flush=True)
    except Exception as e:
        print(f"⚠️ Falha ao inicializar banco: {e}", flush=True)
        traceback.print_exc()


inicializar_banco()


# ============================================================
# APP DASH
# ============================================================

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Painel Pop Rua"
)

server = app.server
server.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")



# ============================================================
# GERENCIADOR DE USUÁRIOS VIA FLASK
# ============================================================
# Esta área evita depender de callbacks dinâmicos do Dash para criar usuários.
# O Dashboard continua igual; apenas a gestão de usuários passa a usar rotas Flask
# protegidas por sessão server-side.

ADMIN_CSS = """
<style>
    body {
        margin: 0;
        font-family: Arial, sans-serif;
        background: #f3f4f6;
        color: #111827;
    }
    .wrap {
        max-width: 1180px;
        margin: 30px auto;
        padding: 0 18px;
    }
    .card {
        background: white;
        border-radius: 16px;
        box-shadow: 0 4px 14px rgba(0,0,0,0.08);
        padding: 22px;
        margin-bottom: 18px;
    }
    .topbar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
        margin-bottom: 18px;
    }
    h1, h2, h3 { margin: 0; }
    .muted { color: #6b7280; font-size: 14px; margin-top: 6px; }
    .grid {
        display: grid;
        grid-template-columns: 1.2fr 1.5fr 1fr 1fr auto;
        gap: 10px;
        align-items: end;
    }
    label {
        display: block;
        font-size: 13px;
        font-weight: 700;
        color: #374151;
        margin-bottom: 6px;
    }
    input, select {
        width: 100%;
        height: 40px;
        border: 1px solid #d1d5db;
        border-radius: 10px;
        padding: 0 10px;
        box-sizing: border-box;
    }
    button, .btn {
        border: none;
        border-radius: 10px;
        padding: 11px 15px;
        cursor: pointer;
        font-weight: 700;
        text-decoration: none;
        display: inline-block;
        font-size: 14px;
    }
    .btn-primary { background: #2563eb; color: white; }
    .btn-dark { background: #1f2937; color: white; }
    .btn-danger { background: #b91c1c; color: white; }
    .btn-light { background: #e5e7eb; color: #111827; }
    .btn-warning { background: #f59e0b; color: #111827; }
    .msg {
        padding: 12px 14px;
        border-radius: 12px;
        margin-bottom: 16px;
        font-weight: 600;
    }
    .msg.ok { background: #ecfdf5; color: #047857; border: 1px solid #a7f3d0; }
    .msg.err { background: #fef2f2; color: #b91c1c; border: 1px solid #fecaca; }
    table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    th, td {
        text-align: left;
        padding: 10px;
        border-bottom: 1px solid #e5e7eb;
        vertical-align: middle;
    }
    th {
        background: #f9fafb;
        color: #374151;
        font-weight: 800;
    }
    .pill {
        padding: 4px 8px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 700;
        display: inline-block;
    }
    .pill.ok { background: #dcfce7; color: #166534; }
    .pill.no { background: #fee2e2; color: #991b1b; }
    .actions {
        display: flex;
        gap: 6px;
        flex-wrap: wrap;
    }
    .actions form { margin: 0; display: inline-flex; gap: 6px; }
    .login-card {
        max-width: 390px;
        margin: 90px auto;
    }
    @media (max-width: 900px) {
        .grid { grid-template-columns: 1fr; }
        .topbar { flex-direction: column; align-items: flex-start; }
        table { font-size: 12px; }
    }
</style>
"""


def admin_usuario_atual():
    token = session.get("admin_token")
    if not token:
        return None

    usuario = obter_usuario_por_token(token)
    if not usuario or usuario.get("perfil") != "admin":
        return None

    return usuario


def admin_redirect_se_necessario():
    if not admin_usuario_atual():
        return redirect(url_for("admin_login"))
    return None


@server.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    erro = ""

    if request.method == "POST":
        email = request.form.get("email")
        senha = request.form.get("senha")

        resultado = autenticar_usuario(
            email=email,
            senha=senha,
            ip=obter_ip_requisicao(),
            user_agent=obter_user_agent()
        )

        if resultado.get("ok") and resultado.get("usuario", {}).get("perfil") == "admin":
            session["admin_token"] = resultado["token_sessao"]
            return redirect(url_for("admin_usuarios"))

        if resultado.get("ok") and resultado.get("token_sessao"):
            encerrar_sessao(resultado["token_sessao"])

        erro = "Acesso negado. Use um usuário com perfil admin."

    return render_template_string(
        """
        <!doctype html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8">
            <title>Login Admin - Painel Pop Rua</title>
            {{ css|safe }}
        </head>
        <body>
            <div class="wrap">
                <div class="card login-card">
                    <h1>Painel Pop Rua</h1>
                    <p class="muted">Área administrativa de usuários</p>

                    {% if erro %}
                        <div class="msg err">{{ erro }}</div>
                    {% endif %}

                    <form method="post">
                        <label>E-mail</label>
                        <input name="email" type="email" required autocomplete="username">

                        <br><br>

                        <label>Senha</label>
                        <input name="senha" type="password" required autocomplete="current-password">

                        <br><br>

                        <button class="btn btn-dark" type="submit" style="width:100%;">Entrar</button>
                    </form>

                    <p class="muted" style="margin-top:18px;">
                        Voltar para o dashboard:
                        <a href="/" style="color:#2563eb;font-weight:700;">abrir painel</a>
                    </p>
                </div>
            </div>
        </body>
        </html>
        """,
        css=ADMIN_CSS,
        erro=erro
    )


@server.route("/admin/logout", methods=["POST", "GET"])
def admin_logout():
    token = session.get("admin_token")
    if token:
        encerrar_sessao(token)
    session.pop("admin_token", None)
    return redirect(url_for("admin_login"))


@server.route("/admin/usuarios", methods=["GET", "POST"])
def admin_usuarios():
    redir = admin_redirect_se_necessario()
    if redir:
        return redir

    usuario_admin = admin_usuario_atual()
    msg = ""
    msg_tipo = "ok"

    if request.method == "POST":
        nome = str(request.form.get("nome") or "").strip()
        email = str(request.form.get("email") or "").strip().lower()
        senha = str(request.form.get("senha") or "")
        perfil = str(request.form.get("perfil") or "usuario").strip().lower()

        try:
            novo_id = criar_usuario(
                nome=nome,
                email=email,
                senha=senha,
                perfil=perfil,
                primeiro_acesso=True
            )
            msg = f"Usuário criado com sucesso. ID: {novo_id}."
            msg_tipo = "ok"
        except Exception as e:
            msg = f"Erro ao criar usuário: {e}"
            msg_tipo = "err"

    usuarios = listar_usuarios()
    logs = listar_logs_acesso(limit=20)

    return render_template_string(
        """
        <!doctype html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8">
            <title>Usuários - Painel Pop Rua</title>
            {{ css|safe }}
        </head>
        <body>
            <div class="wrap">
                <div class="topbar">
                    <div>
                        <h1>Gerenciamento de usuários</h1>
                        <p class="muted">
                            Logado como <strong>{{ admin.nome }}</strong> — {{ admin.email }}
                        </p>
                    </div>

                    <div>
                        <a class="btn btn-light" href="/">Dashboard</a>
                        <a class="btn btn-light" href="{{ url_for('admin_logs') }}">Logs</a>
                        <a class="btn btn-danger" href="{{ url_for('admin_logout') }}">Sair</a>
                    </div>
                </div>

                {% if msg %}
                    <div class="msg {{ msg_tipo }}">{{ msg }}</div>
                {% endif %}

                <div class="card">
                    <h2>Criar novo usuário</h2>
                    <p class="muted">Atenção na criação do usuário.</p>

                    <form method="post" class="grid">
                        <div>
                            <label>Nome</label>
                            <input name="nome" type="text" required>
                        </div>

                        <div>
                            <label>E-mail</label>
                            <input name="email" type="email" required>
                        </div>

                        <div>
                            <label>Senha inicial</label>
                            <input name="senha" type="password" required minlength="6">
                        </div>

                        <div>
                            <label>Perfil</label>
                            <select name="perfil">
                                <option value="usuario">Usuário</option>
                                <option value="visualizador">Visualizador</option>
                                <option value="gestor">Gestor</option>
                                <option value="admin">Admin</option>
                            </select>
                        </div>

                        <div>
                            <button class="btn btn-primary" type="submit">Criar usuário</button>
                        </div>
                    </form>
                </div>

                <div class="card">
                    <h2>Usuários cadastrados</h2>
                    <p class="muted">Total: {{ usuarios|length }}</p>

                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Nome</th>
                                <th>E-mail</th>
                                <th>Perfil</th>
                                <th>Ativo</th>
                                <th>Primeiro acesso</th>
                                <th>Senha expirada</th>
                                <th>Último login</th>
                                <th>Ações</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for u in usuarios %}
                                <tr>
                                    <td>{{ u.id }}</td>
                                    <td>{{ u.nome }}</td>
                                    <td>{{ u.email }}</td>
                                    <td>{{ u.perfil }}</td>
                                    <td>
                                        {% if u.ativo %}
                                            <span class="pill ok">ativo</span>
                                        {% else %}
                                            <span class="pill no">inativo</span>
                                        {% endif %}
                                    </td>
                                    <td>{{ u.primeiro_acesso }}</td>
                                    <td>{{ u.senha_expirada }}</td>
                                    <td>{{ u.ultimo_login or "" }}</td>
                                    <td>
                                        <div class="actions">
                                            {% if u.ativo %}
                                                <form method="post" action="{{ url_for('admin_desativar_usuario', usuario_id=u.id) }}">
                                                    <button class="btn btn-warning" type="submit">Desativar</button>
                                                </form>
                                            {% else %}
                                                <form method="post" action="{{ url_for('admin_ativar_usuario', usuario_id=u.id) }}">
                                                    <button class="btn btn-primary" type="submit">Ativar</button>
                                                </form>
                                            {% endif %}

                                            <form method="post" action="{{ url_for('admin_resetar_senha_usuario', usuario_id=u.id) }}">
                                                <input name="senha_temporaria" type="password" placeholder="Nova senha" minlength="6" required style="width:120px;height:36px;">
                                                <button class="btn btn-light" type="submit">Reset</button>
                                            </form>
                                        </div>
                                    </td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>

                <div class="card">
                    <h2>Últimos logs de acesso</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Data</th>
                                <th>E-mail</th>
                                <th>Sucesso</th>
                                <th>Motivo</th>
                                <th>IP</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for l in logs %}
                                <tr>
                                    <td>{{ l.criado_em }}</td>
                                    <td>{{ l.email }}</td>
                                    <td>{{ l.sucesso }}</td>
                                    <td>{{ l.motivo }}</td>
                                    <td>{{ l.ip or "" }}</td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """,
        css=ADMIN_CSS,
        admin=usuario_admin,
        usuarios=usuarios,
        logs=logs,
        msg=msg,
        msg_tipo=msg_tipo
    )


@server.route("/admin/logs", methods=["GET"])
def admin_logs():
    redir = admin_redirect_se_necessario()
    if redir:
        return redir

    logs = listar_logs_acesso(limit=300)

    return render_template_string(
        """
        <!doctype html>
        <html lang="pt-br">
        <head>
            <meta charset="utf-8">
            <title>Logs - Painel Pop Rua</title>
            {{ css|safe }}
        </head>
        <body>
            <div class="wrap">
                <div class="topbar">
                    <div>
                        <h1>Logs de acesso</h1>
                        <p class="muted">Últimos 300 registros.</p>
                    </div>

                    <div>
                        <a class="btn btn-light" href="{{ url_for('admin_usuarios') }}">Usuários</a>
                        <a class="btn btn-light" href="/">Dashboard</a>
                        <a class="btn btn-danger" href="{{ url_for('admin_logout') }}">Sair</a>
                    </div>
                </div>

                <div class="card">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Usuário ID</th>
                                <th>E-mail</th>
                                <th>Sucesso</th>
                                <th>Motivo</th>
                                <th>IP</th>
                                <th>User Agent</th>
                                <th>Criado em</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for l in logs %}
                                <tr>
                                    <td>{{ l.id }}</td>
                                    <td>{{ l.usuario_id or "" }}</td>
                                    <td>{{ l.email }}</td>
                                    <td>{{ l.sucesso }}</td>
                                    <td>{{ l.motivo }}</td>
                                    <td>{{ l.ip or "" }}</td>
                                    <td>{{ l.user_agent or "" }}</td>
                                    <td>{{ l.criado_em }}</td>
                                </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """,
        css=ADMIN_CSS,
        logs=logs
    )


@server.route("/admin/usuarios/<int:usuario_id>/desativar", methods=["POST"])
def admin_desativar_usuario(usuario_id):
    redir = admin_redirect_se_necessario()
    if redir:
        return redir

    try:
        desativar_usuario(usuario_id)
    except Exception as e:
        print(f"Erro ao desativar usuário {usuario_id}: {e}", flush=True)

    return redirect(url_for("admin_usuarios"))


@server.route("/admin/usuarios/<int:usuario_id>/ativar", methods=["POST"])
def admin_ativar_usuario(usuario_id):
    redir = admin_redirect_se_necessario()
    if redir:
        return redir

    try:
        ativar_usuario(usuario_id)
    except Exception as e:
        print(f"Erro ao ativar usuário {usuario_id}: {e}", flush=True)

    return redirect(url_for("admin_usuarios"))


@server.route("/admin/usuarios/<int:usuario_id>/resetar-senha", methods=["POST"])
def admin_resetar_senha_usuario(usuario_id):
    redir = admin_redirect_se_necessario()
    if redir:
        return redir

    senha_temporaria = request.form.get("senha_temporaria")

    try:
        resetar_senha(usuario_id, senha_temporaria)
    except Exception as e:
        print(f"Erro ao resetar senha do usuário {usuario_id}: {e}", flush=True)

    return redirect(url_for("admin_usuarios"))


# ============================================================
# HELPERS
# ============================================================

def log_erro(contexto, erro):
    print(f"❌ ERRO EM {contexto}: {erro}", flush=True)
    traceback.print_exc()


def mensagem_erro_usuario(contexto, erro):
    return (
        f"Ocorreu um erro ao processar {contexto}. "
        f"Verifique os logs do Railway para o detalhe técnico. "
        f"Resumo: {type(erro).__name__}: {erro}"
    )


def obter_ip_requisicao():
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.remote_addr


def obter_user_agent():
    return request.headers.get("User-Agent", "")


def obter_usuario_por_token(token):
    if not token:
        return None

    try:
        return validar_sessao(token)
    except Exception as e:
        log_erro("validar_sessao", e)
        return None


def usuario_eh_admin(usuario):
    return bool(usuario and usuario.get("perfil") == "admin")


def resolver_token(token=None, token_contexto=None, usuario_store=None):
    """
    Resolve o token de sessão a partir de múltiplas fontes.

    Motivo:
    Em alguns fluxos do Dash, principalmente ao alternar abas renderizadas dinamicamente,
    o State do dcc.Store global pode chegar vazio em callbacks específicos.
    Por isso mantemos também um token_contexto dentro do layout autenticado.
    """
    if token:
        return token

    if token_contexto:
        return token_contexto

    if isinstance(usuario_store, dict):
        return usuario_store.get("token_sessao")

    return None


def formatar_numero(valor):
    try:
        return f"{int(valor):,}".replace(",", ".")
    except Exception:
        return "0"


def converter_datetime_serie(serie):
    """
    Converte datas com ou sem timezone para datetime sem timezone em America/Sao_Paulo.
    Isso evita erros de mixed timezone no Pandas/Plotly/Dash.
    """
    dt = pd.to_datetime(serie, errors="coerce", utc=True)

    try:
        dt = dt.dt.tz_convert(APP_TIMEZONE).dt.tz_localize(None)
    except Exception:
        dt = dt.dt.tz_localize(None)

    return dt


def ler_json_dataframe(dados_json):
    if not dados_json:
        return pd.DataFrame()

    return pd.read_json(StringIO(dados_json), orient="split")


# ============================================================
# BANCO DE DADOS - POP RUA
# ============================================================

def carregar_dados_banco():
    sql = """
    SELECT
        id,
        titulo,
        url,
        municipio,
        uf,
        categoria,
        latitude,
        longitude,
        data_coleta,
        data_publicacao,
        query_origem,
        criado_em
    FROM pop_rua
    ORDER BY data_coleta DESC NULLS LAST, id DESC;
    """

    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        log_erro("carregar_dados_banco", e)
        df = pd.DataFrame()

    return tratar_dataframe(df)


def tratar_dataframe(df):
    df = df.copy()

    colunas_necessarias = [
        "id",
        "titulo",
        "url",
        "municipio",
        "uf",
        "categoria",
        "latitude",
        "longitude",
        "data_coleta",
        "data_publicacao",
        "query_origem",
        "criado_em"
    ]

    for coluna in colunas_necessarias:
        if coluna not in df.columns:
            df[coluna] = None

    for coluna in ["municipio", "uf", "categoria", "titulo", "url", "query_origem"]:
        df[coluna] = df[coluna].fillna("").astype(str).str.strip()

    df["municipio"] = df["municipio"].replace("", "Não identificado")
    df["uf"] = df["uf"].replace("", "NI")
    df["categoria"] = df["categoria"].replace("", "Outros")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df["data_coleta"] = converter_datetime_serie(df["data_coleta"])
    df["data_publicacao"] = converter_datetime_serie(df["data_publicacao"])
    df["criado_em"] = converter_datetime_serie(df["criado_em"])

    df["data"] = df["data_publicacao"].fillna(df["data_coleta"])
    df["quantidade"] = 1

    return df


# ============================================================
# MAPA E GRÁFICOS
# ============================================================

def gerar_mapa(df):
    mapa = folium.Map(
        location=[-14.2350, -51.9253],
        zoom_start=4,
        tiles="OpenStreetMap"
    )

    cluster = MarkerCluster().add_to(mapa)

    if not df.empty:
        for _, row in df.iterrows():
            latitude = row.get("latitude")
            longitude = row.get("longitude")

            if pd.isna(latitude) or pd.isna(longitude):
                continue

            municipio = row.get("municipio", "Não identificado")
            uf = row.get("uf", "NI")
            categoria = row.get("categoria", "Outros")
            quantidade = row.get("quantidade", 1)

            popup = f"""
            <b>Município:</b> {municipio}/{uf}<br>
            <b>Categoria:</b> {categoria}<br>
            <b>Quantidade:</b> {quantidade}
            """

            folium.CircleMarker(
                location=[float(latitude), float(longitude)],
                radius=min(float(quantidade) * 2.5, 18),
                fill=True,
                fill_opacity=0.7,
                popup=folium.Popup(popup, max_width=300)
            ).add_to(cluster)

    mapa.save(ARQ_MAPA_HTML)

    with open(ARQ_MAPA_HTML, encoding="utf-8") as f:
        return f.read()


def criar_figura_vazia(titulo):
    fig = px.scatter(title=titulo)
    fig.update_layout(
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": "Sem dados para exibir",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 16}
            }
        ]
    )
    return fig


def card_resumo(titulo, valor, subtitulo=None):
    return html.Div(
        [
            html.Div(titulo, style={"fontSize": "13px", "color": "#666", "marginBottom": "6px"}),
            html.Div(valor, style={"fontSize": "26px", "fontWeight": "700", "color": "#222"}),
            html.Div(subtitulo or "", style={"fontSize": "12px", "color": "#777", "marginTop": "4px"})
        ],
        style={
            "backgroundColor": "#ffffff",
            "padding": "18px",
            "borderRadius": "14px",
            "boxShadow": "0 4px 14px rgba(0,0,0,0.08)",
            "minWidth": "180px",
            "flex": "1"
        }
    )


def aplicar_filtros(df, ufs, municipios, categorias, data_ini, data_fim, texto_busca):
    df = df.copy()

    if ufs:
        df = df[df["uf"].isin(ufs)]

    if municipios:
        df = df[df["municipio"].isin(municipios)]

    if categorias:
        df = df[df["categoria"].isin(categorias)]

    if data_ini:
        data_ini = pd.to_datetime(data_ini, errors="coerce")
        if pd.notna(data_ini):
            df = df[df["data"] >= data_ini]

    if data_fim:
        data_fim = pd.to_datetime(data_fim, errors="coerce")
        if pd.notna(data_fim):
            data_fim = data_fim + pd.Timedelta(days=1)
            df = df[df["data"] < data_fim]

    if texto_busca:
        texto = str(texto_busca).lower().strip()

        if texto:
            mascara = (
                df["titulo"].astype(str).str.lower().str.contains(texto, na=False, regex=False)
                | df["municipio"].astype(str).str.lower().str.contains(texto, na=False, regex=False)
                | df["categoria"].astype(str).str.lower().str.contains(texto, na=False, regex=False)
                | df["query_origem"].astype(str).str.lower().str.contains(texto, na=False, regex=False)
            )
            df = df[mascara]

    return df


# ============================================================
# LAYOUTS
# ============================================================

def layout_login():
    return html.Div(
        [
            html.Div(
                [
                    html.H1("Painel Pop Rua", style={"margin": "0", "fontSize": "30px", "color": "#111827"}),
                    html.P(
                        "Acesse com seu usuário.",
                        style={"marginTop": "8px", "color": "#6b7280"}
                    ),

                    html.Label("E-mail", style={"fontWeight": "600", "color": "#374151"}),
                    dcc.Input(
                        id="login_email",
                        type="email",
                        placeholder="seu.email@dominio.com",
                        autoComplete="username",
                        style={
                            "width": "100%",
                            "height": "42px",
                            "border": "1px solid #d1d5db",
                            "borderRadius": "10px",
                            "padding": "0 12px",
                            "marginTop": "6px",
                            "marginBottom": "14px",
                            "boxSizing": "border-box"
                        }
                    ),

                    html.Label("Senha", style={"fontWeight": "600", "color": "#374151"}),
                    dcc.Input(
                        id="login_senha",
                        type="password",
                        placeholder="Digite sua senha",
                        autoComplete="current-password",
                        style={
                            "width": "100%",
                            "height": "42px",
                            "border": "1px solid #d1d5db",
                            "borderRadius": "10px",
                            "padding": "0 12px",
                            "marginTop": "6px",
                            "marginBottom": "18px",
                            "boxSizing": "border-box"
                        }
                    ),

                    html.Button(
                        "Entrar",
                        id="btn_login",
                        n_clicks=0,
                        style={
                            "width": "100%",
                            "height": "44px",
                            "backgroundColor": "#1f2937",
                            "color": "white",
                            "border": "none",
                            "borderRadius": "10px",
                            "fontWeight": "700",
                            "cursor": "pointer"
                        }
                    ),

                    html.Div(
                        id="login_mensagem",
                        style={"marginTop": "14px", "fontSize": "14px", "color": "#b91c1c"}
                    )
                ],
                style={
                    "width": "380px",
                    "backgroundColor": "white",
                    "padding": "30px",
                    "borderRadius": "18px",
                    "boxShadow": "0 12px 30px rgba(0,0,0,0.12)"
                }
            )
        ],
        style={
            "minHeight": "100vh",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center",
            "background": "linear-gradient(135deg, #f3f4f6, #e5e7eb)",
            "fontFamily": "Arial, sans-serif"
        }
    )


def layout_dashboard(usuario):
    nome_usuario = usuario.get("nome", "Usuário")
    perfil = usuario.get("perfil", "usuario")

    botoes_usuario = [
        html.Div(
            [
                html.Strong(nome_usuario),
                html.Br(),
                html.Span(f"Perfil: {perfil}", style={"fontSize": "12px", "color": "#6b7280"})
            ],
            style={"textAlign": "right", "marginRight": "14px"}
        )
    ]

    if perfil == "admin":
        botoes_usuario.append(
            html.A(
                "Gerenciar usuários",
                href="/admin/usuarios",
                target="_blank",
                style={
                    "backgroundColor": "#2563eb",
                    "color": "white",
                    "border": "none",
                    "borderRadius": "10px",
                    "padding": "11px 16px",
                    "cursor": "pointer",
                    "fontWeight": "600",
                    "textDecoration": "none",
                    "display": "inline-block",
                    "marginRight": "8px"
                }
            )
        )

    botoes_usuario.append(
        html.Button(
            "Sair",
            id="btn_logout",
            n_clicks=0,
            style={
                "backgroundColor": "#b91c1c",
                "color": "white",
                "border": "none",
                "borderRadius": "10px",
                "padding": "11px 16px",
                "cursor": "pointer",
                "fontWeight": "600"
            }
        )
    )

    return html.Div(
        [
            dcc.Store(id="dados_base"),
            dcc.Store(id="dados_filtrados"),
            dcc.Store(
                id="token_contexto",
                data=usuario.get("token_sessao"),
                storage_type="memory"
            ),

            html.Div(
                [
                    html.Div(
                        [
                            html.H1(
                                "Painel População em Situação de Rua",
                                style={"margin": "0", "fontSize": "30px", "color": "#1f2937"}
                            ),
                            html.P(
                                "Monitoramento de notícias e óbitos",
                                style={"margin": "8px 0 0 0", "color": "#6b7280", "fontSize": "15px"}
                            )
                        ]
                    ),

                    html.Div(
                        botoes_usuario,
                        style={"display": "flex", "alignItems": "center"}
                    )
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "22px"
                }
            ),

            layout_tab_dashboard(perfil)
        ],
        style={
            "padding": "26px",
            "backgroundColor": "#f3f4f6",
            "minHeight": "100vh",
            "fontFamily": "Arial, sans-serif"
        }
    )



def layout_tab_dashboard(perfil="usuario"):
    return html.Div(
        [
            html.Div(
                [
                    html.Button(
                        "🔄 Recarregar dados",
                        id="btn_recarregar",
                        n_clicks=0,
                        style={
                            "backgroundColor": "#1f2937",
                            "color": "white",
                            "border": "none",
                            "borderRadius": "10px",
                            "padding": "12px 18px",
                            "cursor": "pointer",
                            "fontWeight": "600"
                        }
                    ),
                    html.Button(
                        "📰 Coletar novos dados",
                        id="btn_coletar_dados",
                        n_clicks=0,
                        style={
                            "backgroundColor": "#047857",
                            "color": "white",
                            "border": "none",
                            "borderRadius": "10px",
                            "padding": "12px 18px",
                            "cursor": "pointer",
                            "fontWeight": "600"
                        }
                    ) if perfil == "admin" else html.Button(
                        "",
                        id="btn_coletar_dados",
                        n_clicks=0,
                        style={"display": "none"}
                    )
                ],
                style={
                    "marginBottom": "14px",
                    "display": "flex",
                    "gap": "10px",
                    "flexWrap": "wrap"
                }
            ),

            html.Div(id="status_coleta", style={"marginBottom": "8px", "color": "#047857", "fontSize": "13px", "fontWeight": "600"}),
            html.Div(id="status_dados", style={"marginBottom": "18px", "color": "#6b7280", "fontSize": "13px"}),

            html.Div(
                id="cards_resumo",
                style={"display": "flex", "gap": "16px", "flexWrap": "wrap", "marginBottom": "22px"}
            ),

            html.Div(
                [
                    dcc.Dropdown(id="filtro_uf", multi=True, placeholder="Filtrar por UF", style={"width": "100%"}),
                    dcc.Dropdown(id="filtro_municipio", multi=True, placeholder="Filtrar por município", style={"width": "100%"}),
                    dcc.Dropdown(id="filtro_categoria", multi=True, placeholder="Filtrar por categoria", style={"width": "100%"}),
                    dcc.DatePickerRange(
                        id="filtro_data",
                        display_format="DD/MM/YYYY",
                        start_date_placeholder_text="Data inicial",
                        end_date_placeholder_text="Data final"
                    ),
                    dcc.Input(
                        id="filtro_texto",
                        type="text",
                        placeholder="Buscar por título, município, categoria ou query...",
                        debounce=True,
                        style={
                            "width": "100%",
                            "height": "38px",
                            "border": "1px solid #d1d5db",
                            "borderRadius": "6px",
                            "padding": "0 10px"
                        }
                    )
                ],
                style={
                    "display": "grid",
                    "gridTemplateColumns": "1fr 1fr 1fr 1.2fr 1.8fr",
                    "gap": "12px",
                    "marginBottom": "22px",
                    "alignItems": "center"
                }
            ),

            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Mapa de ocorrências", style={"marginTop": "0", "marginBottom": "12px", "color": "#374151"}),
                            html.Iframe(
                                id="mapa_html",
                                style={"width": "100%", "height": "620px", "border": "none", "borderRadius": "12px"}
                            )
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "16px",
                            "borderRadius": "16px",
                            "boxShadow": "0 4px 14px rgba(0,0,0,0.08)",
                            "width": "50%"
                        }
                    ),

                    html.Div(
                        [
                            dcc.Graph(id="grafico_categoria"),
                            dcc.Graph(id="grafico_uf"),
                            dcc.Graph(id="grafico_tempo")
                        ],
                        style={
                            "backgroundColor": "white",
                            "padding": "16px",
                            "borderRadius": "16px",
                            "boxShadow": "0 4px 14px rgba(0,0,0,0.08)",
                            "width": "50%"
                        }
                    )
                ],
                style={"display": "flex", "gap": "18px", "marginBottom": "22px"}
            ),

            html.Div(
                id="insight",
                style={
                    "backgroundColor": "#eef2ff",
                    "border": "1px solid #c7d2fe",
                    "color": "#3730a3",
                    "padding": "16px",
                    "borderRadius": "14px",
                    "marginBottom": "22px",
                    "fontWeight": "500"
                }
            ),

            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Registros", style={"margin": "0", "color": "#374151"}),
                            html.Button(
                                "⬇️ Exportar CSV filtrado",
                                id="btn_exportar",
                                n_clicks=0,
                                style={
                                    "backgroundColor": "#2563eb",
                                    "color": "white",
                                    "border": "none",
                                    "borderRadius": "10px",
                                    "padding": "10px 16px",
                                    "cursor": "pointer",
                                    "fontWeight": "600"
                                }
                            )
                        ],
                        style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "14px"}
                    ),

                    dash_table.DataTable(
                        id="tabela",
                        columns=[
                            {"name": "Data", "id": "data"},
                            {"name": "Município", "id": "municipio"},
                            {"name": "UF", "id": "uf"},
                            {"name": "Categoria", "id": "categoria"},
                            {"name": "Título", "id": "titulo"},
                            {"name": "URL", "id": "url"},
                            {"name": "Query origem", "id": "query_origem"}
                        ],
                        page_size=15,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "left",
                            "padding": "9px",
                            "fontFamily": "Arial",
                            "fontSize": "13px",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "maxWidth": "360px"
                        },
                        style_header={"fontWeight": "bold", "backgroundColor": "#f3f4f6", "color": "#111827"},
                        style_data={"backgroundColor": "white", "color": "#374151"}
                    )
                ],
                style={
                    "backgroundColor": "white",
                    "padding": "18px",
                    "borderRadius": "16px",
                    "boxShadow": "0 4px 14px rgba(0,0,0,0.08)"
                }
            ),

            dcc.Download(id="download_csv")
        ],
        style={"paddingTop": "18px"}
    )


def layout_tab_usuarios():
    return html.Div(
        [
            html.Div(
                [
                    html.H3("Gerenciamento de usuários", style={"marginTop": 0}),
                    html.P(
                        "A gestão de usuários foi movida para uma área administrativa server-side para evitar problemas de sessão em callbacks dinâmicos do Dash.",
                        style={"color": "#6b7280", "fontSize": "14px", "lineHeight": "1.5"}
                    ),
                    html.A(
                        "Abrir gerenciador de usuários",
                        href="/admin/usuarios",
                        target="_blank",
                        style={
                            "backgroundColor": "#2563eb",
                            "color": "white",
                            "borderRadius": "10px",
                            "padding": "12px 18px",
                            "display": "inline-block",
                            "textDecoration": "none",
                            "fontWeight": "700"
                        }
                    )
                ]
            )
        ],
        style={
            "backgroundColor": "white",
            "padding": "18px",
            "borderRadius": "16px",
            "boxShadow": "0 4px 14px rgba(0,0,0,0.08)",
            "marginTop": "18px"
        }
    )


def layout_tab_logs():
    return html.Div(
        [
            html.Div(
                [
                    html.H3("Logs de acesso", style={"marginTop": 0}),
                    html.P(
                        "Os logs de acesso agora ficam na área administrativa server-side.",
                        style={"color": "#6b7280", "fontSize": "14px", "lineHeight": "1.5"}
                    ),
                    html.A(
                        "Abrir logs de acesso",
                        href="/admin/logs",
                        target="_blank",
                        style={
                            "backgroundColor": "#1f2937",
                            "color": "white",
                            "borderRadius": "10px",
                            "padding": "12px 18px",
                            "display": "inline-block",
                            "textDecoration": "none",
                            "fontWeight": "700"
                        }
                    )
                ]
            )
        ],
        style={
            "backgroundColor": "white",
            "padding": "18px",
            "borderRadius": "16px",
            "boxShadow": "0 4px 14px rgba(0,0,0,0.08)",
            "marginTop": "18px"
        }
    )


app.layout = html.Div(
    [
        dcc.Location(id="url"),
        dcc.Store(id="sessao_token", storage_type="local"),
        dcc.Store(id="usuario_logado", storage_type="session"),
        html.Div(id="pagina_container")
    ]
)


# ============================================================
# CALLBACKS - AUTENTICAÇÃO
# ============================================================

@app.callback(
    Output("pagina_container", "children"),
    Input("sessao_token", "data")
)
def renderizar_pagina(token):
    usuario = obter_usuario_por_token(token)

    if not usuario:
        return layout_login()

    return layout_dashboard(usuario)


@app.callback(
    [
        Output("sessao_token", "data"),
        Output("usuario_logado", "data"),
        Output("login_mensagem", "children"),
        Output("login_mensagem", "style")
    ],
    Input("btn_login", "n_clicks"),
    [
        State("login_email", "value"),
        State("login_senha", "value")
    ],
    prevent_initial_call=True
)
def fazer_login(n_clicks, email, senha):
    if not n_clicks:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update

    if not email or not senha:
        return (
            dash.no_update,
            dash.no_update,
            "Informe e-mail e senha.",
            {"marginTop": "14px", "fontSize": "14px", "color": "#b91c1c"}
        )

    resultado = autenticar_usuario(
        email=email,
        senha=senha,
        ip=obter_ip_requisicao(),
        user_agent=obter_user_agent()
    )

    if not resultado.get("ok"):
        return (
            dash.no_update,
            dash.no_update,
            resultado.get("motivo", "Falha ao autenticar."),
            {"marginTop": "14px", "fontSize": "14px", "color": "#b91c1c"}
        )

    usuario_store = dict(resultado["usuario"])
    usuario_store["token_sessao"] = resultado["token_sessao"]

    return (
        resultado["token_sessao"],
        usuario_store,
        "Login realizado com sucesso.",
        {"marginTop": "14px", "fontSize": "14px", "color": "#047857"}
    )


@app.callback(
    [
        Output("sessao_token", "data", allow_duplicate=True),
        Output("usuario_logado", "data", allow_duplicate=True)
    ],
    Input("btn_logout", "n_clicks"),
    State("sessao_token", "data"),
    prevent_initial_call=True
)
def fazer_logout(n_clicks, token):
    if n_clicks and token:
        try:
            encerrar_sessao(token)
        except Exception as e:
            log_erro("fazer_logout", e)

    return None, None


# ============================================================
# CALLBACK: CARREGAR DADOS
# ============================================================

@app.callback(
    [
        Output("dados_base", "data"),
        Output("status_dados", "children"),
        Output("filtro_uf", "options"),
        Output("filtro_municipio", "options"),
        Output("filtro_categoria", "options"),
        Output("status_coleta", "children")
    ],
    [
        Input("btn_recarregar", "n_clicks"),
        Input("btn_coletar_dados", "n_clicks")
    ],
    [
        State("sessao_token", "data"),
        State("token_contexto", "data"),
        State("usuario_logado", "data")
    ]
)
def carregar_dados(n_recarregar, n_coletar, token, token_contexto, usuario_store):
    try:
        token = resolver_token(token, token_contexto, usuario_store)

        # Evita disparos automáticos enquanto o token ainda não foi carregado pelo navegador.
        if not token:
            raise PreventUpdate

        usuario = obter_usuario_por_token(token)

        # Se por algum motivo o token ainda não validou, não sobrescrevemos a tela.
        if not usuario:
            raise PreventUpdate

        trigger = ""
        try:
            trigger = dash.callback_context.triggered[0]["prop_id"].split(".")[0]
        except Exception:
            trigger = ""

        status_coleta = ""

        if trigger == "btn_coletar_dados":
            if not usuario_eh_admin(usuario):
                status_coleta = "Apenas usuários admin podem executar a coleta."
            else:
                inicio_coleta = datetime.now()
                try:
                    import coletor
                    coletor.main()
                    fim_coleta = datetime.now()
                    duracao = int((fim_coleta - inicio_coleta).total_seconds())
                    status_coleta = f"Coleta finalizada com sucesso em {duracao}s. Dados recarregados automaticamente."
                except Exception as e:
                    log_erro("coletar_novos_dados", e)
                    status_coleta = f"Erro ao executar coleta: {type(e).__name__}: {e}"

        df = carregar_dados_banco()

        total = len(df)
        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        status = (
            f"Dados carregados do banco em {agora}. "
            f"Total de registros: {formatar_numero(total)}."
        )

        opcoes_uf = [
            {"label": uf, "value": uf}
            for uf in sorted(df["uf"].dropna().unique())
            if uf
        ]

        opcoes_municipio = [
            {"label": municipio, "value": municipio}
            for municipio in sorted(df["municipio"].dropna().unique())
            if municipio
        ]

        opcoes_categoria = [
            {"label": categoria, "value": categoria}
            for categoria in sorted(df["categoria"].dropna().unique())
            if categoria
        ]

        dados_json = df.to_json(date_format="iso", orient="split")

        return dados_json, status, opcoes_uf, opcoes_municipio, opcoes_categoria, status_coleta

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("carregar_dados", e)
        df_vazio = tratar_dataframe(pd.DataFrame())
        return (
            df_vazio.to_json(date_format="iso", orient="split"),
            mensagem_erro_usuario("carregar dados", e),
            [],
            [],
            [],
            ""
        )


# ============================================================
# CALLBACK: ATUALIZAR DASHBOARD
# ============================================================

@app.callback(
    [
        Output("mapa_html", "srcDoc"),
        Output("grafico_categoria", "figure"),
        Output("grafico_uf", "figure"),
        Output("grafico_tempo", "figure"),
        Output("tabela", "data"),
        Output("cards_resumo", "children"),
        Output("insight", "children"),
        Output("dados_filtrados", "data")
    ],
    [
        Input("dados_base", "data"),
        Input("filtro_uf", "value"),
        Input("filtro_municipio", "value"),
        Input("filtro_categoria", "value"),
        Input("filtro_data", "start_date"),
        Input("filtro_data", "end_date"),
        Input("filtro_texto", "value")
    ],
    [
        State("sessao_token", "data"),
        State("token_contexto", "data"),
        State("usuario_logado", "data")
    ]
)
def atualizar_dashboard(
    dados_base,
    ufs,
    municipios,
    categorias,
    data_ini,
    data_fim,
    texto_busca,
    token,
    token_contexto,
    usuario_store
):
    try:
        token = resolver_token(token, token_contexto, usuario_store)

        # Ao alternar entre abas, alguns callbacks podem disparar antes do token estar disponível.
        # Nesses casos, não atualizamos nada para evitar a mensagem indevida de sessão inválida.
        if not token:
            raise PreventUpdate

        usuario = obter_usuario_por_token(token)

        if not usuario:
            raise PreventUpdate

        if not dados_base:
            df = carregar_dados_banco()
        else:
            df = ler_json_dataframe(dados_base)
            df = tratar_dataframe(df)

        df_filtrado = aplicar_filtros(
            df=df,
            ufs=ufs,
            municipios=municipios,
            categorias=categorias,
            data_ini=data_ini,
            data_fim=data_fim,
            texto_busca=texto_busca
        )

        # MAPA
        if df_filtrado.empty:
            base_mapa = pd.DataFrame(
                columns=["municipio", "uf", "categoria", "latitude", "longitude", "quantidade"]
            )
        else:
            base_mapa = (
                df_filtrado
                .dropna(subset=["latitude", "longitude"])
                .groupby(["municipio", "uf", "categoria", "latitude", "longitude"], as_index=False)["quantidade"]
                .sum()
            )

        mapa_html = gerar_mapa(base_mapa)

        # CATEGORIA
        if df_filtrado.empty:
            fig_categoria = criar_figura_vazia("Registros por categoria")
        else:
            df_categoria = (
                df_filtrado
                .groupby("categoria")
                .size()
                .reset_index(name="qtd")
                .sort_values("qtd", ascending=True)
            )

            fig_categoria = px.bar(
                df_categoria,
                x="qtd",
                y="categoria",
                orientation="h",
                title="Registros por categoria",
                text="qtd"
            )
            fig_categoria.update_layout(
                margin={"l": 20, "r": 20, "t": 50, "b": 20},
                yaxis_title="",
                xaxis_title="Quantidade"
            )

        # UF
        if df_filtrado.empty:
            fig_uf = criar_figura_vazia("Registros por UF")
        else:
            df_uf = (
                df_filtrado
                .groupby("uf")
                .size()
                .reset_index(name="qtd")
                .sort_values("qtd", ascending=True)
            )

            fig_uf = px.bar(
                df_uf,
                x="qtd",
                y="uf",
                orientation="h",
                title="Registros por UF",
                text="qtd"
            )
            fig_uf.update_layout(
                margin={"l": 20, "r": 20, "t": 50, "b": 20},
                yaxis_title="",
                xaxis_title="Quantidade"
            )

        # TEMPO
        if df_filtrado.empty or df_filtrado["data"].dropna().empty:
            fig_tempo = criar_figura_vazia("Evolução no tempo")
        else:
            df_tempo = df_filtrado.dropna(subset=["data"]).copy()
            df_tempo["data_dia"] = pd.to_datetime(df_tempo["data"], errors="coerce").dt.date

            df_tempo = (
                df_tempo
                .dropna(subset=["data_dia"])
                .groupby("data_dia")
                .size()
                .reset_index(name="qtd")
                .sort_values("data_dia")
            )

            fig_tempo = px.line(
                df_tempo,
                x="data_dia",
                y="qtd",
                title="Evolução no tempo",
                markers=True
            )
            fig_tempo.update_layout(
                margin={"l": 20, "r": 20, "t": 50, "b": 20},
                xaxis_title="Data",
                yaxis_title="Quantidade"
            )

        # TABELA
        tabela_df = df_filtrado.copy()

        if not tabela_df.empty:
            tabela_df["data"] = pd.to_datetime(tabela_df["data"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")
            tabela_df = tabela_df[["data", "municipio", "uf", "categoria", "titulo", "url", "query_origem"]]
        else:
            tabela_df = pd.DataFrame(columns=["data", "municipio", "uf", "categoria", "titulo", "url", "query_origem"])

        # CARDS
        total_registros = len(df_filtrado)
        total_municipios = df_filtrado["municipio"].nunique() if not df_filtrado.empty else 0
        total_ufs = df_filtrado["uf"].nunique() if not df_filtrado.empty else 0
        total_categorias = df_filtrado["categoria"].nunique() if not df_filtrado.empty else 0

        cards = [
            card_resumo("Total de registros", formatar_numero(total_registros)),
            card_resumo("Municípios", formatar_numero(total_municipios)),
            card_resumo("UFs", formatar_numero(total_ufs)),
            card_resumo("Categorias", formatar_numero(total_categorias)),
        ]

        # INSIGHT
        if df_filtrado.empty:
            insight = "Sem dados para os filtros selecionados."
        else:
            top_municipio = df_filtrado.groupby("municipio").size().idxmax()
            qtd_top_municipio = df_filtrado.groupby("municipio").size().max()

            top_categoria = df_filtrado.groupby("categoria").size().idxmax()
            qtd_top_categoria = df_filtrado.groupby("categoria").size().max()

            insight = (
                f"Principal município no filtro atual: {top_municipio} "
                f"({formatar_numero(qtd_top_municipio)} registros). "
                f"Principal categoria: {top_categoria} "
                f"({formatar_numero(qtd_top_categoria)} registros)."
            )

        dados_filtrados = df_filtrado.to_json(date_format="iso", orient="split")

        return (
            mapa_html,
            fig_categoria,
            fig_uf,
            fig_tempo,
            tabela_df.to_dict("records"),
            cards,
            insight,
            dados_filtrados
        )

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("atualizar_dashboard", e)

        df_vazio = pd.DataFrame(columns=["data", "municipio", "uf", "categoria", "titulo", "url", "query_origem"])

        return (
            "",
            criar_figura_vazia("Registros por categoria"),
            criar_figura_vazia("Registros por UF"),
            criar_figura_vazia("Evolução no tempo"),
            df_vazio.to_dict("records"),
            [
                card_resumo("Total de registros", "0"),
                card_resumo("Municípios", "0"),
                card_resumo("UFs", "0"),
                card_resumo("Categorias", "0"),
            ],
            mensagem_erro_usuario("atualizar dashboard", e),
            df_vazio.to_json(date_format="iso", orient="split")
        )


# ============================================================
# CALLBACK: EXPORTAR CSV
# ============================================================

@app.callback(
    Output("download_csv", "data"),
    Input("btn_exportar", "n_clicks"),
    State("dados_filtrados", "data"),
    State("sessao_token", "data"),
    State("token_contexto", "data"),
    State("usuario_logado", "data"),
    prevent_initial_call=True
)
def exportar_csv(n_clicks, dados_filtrados, token, token_contexto, usuario_store):
    try:
        token = resolver_token(token, token_contexto, usuario_store)

        if not token:
            raise PreventUpdate

        usuario = obter_usuario_por_token(token)

        if not usuario or not n_clicks:
            raise PreventUpdate

        if dados_filtrados:
            df_export = ler_json_dataframe(dados_filtrados)
            df_export = tratar_dataframe(df_export)
        else:
            df_export = carregar_dados_banco()

        colunas_export = [
            "id",
            "titulo",
            "url",
            "municipio",
            "uf",
            "categoria",
            "latitude",
            "longitude",
            "data_coleta",
            "data_publicacao",
            "query_origem",
            "criado_em"
        ]

        for coluna in colunas_export:
            if coluna not in df_export.columns:
                df_export[coluna] = None

        df_export = df_export[colunas_export]

        return send_data_frame(
            df_export.to_csv,
            "pop_rua_filtrado.csv",
            index=False,
            sep=";",
            encoding="utf-8-sig"
        )

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("exportar_csv", e)
        return dash.no_update


# ============================================================
# CALLBACKS - ADMIN
# ============================================================

@app.callback(
    [
        Output("tabela_usuarios", "data"),
        Output("usuarios_status", "children")
    ],
    Input("btn_recarregar_usuarios", "n_clicks"),
    State("sessao_token", "data"),
    State("token_contexto", "data"),
    State("usuario_logado", "data")
)
def carregar_usuarios_admin(n_clicks, token, token_contexto, usuario_store):
    token = resolver_token(token, token_contexto, usuario_store)

    if not token:
        raise PreventUpdate

    usuario = obter_usuario_por_token(token)

    if not usuario_eh_admin(usuario):
        return [], "Acesso negado."

    try:
        usuarios = listar_usuarios()
        df = pd.DataFrame(usuarios)

        if df.empty:
            return [], "Nenhum usuário encontrado."

        for col in ["criado_em", "atualizado_em", "ultimo_login"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y %H:%M")

        return df.to_dict("records"), f"Usuários carregados: {len(df)}"

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("carregar_usuarios_admin", e)
        return [], f"Erro ao carregar usuários: {e}"


@app.callback(
    Output("usuarios_status", "children", allow_duplicate=True),
    Input("btn_criar_usuario", "n_clicks"),
    [
        State("novo_nome", "value"),
        State("novo_email", "value"),
        State("novo_senha", "value"),
        State("novo_perfil", "value"),
        State("sessao_token", "data"),
        State("token_contexto", "data"),
        State("usuario_logado", "data")
    ],
    prevent_initial_call=True
)
def criar_usuario_admin(n_clicks, nome, email, senha, perfil, token, token_contexto, usuario_store):
    if not token:
        raise PreventUpdate

    usuario = obter_usuario_por_token(token)

    if not usuario_eh_admin(usuario):
        return "Acesso negado."

    if not n_clicks:
        return dash.no_update

    try:
        usuario_id = criar_usuario(
            nome=nome,
            email=email,
            senha=senha,
            perfil=perfil or "usuario",
            primeiro_acesso=True
        )

        return f"✅ Usuário criado com sucesso. ID: {usuario_id}. Clique em Recarregar usuários."

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("criar_usuario_admin", e)
        return f"❌ Erro ao criar usuário: {e}"


@app.callback(
    Output("tabela_logs", "data"),
    Input("btn_recarregar_logs", "n_clicks"),
    State("sessao_token", "data"),
    State("token_contexto", "data"),
    State("usuario_logado", "data")
)
def carregar_logs_admin(n_clicks, token, token_contexto, usuario_store):
    token = resolver_token(token, token_contexto, usuario_store)

    if not token:
        raise PreventUpdate

    usuario = obter_usuario_por_token(token)

    if not usuario_eh_admin(usuario):
        return []

    try:
        logs = listar_logs_acesso(limit=300)
        df = pd.DataFrame(logs)

        if df.empty:
            return []

        if "criado_em" in df.columns:
            df["criado_em"] = pd.to_datetime(df["criado_em"], errors="coerce").dt.strftime("%d/%m/%Y %H:%M:%S")

        return df.to_dict("records")

    except PreventUpdate:
        raise
    except Exception as e:
        log_erro("carregar_logs_admin", e)
        return []


# ============================================================
# RODAR LOCALMENTE
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8050)),
        debug=True
    )
