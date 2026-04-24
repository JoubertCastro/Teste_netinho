import os
from datetime import datetime

import dash
import dash_auth
from dash import dcc, html, Input, Output, State, dash_table
from dash.dcc import send_data_frame

import pandas as pd
import plotly.express as px

import folium
from folium.plugins import MarkerCluster

from sqlalchemy import create_engine, text


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL não encontrada. "
        "Configure essa variável de ambiente no Railway."
    )

DASH_USER = os.getenv("DASH_USER", "MDHC")
DASH_PASSWORD = os.getenv("DASH_PASSWORD", "1234")

VALID_USERNAME_PASSWORD_PAIRS = {
    DASH_USER: DASH_PASSWORD
}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

ARQ_MAPA_HTML = "mapa.html"


# ============================================================
# APP DASH
# ============================================================

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Painel Pop Rua"
)

server = app.server

auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)


# ============================================================
# BANCO DE DADOS
# ============================================================

def carregar_dados_banco():
    """
    Lê os dados da tabela pop_rua no PostgreSQL.
    """
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
        print(f"Erro ao carregar dados do banco: {e}")
        df = pd.DataFrame(
            columns=[
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
        )

    return tratar_dataframe(df)


def tratar_dataframe(df):
    """
    Padroniza o dataframe vindo do banco.
    """
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

    df["municipio"] = df["municipio"].fillna("Não identificado")
    df["uf"] = df["uf"].fillna("NI")
    df["categoria"] = df["categoria"].fillna("Outros")
    df["titulo"] = df["titulo"].fillna("")
    df["url"] = df["url"].fillna("")
    df["query_origem"] = df["query_origem"].fillna("")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce")
    df["data_publicacao"] = pd.to_datetime(df["data_publicacao"], errors="coerce")
    df["criado_em"] = pd.to_datetime(df["criado_em"], errors="coerce")

    # Campo principal usado nos filtros e gráficos.
    # Se tiver data_publicacao, usa ela; senão usa data_coleta.
    df["data"] = df["data_publicacao"].fillna(df["data_coleta"])

    df["quantidade"] = 1

    return df


# ============================================================
# MAPA
# ============================================================

def gerar_mapa(df):
    """
    Gera mapa Folium e retorna HTML.
    """
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


# ============================================================
# HELPERS VISUAIS
# ============================================================

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
            html.Div(
                titulo,
                style={
                    "fontSize": "13px",
                    "color": "#666",
                    "marginBottom": "6px"
                }
            ),
            html.Div(
                valor,
                style={
                    "fontSize": "26px",
                    "fontWeight": "700",
                    "color": "#222"
                }
            ),
            html.Div(
                subtitulo or "",
                style={
                    "fontSize": "12px",
                    "color": "#777",
                    "marginTop": "4px"
                }
            )
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


def formatar_numero(valor):
    try:
        return f"{int(valor):,}".replace(",", ".")
    except Exception:
        return "0"


# ============================================================
# FILTROS
# ============================================================

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
            # inclui o dia inteiro
            data_fim = data_fim + pd.Timedelta(days=1)
            df = df[df["data"] < data_fim]

    if texto_busca:
        texto = str(texto_busca).lower().strip()

        if texto:
            mascara = (
                df["titulo"].astype(str).str.lower().str.contains(texto, na=False)
                | df["municipio"].astype(str).str.lower().str.contains(texto, na=False)
                | df["categoria"].astype(str).str.lower().str.contains(texto, na=False)
                | df["query_origem"].astype(str).str.lower().str.contains(texto, na=False)
            )

            df = df[mascara]

    return df


# ============================================================
# LAYOUT
# ============================================================

def layout_principal():
    return html.Div(
        [
            dcc.Store(id="dados_base"),
            dcc.Store(id="dados_filtrados"),

            html.Div(
                [
                    html.Div(
                        [
                            html.H1(
                                "Painel População em Situação de Rua",
                                style={
                                    "margin": "0",
                                    "fontSize": "30px",
                                    "color": "#1f2937"
                                }
                            ),
                            html.P(
                                "Monitoramento de notícias e óbitos coletados automaticamente",
                                style={
                                    "margin": "8px 0 0 0",
                                    "color": "#6b7280",
                                    "fontSize": "15px"
                                }
                            )
                        ]
                    ),

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
                    )
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "22px"
                }
            ),

            html.Div(
                id="status_dados",
                style={
                    "marginBottom": "18px",
                    "color": "#6b7280",
                    "fontSize": "13px"
                }
            ),

            html.Div(
                id="cards_resumo",
                style={
                    "display": "flex",
                    "gap": "16px",
                    "flexWrap": "wrap",
                    "marginBottom": "22px"
                }
            ),

            html.Div(
                [
                    dcc.Dropdown(
                        id="filtro_uf",
                        multi=True,
                        placeholder="Filtrar por UF",
                        style={"width": "100%"}
                    ),
                    dcc.Dropdown(
                        id="filtro_municipio",
                        multi=True,
                        placeholder="Filtrar por município",
                        style={"width": "100%"}
                    ),
                    dcc.Dropdown(
                        id="filtro_categoria",
                        multi=True,
                        placeholder="Filtrar por categoria",
                        style={"width": "100%"}
                    ),
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
                            html.H3(
                                "Mapa de ocorrências",
                                style={
                                    "marginTop": "0",
                                    "marginBottom": "12px",
                                    "color": "#374151"
                                }
                            ),
                            html.Iframe(
                                id="mapa_html",
                                style={
                                    "width": "100%",
                                    "height": "620px",
                                    "border": "none",
                                    "borderRadius": "12px"
                                }
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
                style={
                    "display": "flex",
                    "gap": "18px",
                    "marginBottom": "22px"
                }
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
                            html.H3(
                                "Registros",
                                style={
                                    "margin": "0",
                                    "color": "#374151"
                                }
                            ),
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
                        style={
                            "display": "flex",
                            "justifyContent": "space-between",
                            "alignItems": "center",
                            "marginBottom": "14px"
                        }
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
                        style_table={
                            "overflowX": "auto"
                        },
                        style_cell={
                            "textAlign": "left",
                            "padding": "9px",
                            "fontFamily": "Arial",
                            "fontSize": "13px",
                            "whiteSpace": "normal",
                            "height": "auto",
                            "maxWidth": "360px"
                        },
                        style_header={
                            "fontWeight": "bold",
                            "backgroundColor": "#f3f4f6",
                            "color": "#111827"
                        },
                        style_data={
                            "backgroundColor": "white",
                            "color": "#374151"
                        }
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
        style={
            "padding": "26px",
            "backgroundColor": "#f3f4f6",
            "minHeight": "100vh",
            "fontFamily": "Arial, sans-serif"
        }
    )


app.layout = layout_principal


# ============================================================
# CALLBACK: CARREGAR DADOS DO BANCO
# ============================================================

@app.callback(
    [
        Output("dados_base", "data"),
        Output("status_dados", "children"),
        Output("filtro_uf", "options"),
        Output("filtro_municipio", "options"),
        Output("filtro_categoria", "options")
    ],
    Input("btn_recarregar", "n_clicks")
)
def carregar_dados(n_clicks):
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

    return dados_json, status, opcoes_uf, opcoes_municipio, opcoes_categoria


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
    ]
)
def atualizar_dashboard(
    dados_base,
    ufs,
    municipios,
    categorias,
    data_ini,
    data_fim,
    texto_busca
):
    if not dados_base:
        df = carregar_dados_banco()
    else:
        df = pd.read_json(dados_base, orient="split")
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

    # =========================
    # MAPA
    # =========================
    if df_filtrado.empty:
        base_mapa = pd.DataFrame(
            columns=[
                "municipio",
                "uf",
                "categoria",
                "latitude",
                "longitude",
                "quantidade"
            ]
        )
    else:
        base_mapa = (
            df_filtrado
            .dropna(subset=["latitude", "longitude"])
            .groupby(
                ["municipio", "uf", "categoria", "latitude", "longitude"],
                as_index=False
            )["quantidade"]
            .sum()
        )

    mapa_html = gerar_mapa(base_mapa)

    # =========================
    # GRÁFICO CATEGORIA
    # =========================
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

    # =========================
    # GRÁFICO UF
    # =========================
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

    # =========================
    # GRÁFICO TEMPO
    # =========================
    if df_filtrado.empty or df_filtrado["data"].dropna().empty:
        fig_tempo = criar_figura_vazia("Evolução no tempo")
    else:
        df_tempo = (
            df_filtrado
            .dropna(subset=["data"])
            .copy()
        )

        df_tempo["data_dia"] = pd.to_datetime(df_tempo["data"]).dt.date

        df_tempo = (
            df_tempo
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

    # =========================
    # TABELA
    # =========================
    tabela_df = df_filtrado.copy()

    if not tabela_df.empty:
        tabela_df["data"] = pd.to_datetime(
            tabela_df["data"],
            errors="coerce"
        ).dt.strftime("%d/%m/%Y %H:%M")

        tabela_df = tabela_df[
            [
                "data",
                "municipio",
                "uf",
                "categoria",
                "titulo",
                "url",
                "query_origem"
            ]
        ]
    else:
        tabela_df = pd.DataFrame(
            columns=[
                "data",
                "municipio",
                "uf",
                "categoria",
                "titulo",
                "url",
                "query_origem"
            ]
        )

    # =========================
    # CARDS
    # =========================
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

    # =========================
    # INSIGHT
    # =========================
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


# ============================================================
# CALLBACK: EXPORTAR CSV
# ============================================================

@app.callback(
    Output("download_csv", "data"),
    Input("btn_exportar", "n_clicks"),
    State("dados_filtrados", "data"),
    prevent_initial_call=True
)
def exportar_csv(n_clicks, dados_filtrados):
    if not n_clicks:
        return dash.no_update

    if dados_filtrados:
        df_export = pd.read_json(dados_filtrados, orient="split")
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


# ============================================================
# RODAR LOCALMENTE
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8050)),
        debug=True
    )