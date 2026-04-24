import os
import re
import time
import random
import unicodedata
from datetime import datetime

import pandas as pd
from ddgs import DDGS
from newspaper import Article

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL não encontrada. "
        "Configure essa variável de ambiente no Railway."
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

URL_IBGE = (
    "https://raw.githubusercontent.com/kelvins/"
    "Municipios-Brasileiros/master/csv/municipios.csv"
)

QUERIES = [
    "morador de rua morto",
    "morador de rua morreu",
    "sem teto morto",
    "assassinato morador de rua",
    "violência contra morador de rua",
    "morador em situação de rua morto",
    "pessoa em situação de rua morta",
    "morador de rua agredido",
    "morador de rua atropelado",
]

CATEGORIAS = {
    "Morte": [
        "morto",
        "morta",
        "morreu",
        "óbito",
        "obito",
        "falecimento",
        "morte",
        "corpo encontrado",
        "encontrado morto",
        "encontrada morta",
    ],
    "Violência": [
        "assassinado",
        "assassinada",
        "agredido",
        "agredida",
        "espancado",
        "espancada",
        "violência",
        "violencia",
        "homicídio",
        "homicidio",
        "facada",
        "tiro",
        "queimado",
        "queimada",
    ],
    "Acidente": [
        "acidente",
        "atropelado",
        "atropelada",
        "atropelamento",
    ],
    "Outros": []
}

MAPA_UF = {
    11: "RO",
    12: "AC",
    13: "AM",
    14: "RR",
    15: "PA",
    16: "AP",
    17: "TO",
    21: "MA",
    22: "PI",
    23: "CE",
    24: "RN",
    25: "PB",
    26: "PE",
    27: "AL",
    28: "SE",
    29: "BA",
    31: "MG",
    32: "ES",
    33: "RJ",
    35: "SP",
    41: "PR",
    42: "SC",
    43: "RS",
    50: "MS",
    51: "MT",
    52: "GO",
    53: "DF",
}


# ============================================================
# NORMALIZAÇÃO DE TEXTO
# ============================================================

def normalizar_texto(valor):
    """
    Normaliza texto para facilitar buscas:
    - converte para minúsculo;
    - remove acentos;
    - remove espaços duplicados.
    """
    texto = str(valor or "").lower().strip()

    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(caractere)
    )

    texto = re.sub(r"\s+", " ", texto)

    return texto


# ============================================================
# BANCO DE DADOS
# ============================================================

def criar_tabela():
    """
    Cria a tabela principal caso ainda não exista.
    A URL fica como UNIQUE para evitar notícia duplicada.
    """
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
        data_coleta TIMESTAMP,
        data_publicacao TIMESTAMP NULL,
        query_origem TEXT,
        criado_em TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_pop_rua_municipio
        ON pop_rua (municipio);

    CREATE INDEX IF NOT EXISTS ix_pop_rua_uf
        ON pop_rua (uf);

    CREATE INDEX IF NOT EXISTS ix_pop_rua_categoria
        ON pop_rua (categoria);

    CREATE INDEX IF NOT EXISTS ix_pop_rua_data_coleta
        ON pop_rua (data_coleta);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


def inserir_registro(registro):
    """
    Insere um registro na tabela pop_rua.
    Caso a URL já exista, ignora.
    """
    sql = """
    INSERT INTO pop_rua (
        titulo,
        url,
        municipio,
        uf,
        categoria,
        latitude,
        longitude,
        data_coleta,
        data_publicacao,
        query_origem
    )
    VALUES (
        :titulo,
        :url,
        :municipio,
        :uf,
        :categoria,
        :latitude,
        :longitude,
        :data_coleta,
        :data_publicacao,
        :query_origem
    )
    ON CONFLICT (url) DO NOTHING;
    """

    with engine.begin() as conn:
        result = conn.execute(text(sql), registro)
        return result.rowcount


# ============================================================
# MUNICÍPIOS
# ============================================================

def carregar_municipios():
    """
    Carrega base pública de municípios brasileiros com latitude e longitude.
    """
    print("📍 Carregando base de municípios do IBGE/GitHub...")

    df_municipios = pd.read_csv(URL_IBGE)

    colunas_necessarias = {
        "nome",
        "latitude",
        "longitude",
        "codigo_uf"
    }

    colunas_faltando = colunas_necessarias - set(df_municipios.columns)

    if colunas_faltando:
        raise RuntimeError(
            f"A base de municípios está sem as colunas: {colunas_faltando}"
        )

    df_municipios["nome_norm"] = df_municipios["nome"].apply(normalizar_texto)

    coords_local = {}

    for _, row in df_municipios.iterrows():
        codigo_uf = int(row["codigo_uf"])

        municipio_norm = row["nome_norm"]

        coords_local[municipio_norm] = {
            "nome_original": row["nome"],
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "uf": MAPA_UF.get(codigo_uf, "NI")
        }

    lista_local = sorted(
        coords_local.keys(),
        key=len,
        reverse=True
    )

    print(f"✅ Municípios carregados: {len(lista_local)}")

    return coords_local, lista_local


# Carrega uma vez na inicialização do script.
COORDS, LISTA_MUNICIPIOS = carregar_municipios()


# ============================================================
# DETECÇÃO DE MUNICÍPIO
# ============================================================

def detectar_municipio(texto):
    """
    Tenta encontrar o município dentro do título/texto da notícia.

    A busca é normalizada para evitar problema com acentos:
    São Paulo -> sao paulo
    Brasília -> brasilia
    """
    texto_norm = normalizar_texto(texto)

    for municipio_norm in LISTA_MUNICIPIOS:
        padrao = rf"\b{re.escape(municipio_norm)}\b"

        if re.search(padrao, texto_norm):
            return municipio_norm

    return None


# ============================================================
# CLASSIFICAÇÃO
# ============================================================

def classificar(texto):
    """
    Classifica a notícia com base em palavras-chave simples.
    """
    texto_norm = normalizar_texto(texto)

    for categoria, palavras in CATEGORIAS.items():
        if categoria == "Outros":
            continue

        for palavra in palavras:
            palavra_norm = normalizar_texto(palavra)

            if palavra_norm in texto_norm:
                return categoria

    return "Outros"


# ============================================================
# PROCESSAMENTO DE NOTÍCIA
# ============================================================

def processar_noticia(url, query_origem):
    """
    Baixa e processa uma notícia.
    Retorna um dicionário pronto para inserir no banco.
    """
    try:
        artigo = Article(
            url,
            language="pt",
            request_timeout=10
        )

        artigo.download()
        artigo.parse()

        titulo = artigo.title or ""
        texto = artigo.text or ""

        if not titulo and not texto:
            print(f"⚠️ Artigo sem título/texto: {url}")
            return None

        base = f" {titulo} {texto} "

        municipio_norm = detectar_municipio(base)

        if municipio_norm:
            info = COORDS.get(municipio_norm)

            municipio = str(info["nome_original"]).title()
            uf = info["uf"]
            latitude = info["latitude"]
            longitude = info["longitude"]
        else:
            municipio = "Não identificado"
            uf = "NI"
            latitude = -14.2350
            longitude = -51.9253

        categoria = classificar(base)

        # Pequeno deslocamento visual para evitar marcadores exatamente sobrepostos.
        latitude = float(latitude) + random.uniform(-0.02, 0.02)
        longitude = float(longitude) + random.uniform(-0.02, 0.02)

        data_publicacao = artigo.publish_date

        # Alguns sites retornam data com timezone. Para evitar erro no PostgreSQL,
        # removemos o timezone mantendo o horário.
        if data_publicacao is not None and data_publicacao.tzinfo is not None:
            data_publicacao = data_publicacao.replace(tzinfo=None)

        return {
            "titulo": titulo.strip(),
            "url": url,
            "municipio": municipio,
            "uf": uf,
            "categoria": categoria,
            "latitude": latitude,
            "longitude": longitude,
            "data_coleta": datetime.now(),
            "data_publicacao": data_publicacao,
            "query_origem": query_origem,
        }

    except Exception as e:
        print(f"❌ Erro ao processar URL: {url}")
        print(f"   Motivo: {e}")
        return None


# ============================================================
# BUSCA DE URLS
# ============================================================

def buscar_urls():
    """
    Busca URLs no DuckDuckGo usando DDGS.
    Retorna um dicionário:
    {
        url: query_origem
    }
    """
    urls_encontradas = {}

    print("🔎 Iniciando buscas...")

    try:
        with DDGS() as ddgs:
            for query in QUERIES:
                print(f"🔍 Buscando query: {query}")

                try:
                    resultados = ddgs.text(
                        query,
                        region="br-pt",
                        max_results=50
                    )

                    for resultado in resultados:
                        url = resultado.get("href")

                        if not url:
                            continue

                        if url not in urls_encontradas:
                            urls_encontradas[url] = query

                except Exception as e:
                    print(f"❌ Erro na busca da query: {query}")
                    print(f"   Motivo: {e}")

                time.sleep(random.uniform(1.0, 2.0))

    except Exception as e:
        print("❌ Erro geral ao iniciar DDGS.")
        print(f"   Motivo: {e}")

    print(f"✅ URLs únicas encontradas: {len(urls_encontradas)}")

    return urls_encontradas


# ============================================================
# RESUMO NO BANCO
# ============================================================

def exibir_resumo_banco():
    """
    Exibe resumo simples da tabela após a coleta.
    """
    sql = """
    SELECT
        COUNT(*) AS total_registros,
        COUNT(DISTINCT url) AS total_urls,
        COUNT(DISTINCT municipio) AS total_municipios,
        COUNT(DISTINCT categoria) AS total_categorias
    FROM pop_rua;
    """

    try:
        with engine.begin() as conn:
            row = conn.execute(text(sql)).mappings().first()

        print("========== RESUMO DO BANCO ==========")
        print(f"Total registros: {row['total_registros']}")
        print(f"Total URLs: {row['total_urls']}")
        print(f"Total municípios: {row['total_municipios']}")
        print(f"Total categorias: {row['total_categorias']}")

    except Exception as e:
        print(f"⚠️ Não foi possível exibir resumo do banco: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("======================================")
    print("🚀 INICIANDO COLETOR POP RUA")
    print(f"⏰ Data/hora início: {datetime.now()}")
    print("======================================")

    criar_tabela()

    urls = buscar_urls()

    if not urls:
        print("⚠️ Nenhuma URL encontrada. Finalizando.")
        return

    total_processadas = 0
    total_inseridas = 0
    total_duplicadas = 0
    total_erros = 0

    for indice, (url, query_origem) in enumerate(urls.items(), start=1):
        print("--------------------------------------")
        print(f"Processando {indice}/{len(urls)}")
        print(f"URL: {url}")

        registro = processar_noticia(url, query_origem)

        if not registro:
            total_erros += 1
            time.sleep(random.uniform(0.5, 1.5))
            continue

        total_processadas += 1

        try:
            inseriu = inserir_registro(registro)

            if inseriu:
                total_inseridas += 1
                print(f"✅ Inserido: {registro['titulo'][:100]}")
            else:
                total_duplicadas += 1
                print(f"🔁 Duplicado ignorado: {registro['titulo'][:100]}")

        except SQLAlchemyError as e:
            total_erros += 1
            print("❌ Erro ao inserir no banco.")
            print(f"   Motivo: {e}")

        time.sleep(random.uniform(0.5, 1.5))

    print("======================================")
    print("📌 RESUMO DA EXECUÇÃO")
    print("======================================")
    print(f"URLs encontradas: {len(urls)}")
    print(f"Notícias processadas: {total_processadas}")
    print(f"Novos registros inseridos: {total_inseridas}")
    print(f"Duplicados ignorados: {total_duplicadas}")
    print(f"Erros: {total_erros}")
    print(f"⏰ Data/hora fim: {datetime.now()}")

    exibir_resumo_banco()

    print("🚀 FINALIZADO")


if __name__ == "__main__":
    main()