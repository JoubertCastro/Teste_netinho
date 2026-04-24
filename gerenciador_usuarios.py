import os
import secrets
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.security import generate_password_hash, check_password_hash


# ============================================================
# CONFIGURAÇÕES
# ============================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL não encontrada. "
        "Configure essa variável de ambiente no Railway ou no ambiente local."
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

PERFIS_VALIDOS = {"admin", "gestor", "usuario", "visualizador"}


# ============================================================
# CRIAÇÃO DAS TABELAS
# ============================================================

def criar_tabelas_auth():
    """
    Cria as tabelas necessárias para autenticação, sessões e logs de acesso.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS usuarios_dash (
        id SERIAL PRIMARY KEY,

        nome TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,

        senha_hash TEXT NOT NULL,

        perfil VARCHAR(30) NOT NULL DEFAULT 'usuario',
        ativo BOOLEAN NOT NULL DEFAULT TRUE,

        primeiro_acesso BOOLEAN NOT NULL DEFAULT TRUE,
        senha_expirada BOOLEAN NOT NULL DEFAULT FALSE,

        criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
        atualizado_em TIMESTAMP NOT NULL DEFAULT NOW(),
        ultimo_login TIMESTAMP NULL
    );

    CREATE TABLE IF NOT EXISTS logs_acesso_dash (
        id SERIAL PRIMARY KEY,

        usuario_id INTEGER NULL REFERENCES usuarios_dash(id),
        email TEXT,

        sucesso BOOLEAN NOT NULL,
        motivo TEXT,

        ip TEXT,
        user_agent TEXT,

        criado_em TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sessoes_dash (
        id SERIAL PRIMARY KEY,

        usuario_id INTEGER NOT NULL REFERENCES usuarios_dash(id),
        token_sessao TEXT NOT NULL UNIQUE,

        criado_em TIMESTAMP NOT NULL DEFAULT NOW(),
        expira_em TIMESTAMP NOT NULL,

        ativo BOOLEAN NOT NULL DEFAULT TRUE
    );

    CREATE INDEX IF NOT EXISTS ix_usuarios_dash_email
        ON usuarios_dash (email);

    CREATE INDEX IF NOT EXISTS ix_usuarios_dash_ativo
        ON usuarios_dash (ativo);

    CREATE INDEX IF NOT EXISTS ix_logs_acesso_dash_email
        ON logs_acesso_dash (email);

    CREATE INDEX IF NOT EXISTS ix_logs_acesso_dash_usuario_id
        ON logs_acesso_dash (usuario_id);

    CREATE INDEX IF NOT EXISTS ix_logs_acesso_dash_criado_em
        ON logs_acesso_dash (criado_em);

    CREATE INDEX IF NOT EXISTS ix_sessoes_dash_token
        ON sessoes_dash (token_sessao);

    CREATE INDEX IF NOT EXISTS ix_sessoes_dash_usuario_id
        ON sessoes_dash (usuario_id);

    CREATE INDEX IF NOT EXISTS ix_sessoes_dash_expira_em
        ON sessoes_dash (expira_em);
    """

    with engine.begin() as conn:
        conn.execute(text(sql))


# ============================================================
# ADMIN INICIAL
# ============================================================

def criar_admin_inicial():
    """
    Cria um usuário admin inicial a partir das variáveis de ambiente:

    ADMIN_NOME
    ADMIN_EMAIL
    ADMIN_PASSWORD

    Exemplo local PowerShell:
    $env:ADMIN_NOME="Administrador"
    $env:ADMIN_EMAIL="admin@teste.com"
    $env:ADMIN_PASSWORD="MinhaSenhaForte123"
    python auth_manager.py init
    """
    admin_nome = os.getenv("ADMIN_NOME", "Administrador")
    admin_email = os.getenv("ADMIN_EMAIL")
    admin_password = os.getenv("ADMIN_PASSWORD")

    if not admin_email or not admin_password:
        print("⚠️ ADMIN_EMAIL e ADMIN_PASSWORD não configurados.")
        print("Configure essas variáveis de ambiente antes de criar o admin inicial.")
        return False

    admin_email = normalizar_email(admin_email)

    with engine.begin() as conn:
        existente = conn.execute(
            text("""
                SELECT id
                FROM usuarios_dash
                WHERE email = :email
                LIMIT 1;
            """),
            {"email": admin_email}
        ).mappings().first()

        if existente:
            print(f"ℹ️ Admin inicial já existe: {admin_email}")
            return False

        conn.execute(
            text("""
                INSERT INTO usuarios_dash (
                    nome,
                    email,
                    senha_hash,
                    perfil,
                    ativo,
                    primeiro_acesso,
                    senha_expirada
                )
                VALUES (
                    :nome,
                    :email,
                    :senha_hash,
                    'admin',
                    TRUE,
                    FALSE,
                    FALSE
                );
            """),
            {
                "nome": admin_nome.strip(),
                "email": admin_email,
                "senha_hash": generate_password_hash(admin_password)
            }
        )

    print(f"✅ Admin inicial criado com sucesso: {admin_email}")
    return True


# ============================================================
# UTILITÁRIOS
# ============================================================

def normalizar_email(email):
    return str(email or "").strip().lower()


def validar_perfil(perfil):
    perfil = str(perfil or "").strip().lower()

    if perfil not in PERFIS_VALIDOS:
        raise ValueError(
            f"Perfil inválido: {perfil}. "
            f"Perfis válidos: {', '.join(sorted(PERFIS_VALIDOS))}"
        )

    return perfil


def gerar_token_sessao():
    return secrets.token_urlsafe(48)


def registrar_log_acesso(
    email,
    sucesso,
    motivo,
    usuario_id=None,
    ip=None,
    user_agent=None
):
    """
    Registra tentativa de acesso, com sucesso ou falha.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO logs_acesso_dash (
                        usuario_id,
                        email,
                        sucesso,
                        motivo,
                        ip,
                        user_agent
                    )
                    VALUES (
                        :usuario_id,
                        :email,
                        :sucesso,
                        :motivo,
                        :ip,
                        :user_agent
                    );
                """),
                {
                    "usuario_id": usuario_id,
                    "email": normalizar_email(email),
                    "sucesso": sucesso,
                    "motivo": motivo,
                    "ip": ip,
                    "user_agent": user_agent
                }
            )
    except Exception as e:
        print(f"⚠️ Falha ao registrar log de acesso: {e}")


# ============================================================
# USUÁRIOS
# ============================================================

def criar_usuario(nome, email, senha, perfil="usuario", primeiro_acesso=True):
    """
    Cria um novo usuário do dashboard.
    A senha é salva somente como hash.

    Retorna o ID do novo usuário.
    """
    email = normalizar_email(email)
    perfil = validar_perfil(perfil)

    if not nome or not str(nome).strip():
        raise ValueError("Nome é obrigatório.")

    if not email:
        raise ValueError("E-mail é obrigatório.")

    if not senha or len(str(senha)) < 6:
        raise ValueError("A senha deve ter pelo menos 6 caracteres.")

    senha_hash = generate_password_hash(str(senha))

    with engine.begin() as conn:
        existente = conn.execute(
            text("""
                SELECT id
                FROM usuarios_dash
                WHERE email = :email
                LIMIT 1;
            """),
            {"email": email}
        ).mappings().first()

        if existente:
            raise ValueError(f"Já existe um usuário com o e-mail {email}.")

        row = conn.execute(
            text("""
                INSERT INTO usuarios_dash (
                    nome,
                    email,
                    senha_hash,
                    perfil,
                    ativo,
                    primeiro_acesso,
                    senha_expirada
                )
                VALUES (
                    :nome,
                    :email,
                    :senha_hash,
                    :perfil,
                    TRUE,
                    :primeiro_acesso,
                    FALSE
                )
                RETURNING id;
            """),
            {
                "nome": str(nome).strip(),
                "email": email,
                "senha_hash": senha_hash,
                "perfil": perfil,
                "primeiro_acesso": primeiro_acesso
            }
        ).mappings().first()

    return row["id"]


def listar_usuarios(apenas_ativos=False):
    """
    Lista usuários cadastrados.
    """
    sql = """
        SELECT
            id,
            nome,
            email,
            perfil,
            ativo,
            primeiro_acesso,
            senha_expirada,
            criado_em,
            atualizado_em,
            ultimo_login
        FROM usuarios_dash
    """

    if apenas_ativos:
        sql += " WHERE ativo = TRUE "

    sql += " ORDER BY id DESC;"

    with engine.begin() as conn:
        rows = conn.execute(text(sql)).mappings().all()

    return [dict(row) for row in rows]


def buscar_usuario_por_email(email):
    """
    Busca usuário por e-mail.
    """
    email = normalizar_email(email)

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                    id,
                    nome,
                    email,
                    senha_hash,
                    perfil,
                    ativo,
                    primeiro_acesso,
                    senha_expirada,
                    criado_em,
                    atualizado_em,
                    ultimo_login
                FROM usuarios_dash
                WHERE email = :email
                LIMIT 1;
            """),
            {"email": email}
        ).mappings().first()

    return dict(row) if row else None


def buscar_usuario_por_id(usuario_id):
    """
    Busca usuário por ID.
    """
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                    id,
                    nome,
                    email,
                    senha_hash,
                    perfil,
                    ativo,
                    primeiro_acesso,
                    senha_expirada,
                    criado_em,
                    atualizado_em,
                    ultimo_login
                FROM usuarios_dash
                WHERE id = :id
                LIMIT 1;
            """),
            {"id": usuario_id}
        ).mappings().first()

    return dict(row) if row else None


def atualizar_usuario(usuario_id, nome=None, email=None, perfil=None, ativo=None):
    """
    Atualiza dados básicos de um usuário.
    Só atualiza os campos informados.
    """
    usuario = buscar_usuario_por_id(usuario_id)

    if not usuario:
        raise ValueError("Usuário não encontrado.")

    novo_nome = str(nome).strip() if nome is not None else usuario["nome"]
    novo_email = normalizar_email(email) if email is not None else usuario["email"]
    novo_perfil = validar_perfil(perfil) if perfil is not None else usuario["perfil"]
    novo_ativo = bool(ativo) if ativo is not None else usuario["ativo"]

    with engine.begin() as conn:
        if novo_email != usuario["email"]:
            existente = conn.execute(
                text("""
                    SELECT id
                    FROM usuarios_dash
                    WHERE email = :email
                      AND id <> :id
                    LIMIT 1;
                """),
                {
                    "email": novo_email,
                    "id": usuario_id
                }
            ).mappings().first()

            if existente:
                raise ValueError(f"Já existe outro usuário com o e-mail {novo_email}.")

        conn.execute(
            text("""
                UPDATE usuarios_dash
                   SET nome = :nome,
                       email = :email,
                       perfil = :perfil,
                       ativo = :ativo,
                       atualizado_em = NOW()
                 WHERE id = :id;
            """),
            {
                "id": usuario_id,
                "nome": novo_nome,
                "email": novo_email,
                "perfil": novo_perfil,
                "ativo": novo_ativo
            }
        )

    return True


def alterar_senha(usuario_id, nova_senha, primeiro_acesso=False, senha_expirada=False):
    """
    Altera a senha de um usuário.
    """
    if not nova_senha or len(str(nova_senha)) < 6:
        raise ValueError("A nova senha deve ter pelo menos 6 caracteres.")

    usuario = buscar_usuario_por_id(usuario_id)

    if not usuario:
        raise ValueError("Usuário não encontrado.")

    nova_hash = generate_password_hash(str(nova_senha))

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE usuarios_dash
                   SET senha_hash = :senha_hash,
                       primeiro_acesso = :primeiro_acesso,
                       senha_expirada = :senha_expirada,
                       atualizado_em = NOW()
                 WHERE id = :id;
            """),
            {
                "id": usuario_id,
                "senha_hash": nova_hash,
                "primeiro_acesso": primeiro_acesso,
                "senha_expirada": senha_expirada
            }
        )

    return True


def resetar_senha(usuario_id, senha_temporaria):
    """
    Reseta a senha e força troca no próximo acesso.
    """
    return alterar_senha(
        usuario_id=usuario_id,
        nova_senha=senha_temporaria,
        primeiro_acesso=True,
        senha_expirada=True
    )


def ativar_usuario(usuario_id):
    return atualizar_usuario(usuario_id, ativo=True)


def desativar_usuario(usuario_id):
    return atualizar_usuario(usuario_id, ativo=False)


# ============================================================
# LOGIN E SESSÃO
# ============================================================

def autenticar_usuario(email, senha, ip=None, user_agent=None):
    """
    Valida e-mail/senha.

    Retorno em caso de sucesso:
    {
        "ok": True,
        "usuario": {...},
        "token_sessao": "..."
    }

    Retorno em caso de falha:
    {
        "ok": False,
        "motivo": "..."
    }
    """
    email = normalizar_email(email)

    usuario = buscar_usuario_por_email(email)

    if not usuario:
        registrar_log_acesso(
            email=email,
            sucesso=False,
            motivo="usuario_nao_encontrado",
            ip=ip,
            user_agent=user_agent
        )
        return {
            "ok": False,
            "motivo": "Usuário ou senha inválidos."
        }

    if not usuario["ativo"]:
        registrar_log_acesso(
            email=email,
            sucesso=False,
            motivo="usuario_inativo",
            usuario_id=usuario["id"],
            ip=ip,
            user_agent=user_agent
        )
        return {
            "ok": False,
            "motivo": "Usuário inativo."
        }

    senha_ok = check_password_hash(usuario["senha_hash"], str(senha or ""))

    if not senha_ok:
        registrar_log_acesso(
            email=email,
            sucesso=False,
            motivo="senha_invalida",
            usuario_id=usuario["id"],
            ip=ip,
            user_agent=user_agent
        )
        return {
            "ok": False,
            "motivo": "Usuário ou senha inválidos."
        }

    token = criar_sessao(usuario["id"])

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE usuarios_dash
                   SET ultimo_login = NOW(),
                       atualizado_em = NOW()
                 WHERE id = :id;
            """),
            {"id": usuario["id"]}
        )

    registrar_log_acesso(
        email=email,
        sucesso=True,
        motivo="login_sucesso",
        usuario_id=usuario["id"],
        ip=ip,
        user_agent=user_agent
    )

    usuario_limpo = usuario.copy()
    usuario_limpo.pop("senha_hash", None)

    return {
        "ok": True,
        "usuario": usuario_limpo,
        "token_sessao": token
    }


def criar_sessao(usuario_id, horas_validade=12):
    """
    Cria uma sessão no banco e retorna o token.
    """
    token = gerar_token_sessao()
    expira_em = datetime.now() + timedelta(hours=horas_validade)

    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO sessoes_dash (
                    usuario_id,
                    token_sessao,
                    expira_em,
                    ativo
                )
                VALUES (
                    :usuario_id,
                    :token_sessao,
                    :expira_em,
                    TRUE
                );
            """),
            {
                "usuario_id": usuario_id,
                "token_sessao": token,
                "expira_em": expira_em
            }
        )

    return token


def validar_sessao(token_sessao):
    """
    Valida se a sessão existe, está ativa e não expirou.
    Retorna os dados do usuário sem senha_hash.
    """
    if not token_sessao:
        return None

    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT
                    s.id AS sessao_id,
                    s.token_sessao,
                    s.expira_em,
                    s.ativo AS sessao_ativa,

                    u.id AS usuario_id,
                    u.nome,
                    u.email,
                    u.perfil,
                    u.ativo AS usuario_ativo,
                    u.primeiro_acesso,
                    u.senha_expirada,
                    u.ultimo_login
                FROM sessoes_dash s
                INNER JOIN usuarios_dash u
                    ON u.id = s.usuario_id
                WHERE s.token_sessao = :token_sessao
                  AND s.ativo = TRUE
                  AND s.expira_em > NOW()
                  AND u.ativo = TRUE
                LIMIT 1;
            """),
            {"token_sessao": token_sessao}
        ).mappings().first()

    if not row:
        return None

    return dict(row)


def encerrar_sessao(token_sessao):
    """
    Encerra uma sessão específica.
    """
    if not token_sessao:
        return False

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE sessoes_dash
                   SET ativo = FALSE
                 WHERE token_sessao = :token_sessao;
            """),
            {"token_sessao": token_sessao}
        )

    return True


def encerrar_sessoes_usuario(usuario_id):
    """
    Encerra todas as sessões ativas de um usuário.
    Útil ao resetar senha ou desativar usuário.
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE sessoes_dash
                   SET ativo = FALSE
                 WHERE usuario_id = :usuario_id
                   AND ativo = TRUE;
            """),
            {"usuario_id": usuario_id}
        )

    return True


def limpar_sessoes_expiradas():
    """
    Marca como inativas todas as sessões expiradas.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE sessoes_dash
                   SET ativo = FALSE
                 WHERE expira_em <= NOW()
                   AND ativo = TRUE;
            """)
        )

    return result.rowcount


# ============================================================
# LOGS
# ============================================================

def listar_logs_acesso(limit=100, email=None, apenas_falhas=False):
    """
    Lista logs de acesso.
    """
    params = {
        "limit": int(limit)
    }

    filtros = []

    if email:
        filtros.append("email = :email")
        params["email"] = normalizar_email(email)

    if apenas_falhas:
        filtros.append("sucesso = FALSE")

    where_sql = ""

    if filtros:
        where_sql = " WHERE " + " AND ".join(filtros)

    sql = f"""
        SELECT
            id,
            usuario_id,
            email,
            sucesso,
            motivo,
            ip,
            user_agent,
            criado_em
        FROM logs_acesso_dash
        {where_sql}
        ORDER BY criado_em DESC
        LIMIT :limit;
    """

    with engine.begin() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return [dict(row) for row in rows]


# ============================================================
# CLI SIMPLES PARA TESTES
# ============================================================

def imprimir_usuarios():
    usuarios = listar_usuarios()

    if not usuarios:
        print("Nenhum usuário cadastrado.")
        return

    print("========== USUÁRIOS ==========")

    for u in usuarios:
        print(
            f"ID: {u['id']} | "
            f"Nome: {u['nome']} | "
            f"E-mail: {u['email']} | "
            f"Perfil: {u['perfil']} | "
            f"Ativo: {u['ativo']} | "
            f"Último login: {u['ultimo_login']}"
        )


def imprimir_logs():
    logs = listar_logs_acesso(limit=50)

    if not logs:
        print("Nenhum log encontrado.")
        return

    print("========== LOGS ==========")

    for log in logs:
        print(
            f"{log['criado_em']} | "
            f"E-mail: {log['email']} | "
            f"Sucesso: {log['sucesso']} | "
            f"Motivo: {log['motivo']} | "
            f"IP: {log['ip']}"
        )


def executar_cli():
    """
    Comandos úteis:

    python auth_manager.py init
    python auth_manager.py usuarios
    python auth_manager.py logs

    Criar usuário via variáveis:
    $env:NOVO_NOME="Usuário Teste"
    $env:NOVO_EMAIL="usuario@teste.com"
    $env:NOVO_SENHA="123456"
    $env:NOVO_PERFIL="usuario"
    python auth_manager.py criar_usuario
    """
    import sys

    comando = sys.argv[1].strip().lower() if len(sys.argv) > 1 else "help"

    if comando == "init":
        criar_tabelas_auth()
        criar_admin_inicial()
        print("✅ Estrutura de autenticação inicializada.")

    elif comando == "usuarios":
        criar_tabelas_auth()
        imprimir_usuarios()

    elif comando == "logs":
        criar_tabelas_auth()
        imprimir_logs()

    elif comando == "criar_usuario":
        criar_tabelas_auth()

        nome = os.getenv("NOVO_NOME")
        email = os.getenv("NOVO_EMAIL")
        senha = os.getenv("NOVO_SENHA")
        perfil = os.getenv("NOVO_PERFIL", "usuario")

        usuario_id = criar_usuario(
            nome=nome,
            email=email,
            senha=senha,
            perfil=perfil,
            primeiro_acesso=True
        )

        print(f"✅ Usuário criado com sucesso. ID: {usuario_id}")

    elif comando == "limpar_sessoes":
        criar_tabelas_auth()
        qtd = limpar_sessoes_expiradas()
        print(f"✅ Sessões expiradas inativadas: {qtd}")

    else:
        print("Comandos disponíveis:")
        print("  python auth_manager.py init")
        print("  python auth_manager.py usuarios")
        print("  python auth_manager.py logs")
        print("  python auth_manager.py criar_usuario")
        print("  python auth_manager.py limpar_sessoes")


if __name__ == "__main__":
    try:
        executar_cli()
    except SQLAlchemyError as e:
        print(f"❌ Erro de banco de dados: {e}")
    except Exception as e:
        print(f"❌ Erro: {e}")
