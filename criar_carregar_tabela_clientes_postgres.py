import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, BigInteger
import glob
import psycopg2

# Caminho dos arquivos CSV
clientes_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/clientes/*.csv"

# Configuração do PostgreSQL
usuario = "postgres"
senha = "postgres"
host = "localhost"
porta = "5432"
banco = "datamart"

# Criando conexão com PostgreSQL
url = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}"

try:
    engine = create_engine(url, pool_pre_ping=True)
    print("[OK] Conexão com o PostgreSQL estabelecida.")
except Exception as e:
    print(f"[ERRO] Não foi possível conectar ao PostgreSQL: {e}")
    exit(1)

# Função para carregar múltiplos arquivos CSV
def carregar_csv(caminho):
    arquivos = glob.glob(caminho)
    if not arquivos:
        print(f"[AVISO] Nenhum arquivo encontrado em: {caminho}")
        return None

    dfs = []
    for f in arquivos:
        try:
            # Ajuste aqui: Não usaremos 'parse_dates' já que não há coluna de data
            df = pd.read_csv(f, dtype={"id": int, "nome": str, "sobrenome": str, "email": str, "telefone": str})
            dfs.append(df)
        except Exception as e:
            print(f"[ERRO] Falha ao carregar {f}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else None

# Carregar os dados
clientes_df = carregar_csv(clientes_csv)

# Verificar se há dados
if clientes_df is not None and not clientes_df.empty:
    # Ajustar tipos para evitar erros de compatibilidade
    clientes_df.dropna(subset=['id', 'nome', 'sobrenome', 'email', 'telefone'], inplace=True)

    # Garantir que as colunas sejam do tipo correto
    clientes_df['id'] = clientes_df['id'].astype(int)
    clientes_df['nome'] = clientes_df['nome'].astype(str)
    clientes_df['sobrenome'] = clientes_df['sobrenome'].astype(str)
    clientes_df['email'] = clientes_df['email'].astype(str)
    clientes_df['telefone'] = clientes_df['telefone'].astype(str)

    # Inserir no PostgreSQL
    try:
        clientes_df.to_sql(
            "clientes",
            engine,
            if_exists="append",  # Garantir que os dados sejam adicionados à tabela existente
            index=False,
            dtype={
                "id": BigInteger,  # Ajustado para BigInt
                "nome": String(255),
                "sobrenome": String(255),
                "email": String(255),
                "telefone": String(20),  # Ajuste de tamanho para o telefone
            }
        )
        print("[SUCESSO] Dados inseridos na tabela 'clientes'!")
    except Exception as e:
        print(f"[ERRO] Falha ao inserir na tabela 'clientes': {e}")

else:
    print("[AVISO] Nenhum dado válido encontrado para inserção.")

# Fechar conexão
engine.dispose()

print("[FINALIZADO] Processo concluído com sucesso!")
