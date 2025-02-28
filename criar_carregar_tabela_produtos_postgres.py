import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, Float, BigInteger
import glob
import psycopg2

# Caminho dos arquivos CSV
produtos_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/produtos/*.csv"

# Configuração do PostgreSQL
usuario = "postgres"
senha = "postgres"
host = "localhost"
porta = "5432"
banco = "datamart"

# Criando conexão com PostgreSQL
url = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}"
engine = create_engine(url, pool_pre_ping=True)

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
            df = pd.read_csv(f, dtype={"id": int, "nome": str, "descricao": str, "preco": float, "ean": int})
            dfs.append(df)
        except Exception as e:
            print(f"[ERRO] Falha ao carregar {f}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else None

# Carregar os dados
produtos_df = carregar_csv(produtos_csv)

# Verificar se há dados
if produtos_df is not None and not produtos_df.empty:
    # Ajustar tipos para evitar erros de compatibilidade
    produtos_df.dropna(subset=['id', 'nome', 'descricao', 'preco', 'ean'], inplace=True)

    # Garantir que as colunas sejam do tipo correto
    produtos_df['id'] = produtos_df['id'].astype(int)
    produtos_df['nome'] = produtos_df['nome'].astype(str)
    produtos_df['descricao'] = produtos_df['descricao'].astype(str)
    produtos_df['preco'] = produtos_df['preco'].astype(float)
    produtos_df['ean'] = produtos_df['ean'].astype(int)

    # Inserir no PostgreSQL
    try:
        produtos_df.to_sql(
            "produtos",
            engine,
            if_exists="append",  # Garantir que os dados sejam adicionados à tabela existente
            index=False,
            dtype={
                "id": BigInteger,  # Ajustado para BigInt
                "nome": String(255),
                "descricao": String(1024),
                "preco": Float,
                "ean": BigInteger  # Ajustado para BigInt
            }
        )
        print("[SUCESSO] Dados inseridos na tabela 'produtos'!")
    except Exception as e:
        print(f"[ERRO] Falha ao inserir na tabela 'produtos': {e}")

else:
    print("[AVISO] Nenhum dado válido encontrado para inserção.")

# Fechar conexão
engine.dispose()

print("[FINALIZADO] Processo concluído com sucesso!")
