import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, inspect
from sqlalchemy.types import Integer, DateTime
import glob
import psycopg2

# Caminho dos arquivos CSV
transacoes_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/transacoes/*.csv"

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
            df = pd.read_csv(f, dtype={"id_transacao": int, "id_cliente": int, "id_produto": int, "quantidade": int}, parse_dates=["data_transacao"])
            dfs.append(df)
        except Exception as e:
            print(f"[ERRO] Falha ao carregar {f}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else None

# Carregar os dados
transacoes_df = carregar_csv(transacoes_csv)

# Verificar se há dados
if transacoes_df is not None and not transacoes_df.empty:
    # Ajustar tipos para evitar erros de compatibilidade
    transacoes_df.dropna(subset=['id_transacao', 'id_cliente', 'id_produto', 'quantidade', 'data_transacao'], inplace=True)

    # Garantir que as colunas sejam do tipo int (não nulo)
    transacoes_df['id_transacao'] = transacoes_df['id_transacao'].astype(int)
    transacoes_df['id_cliente'] = transacoes_df['id_cliente'].astype(int)
    transacoes_df['id_produto'] = transacoes_df['id_produto'].astype(int)
    transacoes_df['quantidade'] = transacoes_df['quantidade'].astype(int)
    transacoes_df['data_transacao'] = pd.to_datetime(transacoes_df['data_transacao'])

    # Criar a tabela 'transacoes' se não existir
    try:
        # Definir os metadados
        metadata = MetaData()

        # Criar a tabela 'transacoes' com base no schema do DataFrame
        transacoes_table = Table('transacoes', metadata,
            Column('id_transacao', Integer, primary_key=True),
            Column('id_cliente', Integer),
            Column('id_produto', Integer),
            Column('quantidade', Integer),
            Column('data_transacao', DateTime)
        )

        # Usar o 'inspect' para verificar se a tabela existe
        inspector = inspect(engine)
        if not inspector.has_table('transacoes'):
            metadata.create_all(engine)  # Cria a tabela
            print("[SUCESSO] Tabela 'transacoes' criada com sucesso!")
        else:
            print("[AVISO] A tabela 'transacoes' já existe. Dados serão inseridos.")

        # Inserir os dados na tabela
        transacoes_df.to_sql(
            "transacoes",
            engine,
            if_exists="append",  # Adiciona os dados sem apagar a tabela
            index=False,
            dtype={
                "id_transacao": Integer,
                "id_cliente": Integer,
                "id_produto": Integer,
                "quantidade": Integer,
                "data_transacao": DateTime
            }
        )
        print("[SUCESSO] Dados inseridos na tabela 'transacoes'!")

    except Exception as e:
        print(f"[ERRO] Falha ao criar ou inserir na tabela 'transacoes': {e}")

else:
    print("[AVISO] Nenhum dado válido encontrado para inserção.")

# Fechar conexão
engine.dispose()

print("[FINALIZADO] Processo concluído com sucesso!")
