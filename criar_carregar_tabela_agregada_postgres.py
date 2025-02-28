import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import BigInteger
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import String
import glob
import os

# Caminho de entrada dos arquivos CSV
clientes_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/clientes/*.csv"
produtos_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/produtos/*.csv"
transacoes_csv = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/transacoes/*.csv"

# Caminho de saída para os dados agregados em Parquet
caminho_parquet = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/Trusted/clientes_agregados.parquet"

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
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            print(f"[ERRO] Falha ao carregar {f}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else None

# Carregar os dados das três tabelas
clientes_df = carregar_csv(clientes_csv)
produtos_df = carregar_csv(produtos_csv)
transacoes_df = carregar_csv(transacoes_csv)

# Exibir as 5 primeiras linhas de cada tabela para verificar
print("Clientes DataFrame - 5 primeiras linhas:")
print(clientes_df.head())

print("\nProdutos DataFrame - 5 primeiras linhas:")
print(produtos_df.head())

print("\nTransações DataFrame - 5 primeiras linhas:")
print(transacoes_df.head())

# Verificar o schema de cada DataFrame (tipos de dados das colunas)
print("\nClientes DataFrame - Schema:")
print(clientes_df.dtypes)

print("\nProdutos DataFrame - Schema:")
print(produtos_df.dtypes)

print("\nTransações DataFrame - Schema:")
print(transacoes_df.dtypes)

# Verificar se as tabelas foram carregadas corretamente
if clientes_df is not None and produtos_df is not None and transacoes_df is not None:
    # Identificar as colunas de cada DataFrame
    print("\nClientes Columns:", clientes_df.columns)
    print("Produtos Columns:", produtos_df.columns)
    print("Transações Columns:", transacoes_df.columns)

    # Criar a tabela agregada
    # Confirmar estrutura da tabela de transações
    if 'id_cliente' in transacoes_df.columns and 'quantidade' in transacoes_df.columns and 'preco' in produtos_df.columns:
        # Calcular o valor da transação multiplicando quantidade por preço
        transacoes_df = transacoes_df.merge(produtos_df[['id', 'preco']], left_on='id_produto', right_on='id', how='left')
        transacoes_df['valor'] = transacoes_df['quantidade'] * transacoes_df['preco']

        # Agregar dados
        clientes_agregado_df = transacoes_df.groupby('id_cliente').agg(
            receita_total=('valor', 'sum'),
            numero_transacoes=('id_transacao', 'count')
        ).reset_index()

        # Juntar os dados de clientes e o produto mais comprado por cliente
        cliente_produto = transacoes_df.groupby(['id_cliente', 'id_produto']).agg(
            total_compras=('id_transacao', 'count')
        ).reset_index()

        # Encontrar o produto mais comprado por cliente
        produto_mais_comprado = cliente_produto.loc[
            cliente_produto.groupby('id_cliente')['total_compras'].idxmax()
        ]

        # Unir com o DataFrame de clientes
        clientes_agregado_df = clientes_agregado_df.merge(produto_mais_comprado[['id_cliente', 'id_produto']],
                                                          on='id_cliente', how='left')

        # Juntar as informações de clientes com os dados agregados
        clientes_agregado_df = clientes_agregado_df.merge(clientes_df[['id', 'nome']], left_on='id_cliente',
                                                          right_on='id', how='left')

        # Salvar o DataFrame agregado em Parquet
        diretorio = os.path.dirname(caminho_parquet)
        if not os.path.exists(diretorio):
            os.makedirs(diretorio)

        # Salvar no formato Parquet
        clientes_agregado_df.to_parquet(caminho_parquet, index=False)

        print(f"[SUCESSO] Dados agregados salvos em: {caminho_parquet}")

        # Carregar os dados agregados para o PostgreSQL
        try:
            clientes_agregado_df.to_sql(
                "clientes_agregados",
                engine,
                if_exists="replace",  
                dtype={
                    "id_cliente": BigInteger,
                    "receita_total": Float,
                    "numero_transacoes": Integer,
                    "id_produto": BigInteger,
                    "nome": String(255)
                }
            )
            print("[SUCESSO] Dados agregados inseridos na tabela 'clientes_agregados'!")
        except Exception as e:
            print(f"[ERRO] Falha ao inserir na tabela 'clientes_agregados': {e}")

# Fechar a conexão com o banco de dados
engine.dispose()

print("[FINALIZADO] Processo concluído com sucesso!")
