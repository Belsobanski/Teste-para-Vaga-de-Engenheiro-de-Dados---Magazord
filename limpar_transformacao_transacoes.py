import pandas as pd
import os
import glob

# Caminho do diretório de entrada (onde estão os arquivos CSV)
arquivo_entrada = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/raw_layer/transacoes/*.csv"

# Caminho do diretório de saída
diretorio_saida = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/transacoes"

# Criar diretório de saída, se não existir
os.makedirs(diretorio_saida, exist_ok=True)

# Carregar todos os arquivos CSV do diretório
transacoes_files = glob.glob(arquivo_entrada)

# Verificar se existem arquivos para processar
if not transacoes_files:
    print("Nenhum arquivo CSV encontrado no diretório de entrada.")
    exit()

print(f"{len(transacoes_files)} arquivos encontrados. Iniciando processamento...")

# Carregar os arquivos CSV para DataFrame e concatená-los
df = pd.concat([pd.read_csv(f, header=0) for f in transacoes_files], ignore_index=True)

# Exibir amostra dos dados lidos
print("Amostra dos dados lidos:")
print(df.head())

# Exibir esquema do DataFrame
print("\nEsquema do DataFrame:")
df.info()

# Converter nomes das colunas para minúsculas e remover espaços extras
df.columns = df.columns.str.lower().str.strip()

# Converter tipos de dados com tratamento de erro
df["id_transacao"] = pd.to_numeric(df["id_transacao"], errors="coerce").fillna(0).astype(int)
df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce").fillna(0).astype(int)
df["id_produto"] = pd.to_numeric(df["id_produto"], errors="coerce").fillna(0).astype(int)
df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0).astype(int)

# Tentar converter a coluna data_transacao para datetime, tratando erros e forçando a conversão
df["data_transacao"] = pd.to_datetime(df["data_transacao"], errors="coerce", format="%Y-%m-%d %H:%M:%S")

# Verificar se a conversão foi realizada com sucesso
print("\nApós conversão para datetime:")
df["data_transacao"] = df["data_transacao"].fillna(pd.Timestamp("2000-01-01 00:00:00"))

# Exibir tipo de dados novamente para confirmar que a conversão foi realizada
df.info()

# Ordenar os dados por data_transacao
df = df.sort_values(by="data_transacao")

# Caminho completo para salvar o arquivo processado
arquivo_saida = os.path.join(diretorio_saida, "transacoes_process.csv")

# Salvar o DataFrame limpo, garantindo que a coluna de data seja salva corretamente
df.to_csv(arquivo_saida, index=False, date_format="%Y-%m-%d %H:%M:%S")

print(f"\nArquivo salvo em: {arquivo_saida}")
