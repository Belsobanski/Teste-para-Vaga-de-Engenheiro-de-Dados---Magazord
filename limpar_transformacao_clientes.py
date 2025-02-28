import pandas as pd
import os
import glob

# Caminho do arquivo de entrada
arquivo_entrada = "C:/Users/Belchior/Desktop/Teste Magazord/data_lake/raw_layer/clientes/*.csv"

# Caminho do diretório de saída
diretorio_saida = "C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/clientes/"

# Criar diretório se não existir
os.makedirs(diretorio_saida, exist_ok=True)

# Carregar todos os arquivos CSV do diretório
clientes_files = glob.glob(arquivo_entrada)

# Carregar os arquivos CSV para DataFrame e concatená-los
df = pd.concat([pd.read_csv(f, header=0) for f in clientes_files], ignore_index=True)

# Exibir as primeiras linhas para verificação
print("Amostra dos dados lidos:")
print(df.head())
print("\nEsquema do DataFrame:")
print(df.info())

# Converter nomes das colunas para minúsculas
df.columns = df.columns.str.lower()

# Remover espaços extras nas colunas
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Normalizar nomes próprios (Primeira letra maiúscula, restante minúscula)
df["nome"] = df["nome"].str.title()
df["sobrenome"] = df["sobrenome"].str.title()

# Padronizar e-mails em minúsculas e remover aspas
df["email"] = df["email"].str.lower().str.replace(r'\"', '', regex=True)

# Validar e-mails (remover os inválidos)
df = df[df["email"].str.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", na=False)]

# Padronizar telefones: remover caracteres não numéricos e formatar
df["telefone"] = df["telefone"].str.replace(r"\D", "", regex=True)
df["telefone"] = df["telefone"].apply(lambda x: f"({x[:2]}) {x[2:7]}-{x[7:]}" if len(x) == 11 else f"({x[:2]}) {x[2:6]}-{x[6:]}" if len(x) == 10 else "Inválido")

# Remover telefones inválidos
df = df[df["telefone"] != "Inválido"]

# Remover duplicatas com base no e-mail
df = df.drop_duplicates(subset=["email"], keep="first")

# Preencher valores nulos com 'Desconhecido'
df = df.fillna("Desconhecido")

# Ordenar os dados por nome
df = df.sort_values(by="nome")

# Caminho completo do arquivo de saída
arquivo_saida = os.path.join(diretorio_saida, "clientes_process.csv")

# Salvar o DataFrame limpo
df.to_csv(arquivo_saida, index=False)

print(f"Arquivo salvo em: {arquivo_saida}")


