import pandas as pd
import os
import glob

# Caminho do arquivo de entrada (substitua pelo caminho correto do seu arquivo)
arquivo_entrada = "C:/Users/Belchior/Desktop/Teste Magazord/data_lake/raw_layer/produtos/*.csv"

# Caminho do diretório de saída
diretorio_saida = "C:/Users/Belchior/Desktop/Teste Magazord/data_lake/process/produtos/"

# Criar diretório de saída, se não existir
os.makedirs(diretorio_saida, exist_ok=True)

# Carregar todos os arquivos CSV do diretório
produtos_files = glob.glob(arquivo_entrada)

# Carregar os arquivos CSV para DataFrame e concatená-los
df = pd.concat([pd.read_csv(f, header=0) for f in produtos_files], ignore_index=True)

# Exibir amostra dos dados lidos
print("Amostra dos dados lidos:")
print(df.head())

#Exibir esquema 
print("\nEsquema do DataFrame:")
print(df.info())

# Converter nomes das colunas para minúsculas
df.columns = df.columns.str.lower()

# Remover espaços extras nas colunas
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# Padronizar a coluna 'nome' (primeira letra maiúscula)
if "nome" in df.columns:
    df["nome"] = df["nome"].str.title()

# Normalizar a coluna 'descricao' (remover espaços extras)
if "descricao" in df.columns:
    df["descricao"] = df["descricao"].str.strip()

# Garantir que 'preco' seja numérico (converter para float e tratar erros)
if "preco" in df.columns:
    df["preco"] = pd.to_numeric(df["preco"], errors='coerce')
    
    # Calcular a média dos preços, ignorando valores nulos
    preco_medio = df["preco"].mean()

    # Substituir valores nulos pelo preço médio (correção do aviso)
    df["preco"] = df["preco"].fillna(preco_medio)

# Garantir que 'ean' seja numérico (remover caracteres não numéricos)
if "ean" in df.columns:
    df["ean"] = df["ean"].astype(str).str.replace(r"\D", "", regex=True)
    df["ean"] = df["ean"].fillna("Desconhecido")

# Preencher valores nulos com 'Desconhecido' para 'descricao'
if "descricao" in df.columns:
    df["descricao"] = df["descricao"].fillna("Desconhecido")

# Ordenar os dados por nome, se a coluna existir
if "nome" in df.columns:
    df = df.sort_values(by="nome")

# Caminho completo para salvar o arquivo
arquivo_saida = os.path.join(diretorio_saida, "produto_process.csv")

# Salvar o DataFrame limpo
df.to_csv(arquivo_saida, index=False)

print(f"\nArquivo salvo em: {arquivo_saida}")
