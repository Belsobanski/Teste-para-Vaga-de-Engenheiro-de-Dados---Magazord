
import pandas as pd

# Caminho do arquivo CSV
csv_path = r"C:/Users/Belchior/Desktop/Teste Magazord/data_lake/trusted/clientes_agregados.csv" 

# Ler o arquivo CSV
df = pd.read_csv(csv_path)

# Exibir as 10 primeiras linhas
print("Primeiras 10 linhas do CSV:")
print(df.head(10))

# Contar o número de registros
num_registros = df.shape[0]
print(f"\nNúmero de registros: {num_registros}")

# Exibir o schema (tipos de dados das colunas)
print("\nSchema do DataFrame (tipos de dados):")
print(df.dtypes)

# Verificar se há valores nulos
print("\nValores nulos por coluna:")
print(df.isnull().sum())

# Verificar se há registros duplicados
print("\nNúmero de registros duplicados:")
print(df.duplicated().sum())
