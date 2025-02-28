# Teste para Vaga de Engenheiro de Dados - Magazord

## Data Lake Simulation

Este repositório simula um **Data Lake** com três camadas principais: **raw_layer**, **process** e **trusted**. O objetivo do projeto é demonstrar como construir um pipeline de ingestão e transformação de dados usando scripts Python para manipulação de arquivos CSV e carregamento em um banco de dados.

## Estrutura do Data Lake

### **raw_layer**:
Contém os dados brutos fornecidos pela atividade.

### **process**:
Contém os dados processados, ou seja, os dados das tabelas "clientes", "produtos" e "transacoes" após passarem por um processo de limpeza e transformação.

### **trusted**:
Contém os dados agregados para análises mais performáticas:
- Receita total por cliente.
- Número total de transações por cliente.
- Produto mais comprado por cliente.

## Descrição dos Scripts

### **1. Scripts de Limpeza e Transformação (camada `process`)**

Esses scripts são responsáveis pela transformação dos dados das tabelas "clientes", "produtos" e "transacoes". Cada tabela é processada de forma independente, o que facilita o controle e manutenção do código.

#### Exemplo do script `limpar_transformacao_nome_da_tabela.py`:

1. **Importação das bibliotecas**:
   - `pandas`: Para manipulação dos dados.
   - `os`: Para manipulação de diretórios.
   - `glob`: Para buscar arquivos dentro do diretório.

2. **Definição dos Caminhos**:
   - `arquivo_entrada`: Define a localização dos arquivos CSV brutos.
   - `diretorio_saida`: Define onde o arquivo processado será salvo.

3. **Carregamento dos Arquivos CSV**:
   - Lê e combina os arquivos CSV em um único DataFrame usando `glob` e `pandas`.

4. **Normalização dos Dados**:
   - Padroniza os nomes das colunas (para minúsculas e remove espaços extras).
   - Converte tipos de dados, como IDs e quantidades para inteiros.
   - Trata a coluna de data para garantir o formato correto.

5. **Organização e Exportação**:
   - Ordena os dados e realiza diversas transformações, como remoção de duplicatas e preenchimento de valores nulos.
   - Salva os dados transformados em um novo arquivo CSV.

### **2. Scripts de Criação e Carregamento no Banco de Dados**

Após a limpeza e transformação dos dados, os dados são carregados no banco de dados "datamart" para serem usados em análises e relatórios.

#### Exemplo do script `criar_carregar_nome_da_tabela.py`:

1. **Importação das Bibliotecas**:
   - `pandas`: Para manipulação dos dados.
   - `sqlalchemy`: Para conexão com o banco de dados e inserção de dados.
   - `psycopg2`: Para conexão com PostgreSQL.

2. **Direcionamento dos Dados**:
   - Define o caminho para os dados processados.

3. **Conexão com o Banco de Dados**:
   - Conecta-se ao banco de dados "datamart" usando `SQLAlchemy`.

4. **Carregamento dos Dados**:
   - Carrega os dados processados e insere-os no banco de dados usando `pandas.DataFrame.to_sql`.

5. **Finalização**:
   - Exibe uma mensagem de sucesso ao final do carregamento.

### **3. Script de Criação e Agregação dos Dados - `criar_carregar_tabela_agregada_postgres.py`**

#### Objetivo:
Criar uma nova tabela agregada com:
- **Receita total por cliente**
- **Número total de transações por cliente**
- **Produto mais comprado por cliente**

Carregar essa tabela agregada no banco de dados **datamart** e salvar os dados no formato **Parquet** na camada **trusted** para análises mais performáticas.

#### Explicação do Código:

1. **Importação das Bibliotecas**:
   - `pandas`: Para manipulação dos dados.
   - `sqlalchemy`: Para conectar ao banco de dados PostgreSQL.
   - `psycopg2`: Para a conexão com PostgreSQL.

2. **Carregamento dos Dados**:
   - Os dados processados das tabelas `clientes`, `transacoes` e `produtos` são carregados em DataFrames do `pandas`.

3. **Agregação dos Dados**:
   - **Receita total por cliente**: Somamos o valor das transações por `cliente_id`.
   - **Número total de transações por cliente**: Contamos o número de transações por `cliente_id`.
   - **Produto mais comprado por cliente**: Contamos a quantidade de cada produto comprado por `cliente_id` e escolhemos o produto com a maior quantidade comprada.

4. **Mesclagem dos DataFrames**:
   - As tabelas agregadas são mescladas em um único DataFrame, utilizando o `cliente_id` como chave de junção.

5. **Conexão com o Banco de Dados**:
   - Utilizamos `SQLAlchemy` para conectar ao banco de dados **datamart** e carregamos os dados agregados na tabela **clientes_aggregados**.

6. **Salvar os Dados no Banco de Dados**:
   - O método `to_sql()` do `pandas` é utilizado para criar a tabela no banco de dados e inserir os dados.

7. **Salvar os Dados no Formato Parquet**:
   - Os dados agregados são salvos no formato **Parquet** na camada **trusted** para garantir uma análise mais performática em etapas futuras.

8. **Mensagem de Conclusão**:
   - Após a execução, o script exibe uma mensagem confirmando a criação da tabela no banco e o arquivo Parquet.

---
