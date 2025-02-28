
O arquivo do banco de dados foi particionado, pois o tamanho total dele é de 45.233kB e nao possivel de fazer upload no github.
foi utilizado o metodo  split, para dividir o arquivo em 3 parte:
parte_1aa
parte_1ab
parte_1ac

utilizado o comando: 
split -b 22m seu_arquivo.sql parte_1

para unir as partes do arquivo, vamos utilizar o comando:  cat parte_1aa parte_1ab parte_1ac > arquivo_completo.sql

usei o git bash para fazer o processo.


