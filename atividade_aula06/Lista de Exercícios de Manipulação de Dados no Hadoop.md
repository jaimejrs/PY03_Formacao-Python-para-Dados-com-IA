Aqui está o arquivo Markdown consolidado com os comandos utilizados na sua atividade prática:

# Exercícios de Manipulação de Dados no Hadoop

### 1. Enviar o Arquivo para o HDFS

* 
**Tarefa:** Faça o upload do arquivo `amazon1.csv` para o diretório `/data` no HDFS.


* 
**Comando esperado:** `hdfs dfs -put /opt/amazon1.csv /data/` 



### 2. Listar os Arquivos no Diretório

* 
**Tarefa:** Liste os arquivos no diretório `/data` para confirmar que o arquivo foi enviado.


* 
**Comando esperado:** `hdfs dfs -ls /data` 



### 3. Exibir as Primeiras Linhas do Arquivo

* 
**Tarefa:** Exiba as primeiras 10 linhas do arquivo `amazon1.csv` diretamente no HDFS.


* 
**Comando esperado:** `hdfs dfs -cat /data/amazon1.csv | head -n 10` 



### 4. Filtrar Dados por Categoria

* 
**Tarefa:** Utilize `grep` para encontrar todas as linhas que pertencem à categoria `Computers&Accessories`.


* 
**Comando esperado:** `hdfs dfs -cat /data/amazon1.csv | grep "Computers&Accessories"` 



### 5. Contar o Número Total de Linhas

* **Tarefa:** Conte quantas linhas existem no arquivo `amazon1.csv`.
* **Comando esperado:** `hdfs dfs -cat /data/amazon1.csv | wc -l`

### 6. Copiar o Arquivo para um Diretório com seu nome

* 
**Tarefa:** Copie o arquivo `amazon1.csv` do diretório `/data` para o diretório `/jaime`.


* 
**Comando esperado:** `hdfs dfs -mkdir /jaime` `hdfs dfs -cp /data/amazon1.csv /jaime/` 



### 7. Remover Arquivos

* 
**Tarefa:** Delete o arquivo `amazon1.csv` no diretório `/backup`.


* 
**Comando esperado:** `hdfs dfs -rm /backup/amazon1.csv` 



### 8. Verificar o Espaço Ocupado pelo Arquivo

* 
**Tarefa:** Verifique o espaço total ocupado pelo arquivo `amazon1.csv` no HDFS.


* 
**Comando esperado:** `hdfs dfs -du -h /data/amazon1.csv` 


