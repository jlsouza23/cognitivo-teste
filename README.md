# # cognitivo-teste
# Para executar o processo com sucesso será necessário executar os passos abaixo

0- git clone do repositorio<br />
1- Criar o bucket no s3: s3_cognitivo
2- Criar as subpastas: "output", "input", "config", "job_spark"
3- Upload dos arquivos conforme abaixo:
    a- load.csv - "s3://s3_cognitivo/input/load.csv"
    b- types_mapping.json - "s3://s3_cognitivo/config/types_mapping.json"
    c- job-spark.py - "s3://s3_cognitivo/script/job-spark.py"
4- Criar a lambda: create-emr-up.py 
    - Essa lambda irá subir o cluster emr e fazer o spark submit do script job-spark.py.

# Outra forma para executar o script é seguir os passos acima ate no numero 3, subir um cluster EMR e fazer o submit do script job-spark.py

Esse script seguira os seguintes passos:
1 - carregar o csv em um dataframe
2 - converter de csv para parquet
4 - selecionar 3 campos do dataframe inicial
5 - salvar esse dataframe com 3 campos
6 - criar a estutura usada para converter o schema do parquet de acordo com o json de configuração
7 - carregar o parquet com o novo schema
