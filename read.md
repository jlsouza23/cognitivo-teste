Para executar o processo com sucesso será necessário executar os passos abaixo

1- Criar o bucket no s3: s3_cognitivo
2- Criar as subpastas: "output", "input", "config", "job_spark"
3- Upload dos arquivos conforme abaixo:
    a- load.csv - "s3://s3_cognitivo/input/load.csv"
    b- types_mapping.json - "s3://s3_cognitivo/config/types_mapping.json"
    c- job-spark.py - "s3://s3_cognitivo/script/job-spark.py"
4- Criar a lambda: create-emr-up.py 
    - Essa lambda irá subir o cluster emr e fazer o spark submit do script job-spark.py.
