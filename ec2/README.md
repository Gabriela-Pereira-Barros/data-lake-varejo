# Preparação Instância EC2

### Configurações ao subir instância na AWS

- Nome: airflow_podacademy
- Sistema Operacional: Ubuntu
- Tipo de Instância: t3.medium
- Criar novo par de chaves: (.pem)
- Executar a instância

### Conectar à instância EC2 (terminal máquina local)

Na pasta em que o arquivo da chave SSH (.pem) estiver:

Mudar a permissão do arquivo da chave SSH:
```bash
chmod 400 chave.pem  
```
Conectar à instância:
```bash
ssh -i chave.pem ubuntu@<Public_DNS>
```
Para conferir se está em /home/ubuntu:
```bash
pwd 
```

### Instalar Miniconda

```bash
mkdir miniconda
cd miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash ./Miniconda3-latest-Linux-x86_64.sh
```
Sair da instância: Ctrl + D  

Conectar à instância novamente.

```bash
~/miniconda3/bin/conda init bash
```
Sair da instância: Ctrl + D  

Conectar à instância novamente.

```bash
export PATH=$PATH:~/miniconda3/bin/
conda create --name airflow_env python=3.11 -y
conda activate airflow_env
```
### Instalar Airflow

```bash
pip install "apache-airflow[celery]==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt"
airflow db init
airflow users create --username admin --password admin --role Admin --firstname admin --lastname admin --email admin@email.com
airflow webserver -D
airflow scheduler -D
```
Liberar porta e IP na instância para que seja possível acessar o Airflow:

&emsp; Na instância, na aba Security, clicar no grupo de segurança e editar regras de entrada, adicionando nova regra:

&emsp;&emsp; TCP Porta 8080 no meu IP

Para acessar o airflow webserver:

&emsp; Copiar DNS público e adcionar porta :8080 no navegador.

### Instalar aws cli (para copiar dag de um bucket s3 para dentro da instância)

```bash
sudo snap install aws-cli --classic
aws configure
```
Preencher:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name

Copiar arquivos do bucket s3 para a instancia ec2:

```bash
aws s3 sync s3://bucket_com_dag /home/ubuntu/airflow/dags
```

Instalar boto3:

```bash
pip install boto3
```

### Configurar instância para que o airflow inicie aumtomaticamente ao ligar a instância

Executar os comandos dentro da instância:

Criar script para a inicialização:
```bash
sudo nano /home/ubuntu/start_airflow.sh
```

Conteúdo do script:
```bash
#!/bin/bash
export AIRFLOW_HOME=/home/ubuntu/airflow
source /home/ubuntu/miniconda3/bin/activate airflow_env
airflow scheduler -D
exec airflow webserver --port 8080
```

Tornar o script executável:
```bash
sudo chmod +x /home/ubuntu/start_airflow.sh
```

Testar script:
```bash
bash /home/ubuntu/start_airflow.sh
```

Criar um arquivo de serviço systemd
```bash
sudo nano /etc/systemd/system/airflow.service
```

Conteúdo do arquivo:
```bash
[Unit]
Description=Airflow webserver and scheduler
After=network-online.target
Wants=network-online.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
Environment="PATH=/home/ubuntu/miniconda3/envs/airflow_env/bin:/home/ubuntu/miniconda3/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/home/ubuntu/start_airflow.sh
Restart=always
Type=simple

[Install]
WantedBy=multi-user.target
```

Habilitar o serviço:
```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow.service
```

Iniciar o serviço para testá-lo:
```bash
sudo systemctl start airflow.service
```

Verificar o status do serviço (para confirmar que está funcionando):
```bash
sudo systemctl status airflow.service
```
