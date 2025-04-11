# Função Lambda para Iniciar uma Instância EC2

import json
import boto3

def lambda_handler(event, context):
    try:
        # Inicializa o cliente EC2
        ec2_client = boto3.client('ec2')
        
        # ID da instância EC2
        instance_id = 'i-XXXXXXXXXXXXXX'
        
        # Inicia a instância
        response = ec2_client.start_instances(
            InstanceIds=[instance_id]
        )
        
        # Retorna a resposta
        return {
            'statusCode': 200,
            'body': json.dumps(f'Instância {instance_id} iniciada com sucesso!')
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro ao iniciar a instância: {str(e)}')
        }