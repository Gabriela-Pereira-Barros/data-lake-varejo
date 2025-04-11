# Função Lambda para Parar uma Instância EC2

import json
import boto3

def lambda_handler(event, context):
    try:
        # Inicializa o cliente EC2
        ec2_client = boto3.client('ec2')
        
        # ID da instância EC2
        instance_id = 'i-XXXXXXXXXXXXXX'
        
        # Para a instância
        response = ec2_client.stop_instances(
            InstanceIds=[instance_id]
        )
        
        # Retorna a resposta
        return {
            'statusCode': 200,
            'body': json.dumps(f'Instância {instance_id} parada com sucesso!')
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Erro ao parar a instância: {str(e)}')
        }