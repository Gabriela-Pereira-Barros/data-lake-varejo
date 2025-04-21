# Data Lake Varejo
Este repositório apresenta os códigos criados para o projeto de um Data Lake desenvolvido com dados de varejo no curso de Engenharia de Dados da [PoD Academy](https://www.podacademy.com.br/).<br><br>

Nas pastas do repositório são encontrados os seguintes conteúdos:

- **dags**: A dag do airflow que contém o pipeline de todo o processo de transformação da camada raw até a camada refined.

- **ec2**: As instruções para configuração da instância EC2 em que o airflow será executado.
  
- **lambda_functions**: O arquivo .zip para configuração da função de ingestão e as funções para ligar e interromper a instância EC2, assim como as respectivas políticas que devem ser configuradas para acesso aos recursos.
  
- **scripts_emr**: Os scripts de processamento das camadas, assim como o script bootstrap.sh necessário para a criação do cluster EMR.<br><br>

O Data Lake possui a seguinte arquitetura:

![data-lake-varejo drawio](https://github.com/user-attachments/assets/176549a1-592c-4b68-bf86-fcdb3361be69)<br><br>

Para conhecer mais sobre o desenvolvimento do projeto acesse o artigo do Medium em:

https://medium.com/@gabriela-pereira-barros/data-lake-varejo-aa685a87435d
