# Data Lake e Análise de Decisão - Hospital Universitário Onofre Lopes

Projeto de implementação de Data Lake e Business Intelligence para a disciplina de Sistemas de Apoio à Decisão. O trabalho consiste na integração de dados estruturados (internações do SUS) e dados não estruturados (comentários de redes sociais) para análise de demanda hospitalar e percepção de qualidade.

## Arquitetura da Solução
A solução foi desenvolvida seguindo a arquitetura de camadas (Medallion Architecture):
* **Camada Bronze:** Ingestão de dados brutos (CSV do Governo e coleta manual do Instagram).
* **Camada Prata:** Tratamento de dados, limpeza, tipagem e anonimização.
* **Camada Ouro:** Modelagem dimensional para consumo em ferramentas de BI.

## Tecnologias
* Python 3.12
* Pandas (ETL e manipulação de dados)
* TextBlob (Processamento de Linguagem Natural e Análise de Sentimentos)
* Power BI (Dashboard e Visualização)

## Estrutura do Repositório
* `etl_hospital.py`: Script de orquestração do pipeline de dados (Leitura, Transformação e Carga).
* `comentarios_instagram.csv`: Dataset coletado manualmente contendo amostra de comentários para análise de sentimentos.

## Como Executar
1. Instale as bibliotecas Python:
   ```bash
   pip install pandas textblob unidecode openpyxl
   python -m textblob.download_corpora
Baixe os dados de Internações Hospitalares (Período Jan/2024 a Set/2025) no Portal de Dados Abertos do Governo Federal.

Posicione os arquivos CSV na mesma pasta do script e execute:

Bash

python etl_hospital.py
Os arquivos processados (prefixo OURO_) serão gerados automaticamente para importação no Power BI.
