# Data Lake & Analytics - Hospital Privado vs. SUS

Repositório contendo a implementação do trabalho da disciplina de **Sistemas de Apoio à Decisão**. O projeto consiste na construção de um Data Lake para análise de internações hospitalares (SUS) e percepção de marca em redes sociais (Instagram).

## 🛠️ Tecnologias Utilizadas
- **Python 3.12**
- **Pandas** (Manipulação de dados e ETL)
- **TextBlob** (Análise de Sentimentos/NLP)
- **Power BI** (Dashboard e Visualização)

## 📂 Estrutura do Projeto
- `etl_hospital.py`: Script principal que realiza a leitura (Bronze), tratamento (Prata) e modelagem (Ouro) dos dados.
- `comentarios_instagram.csv`: Base de dados coletada manualmente contendo comentários de redes sociais.

## 🚀 Como Rodar
1. Instale as dependências:
   ```bash
   pip install pandas textblob unidecode openpyxl
   python -m textblob.download_corpora
