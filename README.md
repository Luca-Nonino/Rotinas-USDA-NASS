# Documentação do Projeto de Pipeline de Dados do USDA-NASS

## Descrição Geral

Este projeto desenvolve uma pipeline automatizada para processar e analisar dados do Serviço Nacional de Estatísticas Agrícolas dos Estados Unidos (USDA-NASS). O objetivo é extrair, transformar e carregar dados que são fundamentais para a análise de produção agrícola e tendências do mercado.

## Estrutura do Projeto

O projeto é organizado nas seguintes pastas e arquivos principais:

- **data/**: Contém todos os dados brutos, processados e os logs.
  - **ipvs/**: Diretório para os arquivos IPV gerados.
  - **logs/**: Armazena logs de erros e de atualização.
  - **raw/**: Dados brutos coletados das APIs.
- **scripts/**: Scripts Python para tarefas específicas de processamento de dados.
  - **fetch_data.py**: Script para a coleta de dados da API do USDA-NASS.
  - **generate_ipvs.py**: Script para transformar os dados em formatos úteis para análise.
- **main.py**: Script principal que coordena as operações de coleta e processamento de dados.
- **requirements.txt**: Lista de dependências do projeto.

## Configuração e Instalação

Antes de iniciar, é necessário ter Python 3.8 ou superior instalado. Clone o repositório em seu ambiente de desenvolvimento e instale as dependências listadas em `requirements.txt` usando o comando:

```bash
pip install -r requirements.txt
```

## Execução do Projeto

Para rodar o projeto, execute o script `main.py` a partir da linha de comando:

```bash
python main.py
```

Este script coordena as seguintes operações:

1. Coleta de dados das APIs do USDA para os últimos 10 anos.
2. Transformação dos dados JSON em CSVs consolidados.
3. Geração de arquivos IPV para análises específicas.

Os arquivos IPV gerados são armazenados no diretório `data/ipvs/`, organizados por ano e mês.

## Logs

Os logs de erros são salvos em `data/logs/error_logs.txt`, e os logs de atualização em `data/logs/update_log.json`, que ajudam a rastrear a execução e possíveis problemas.

## Limpeza de Dados

Após cada ciclo de execução, o script `main.py` inclui uma rotina para limpar os diretórios de dados brutos e processados, evitando a acumulação de arquivos desnecessários e mantendo a organização do sistema de arquivos.
