import requests
import csv
from io import StringIO
import os
import json
from datetime import datetime
import subprocess
import time

def fetch_crop_production_data(api_key):
    current_date = datetime.now()  # Obtém a data atual
    market_year = current_date.year - 1 if current_date.month <= 5 else current_date.year  # Determina o ano de mercado baseado no mês atual
    filename = os.path.join("data", "raw", f"Crop_Production_{market_year}.csv")  # Define o caminho do arquivo com o ano de mercado

    os.makedirs(os.path.dirname(filename), exist_ok=True)  # Cria diretórios necessários se não existirem

    base_url = 'https://quickstats.nass.usda.gov/api/api_GET/'  # URL base da API
    commodities = ['CORN', 'SOYBEANS']  # Lista de commodities
    categories = ['PRODUCTION']  # Lista de categorias
    last_updated = '1900-01-01 00:00:00.000'  # Inicializa a data da última atualização

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        is_first_write = True  # Sinalizador para controle da primeira escrita

        for commodity in commodities:
            for category in categories:
                params = {
                    'key': api_key,
                    'domain_desc': 'TOTAL',
                    'sector_desc': 'CROPS',
                    'group_desc': 'FIELD CROPS',
                    'commodity_desc': commodity,
                    'statisticcat_desc': category,
                    'agg_level_desc': 'STATE',
                    #'year': str(market_year),
                    'year': 2023,
                    'format': 'csv'
                }  # Parâmetros para requisição na API

                response = requests.get(base_url, params=params)  # Realiza a requisição para a API

                if response.status_code == 200:
                    csv_file = StringIO(response.text)
                    reader = csv.DictReader(csv_file)

                    if is_first_write:  # Confere se é a primeira gravação
                        writer.writerow(reader.fieldnames)  # Escreve os nomes dos campos
                        is_first_write = False

                    for row in reader:
                        writer.writerow(row.values())  # Escreve os valores no arquivo CSV
                        if row['load_time']:
                            last_updated = max(last_updated, row['load_time'], key=lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))  # Atualiza a última data de carregamento

    return last_updated  # Retorna a última data de atualização

def update_log(load_time):
    if load_time:
        date = datetime.strptime(load_time, "%Y-%m-%d %H:%M:%S.%f")
        log_data = {
            "LAST_UPDATED": {
                "YEAR": str(date.year),
                "MONTH": str(date.month).zfill(2),
                "DAY": str(date.day).zfill(2)
            }
        }  # Dados para registro no log

        log_filename = os.path.join("data","logs", "update_log.json")
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)
        with open(log_filename, 'r') as log_file:
            data = json.load(log_file)
        with open(log_filename, 'w') as f:
            json.dump(log_data, f, indent=4)  # Salva os dados no log
    else:
        print("No load_time data available to update the log.")

def check_data_update():
    # Checa o log de atualização para obter a última data de atualização
    log_filename = os.path.join("data","logs", "update_log.json")
    try:
        with open(log_filename, 'r') as f:
            log_data = json.load(f)
        log_date_str = f"{log_data['LAST_UPDATED']['YEAR']}-{log_data['LAST_UPDATED']['MONTH']}-{log_data['LAST_UPDATED']['DAY']}"
        log_date = datetime.strptime(log_date_str, "%Y-%m-%d").date()
        return log_date
    except Exception as e:
        print(f"Erro ao ler o log de atualização: {e}")
        # Se houver um erro na leitura do log, considera-se que os dados não estão atualizados
        return datetime.min.date()

start_time = time.time()
api_key = '1E0BC2ED-A9F9-367F-9AAA-5613BFF76DD5'
last_updated_str = fetch_crop_production_data(api_key)  # Busca dados de produção de culturas
last_updated = datetime.strptime(last_updated_str[:10], "%Y-%m-%d").date()
log_date = check_data_update()  # Checa a data de atualização

if last_updated == log_date:
    print(f"Execução bem-sucedida. Dados já atualizados. Última atualização foi em {log_date}.")
else:
    print(f"Dados atualizados. Última atualização foi em {log_date}, e a nova atualização é em {last_updated}.")
    update_log(last_updated_str)
    # Executa outros scripts se os dados forem atualizados
    scripts = [
        "scripts/fetch_data.py",
        "scripts/generate_ipvs.py",
    ]
    for script in scripts:
        subprocess.run(["python", script], check=True)  # Executa os scripts de processamento adicionais
execution_time = time.time() - start_time
print(f"Total execution time: {execution_time} seconds")