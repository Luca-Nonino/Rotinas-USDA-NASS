import aiohttp
import aiofiles
import csv
from collections import defaultdict
import os
from datetime import datetime
import asyncio

async def fetch_and_transform_crop_data(api_key, commodities, categories_with_items, years):
    base_url = 'https://quickstats.nass.usda.gov/api/api_GET/'  # URL base para a API
    filename = os.path.join("data", "raw", "USDA-QuickStats.csv")  # Define o caminho do arquivo

    forecast_mapping = {
        'CORN': {
            'YEAR - AUG FORECAST': 'AUG',  # Mapeia previsão para agosto
            'YEAR - NOV FORECAST': 'NOV',  # Mapeia previsão para novembro
            'YEAR - OCT FORECAST': 'OCT',  # Mapeia previsão para outubro
            'YEAR - SEP FORECAST': 'SEP',  # Mapeia previsão para setembro
        },
        'SOYBEANS': {
            'YEAR - JUN ACREAGE': 'JUN',  # Mapeia medidas de área para junho
            'YEAR - MAR ACREAGE': 'MAR',  # Mapeia medidas de área para março
            'YEAR - AUG ACREAGE': 'AUG',  # Mapeia medidas de área para agosto
            'YEAR - OCT ACREAGE': 'OCT',  # Mapeia medidas de área para outubro
        }
    }

    os.makedirs(os.path.dirname(filename), exist_ok=True)  # Cria diretório se não existir

    aggregated_data = defaultdict(lambda: defaultdict(lambda: defaultdict(str)))  # Agrega dados em um dicionário multi-nível

    async with aiohttp.ClientSession() as session:
        tasks = []
        for commodity in commodities:
            for category, data_items in categories_with_items.items():
                for data_item in data_items:
                    if commodity in data_item:
                        for year in years:
                            tasks.append(fetch_data(session, base_url, api_key, commodity, category, data_item, year, forecast_mapping, aggregated_data))
        await asyncio.gather(*tasks)  # Executa todas as tarefas em paralelo

    # Escreve dados agregados no arquivo CSV sem cabeçalhos
    async with aiofiles.open(filename, mode='w', newline='') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_ALL)  # Garante que todos os campos sejam citados
        for key, data in aggregated_data.items():
            commodity_desc, statisticcat_desc, short_desc, state_alpha, location_desc, year = key
            row = [
                commodity_desc, statisticcat_desc, short_desc, state_alpha, location_desc, year,
                data.get('CV (%)', ''),
                data['MONTH'].get('MAR', ''),
                data['MONTH'].get('JUN', ''),
                data['MONTH'].get('SEP', ''),
                data['MONTH'].get('AUG', ''),
                data['MONTH'].get('OCT', ''),
                data['MONTH'].get('NOV', ''),
                data['Value']
            ]
            await file.write(','.join(f'"{item}"' for item in row) + '\n')  # Cita manualmente todos os campos para máximo controle

async def fetch_data(session, base_url, api_key, commodity, category, data_item, year, forecast_mapping, aggregated_data):
    params = {
        'key': api_key,
        'domain_desc': 'TOTAL',
        'sector_desc': 'CROPS',
        'group_desc': 'FIELD CROPS',
        'commodity_desc': commodity,
        'statisticcat_desc': category,
        'short_desc': data_item,
        'agg_level_desc': 'STATE',
        'year': year,
        'format': 'csv'
    }

    async with session.get(base_url, params=params) as response:
        if response.status == 200:
            text = await response.text()
            reader = csv.DictReader(text.splitlines())

            for row in reader:
                key = (row['commodity_desc'], row['statisticcat_desc'], row['short_desc'],
                       row['state_alpha'], row['location_desc'], row['year'])
                aggregated_data[key]['CV (%)'] = row['CV (%)']  # Armazena o coeficiente de variação
                aggregated_data[key]['Value'] = row['Value']  # Armazena o valor

                if commodity in forecast_mapping and row['reference_period_desc'] in forecast_mapping[commodity]:
                    month = forecast_mapping[commodity][row['reference_period_desc']]
                    aggregated_data[key]['MONTH'][month] = row['Value']  # Armazena o valor mensal com base na previsão
            # print(f"Dados de {commodity} para {year} obtidos com sucesso")
        else:
            # print(f"Failed to fetch data for {commodity} in {year}")  # Mensagem de erro se a solicitação falhar
            pass

# Parte de execução
api_key = '1E0BC2ED-A9F9-367F-9AAA-5613BFF76DD5'  # Chave da API
commodities = ['CORN', 'SOYBEANS']  # Lista de mercadorias
categories_with_items = {
    'PRODUCTION': ['CORN, GRAIN - PRODUCTION, MEASURED IN BU', 'SOYBEANS - PRODUCTION, MEASURED IN BU'],
    'AREA HARVESTED': ['CORN, GRAIN - ACRES HARVESTED', 'SOYBEANS - ACRES HARVESTED'],
    'AREA PLANTED': ['CORN - ACRES PLANTED', 'SOYBEANS - ACRES PLANTED'],
    'YIELD': ['SOYBEANS - YIELD, MEASURED IN BU / ACRE', 'CORN, GRAIN - YIELD, MEASURED IN BU / ACRE']
}
years = [str(year) for year in range(datetime.now().year - 10, datetime.now().year + 1)]  # Lista de anos

asyncio.run(fetch_and_transform_crop_data(api_key, commodities, categories_with_items, years))