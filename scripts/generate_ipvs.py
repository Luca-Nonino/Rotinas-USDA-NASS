import pandas as pd
import os
import logging
import json


def load_and_transform_data(input_file):
    try:
        df = pd.read_csv(input_file, delimiter=',', thousands=',', quotechar='"',
                         names=['commodity_desc', 'statisticcat_desc', 'short_desc', 'state_alpha', 'location_desc', 'year', 'CV (%)', 'MAR', 'JUN', 'SEP', 'AUG', 'OCT', 'NOV', 'Value'],
                         usecols=['commodity_desc', 'statisticcat_desc', 'short_desc', 'state_alpha', 'year', 'Value'],
                         header=None, dtype={'Value': str})
        logging.info("Arquivo CSV lido com sucesso")
    except Exception as e:
        logging.error(f"Falha ao ler CSV: {e}")
        with open("data/logs/generation_errors.log", "a") as error_log:
            error_log.write(f"Erro ao carregar CSV para {input_file}: {e}\n")
        return pd.DataFrame()

    df['Value'] = pd.to_numeric(df['Value'].str.replace(',', ''), errors='coerce')  # Converte campo "Value" para numérico, tratando erros.
    df['commodity_desc'] = df['commodity_desc'].map({'CORN': 'COS', 'SOYBEANS': 'SBS'})  # Mapeia descrições de mercadorias para novos valores.

    # Handling both state-level and country-level data
    df_state = df.copy()  # Cria uma cópia do DataFrame para dados estaduais.
    df_state['<cod>'] = 'NASS:' + df_state['commodity_desc'] + '_' + df_state['state_alpha'] + '_US'
    df_country = df.copy()  # Cria uma cópia para dados nacionais.
    df_country['<cod>'] = 'NASS:' + df_country['commodity_desc'] + '_US'

    df_state['<data>'] = pd.to_datetime(df_state['year'], format='%Y').dt.strftime('%Y-01-01')  # Formata a coluna 'year' como data.
    df_country['<data>'] = df_state['<data>']

    # Pivot and aggregate state-level data
    df_state = df_state.pivot_table(index=['<cod>', '<data>'], columns='statisticcat_desc', values='Value', aggfunc='sum').reset_index()  # Cria pivot table para dados estaduais.
    apply_conversions(df_state)

    # Aggregate and pivot country-level data
    df_country = df_country.pivot_table(index=['<cod>', '<data>'], columns='statisticcat_desc', values='Value', aggfunc='sum').reset_index()  # Cria pivot table para dados nacionais.
    apply_conversions(df_country)

    # Combine state and country data
    df = pd.concat([df_state, df_country], ignore_index=True)  # Combina dados estaduais e nacionais.
    df.rename(columns={'PRODUCTION': '<PRO>', 'AREA HARVESTED': '<ARH>', 'AREA PLANTED': '<ARP>', 'YIELD': '<YLD>'}, inplace=True)  # Renomeia algumas colunas.

    return df


def apply_conversions(df):
    """ Aplica conversões de unidade às colunas do DataFrame """
    for col in ['<PRO>', '<ARH>', '<ARP>', '<YLD>']:
        if col in df:
            df[col] = df[col].apply(lambda x: x / 36.744 if col == '<PRO>' else x / 2.471 if col in ['<ARH>', '<ARP>'] else x * 67.249 if col == '<YLD>' else x)

def main():
    logging.basicConfig(level=logging.INFO)
    input_file = "data/raw/USDA-QuickStats.csv"
    error_log_path = "data/logs/generation_errors.log"
    if not os.path.exists(os.path.dirname(error_log_path)):  # Verifica e cria diretório do log de erros se necessário.
        os.makedirs(os.path.dirname(error_log_path))

    with open("data/logs/update_log.json", "r") as file:
        update_info = json.load(file)
        last_updated = update_info["LAST_UPDATED"]
        date_suffix = f"{last_updated['DAY']}_{last_updated['MONTH']}_{last_updated['YEAR']}"
        output_file = f"data/ipvs/NASS_{date_suffix}.ipv"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)  # Verifica e cria diretório do arquivo de saída se necessário.
    df = load_and_transform_data(input_file)

    try:
        df.to_csv(output_file, index=False, columns=['<cod>', '<data>', '<PRO>', '<ARH>', '<ARP>', '<YLD>'], float_format='%.3f')
        logging.info(f"Dados gravados com sucesso em {output_file}")
    except Exception as e:
        logging.error(f"Falha ao gravar em {output_file}: {e}")
        with open(error_log_path, "a") as error_log:
            error_log.write(f"Erro ao exportar CSV para {output_file}: {e}\n")

    logging.info("Geração de IPVS completada.")
    # Clear the contents of data/raw after IPV generation
    for file in os.listdir("data/raw"):
        os.remove(os.path.join("data/raw", file))
    logging.info("Todos os arquivos em data/raw foram limpos.")  # Limpeza dos arquivos na pasta data/raw após a geração.

if __name__ == '__main__':
    main()