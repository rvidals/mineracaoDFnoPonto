import requests
import pandas as pd
from datetime import datetime
import time
import certifi
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cria a pasta 'dados' se não existir
os.makedirs("dados", exist_ok=True)

def get_operacaoDFTRANS_API():
    url = 'https://www.sistemas.dftrans.df.gov.br/service/gps/operacoes'
    try:
        # Tenta com verificação SSL
        response = requests.get(url, verify=certifi.where(), timeout=10)
        response.raise_for_status()
    except requests.exceptions.SSLError:
        print("⚠️ Tentando novamente sem verificação SSL")
        try:
            response = requests.get(url, verify=False, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print("❌ Falha ao acessar mesmo sem SSL:", e)
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print("❌ Erro na requisição:", e)
        return pd.DataFrame()

    data = response.json()
    lista_df = []

    for operadora in data:
        id = operadora['operadora']['id']
        nome = operadora['operadora']['nome']
        sigla = operadora['operadora']['sigla']
        razaoSocial = operadora['operadora']['razaoSocial']
        numero = [v['numero'] for v in operadora['veiculos']]
        linha = [v['linha'] for v in operadora['veiculos']]
        horario = [v['horario'] for v in operadora['veiculos']]
        lat = [v['localizacao']['latitude'] for v in operadora['veiculos']]
        long = [v['localizacao']['longitude'] for v in operadora['veiculos']]
        try:
            veloc_uni = [v['velocidade']['unidade'] for v in operadora['veiculos']]
            veloc_valor = [v['velocidade']['valor'] for v in operadora['veiculos']]
        except:
            veloc_uni = None
            veloc_valor = None
        codigoImei = [v['codigoImei'] for v in operadora['veiculos']]
        sentido = [v['sentido'] for v in operadora['veiculos']]
        direcao = [v['direcao'] for v in operadora['veiculos']]
        valid = [v['valid'] for v in operadora['veiculos']]

        df = pd.DataFrame({
            'Id_Operadora': id,
            'Nome_Empresa': nome,
            'Sigla': sigla,
            'Razao_Social': razaoSocial,
            'id_Veiculo': numero,
            'Linha': linha,
            'Horario_Operacao': horario,
            'Latitude': lat,
            'Longitude': long,
            'Velocidade_Unidade': veloc_uni,
            'Velocidade_Valor': veloc_valor,
            'CódigoImei': codigoImei,
            'Sentido': sentido,
            'Direcao': direcao,
            'Validade': valid
        })

        lista_df.append(df)

    return pd.concat(lista_df)

def salvar_csv(df, coleta_num):
    df_copy = df.copy()
    df_copy = df_copy.reset_index(drop=True)
    df_copy['Horario_Operacao'] = pd.to_datetime(df_copy['Horario_Operacao'].astype(str).str[:10], unit='s')
    df_copy['Data_Extracao'] = datetime.now().strftime("%Y-%m-%d")
    df_copy['Hora_Extracao'] = datetime.now().strftime("%H:%M:%S")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'dados/OperacaoTRANSDF_{timestamp}_coleta{coleta_num}.csv'
    df_copy.to_csv(file_name, index=False, encoding='utf-8')
    print(f"✅ Arquivo salvo: {os.path.abspath(file_name)}")

if __name__ == '__main__':
    intervalo_em_minutos = 60  #  2 para testar rapidamente
    contador = 1

    print("⏳ Iniciando a coleta de dados a cada 60 segundos...\n")

    while intervalo_em_minutos > 0:
        time.sleep(60)
        df = get_operacaoDFTRANS_API()
        if not df.empty:
            salvar_csv(df, contador)
            print(f"✅ Coleta {contador} realizada com sucesso.\n")
        else:
            print(f"⚠️ Coleta {contador} retornou dados vazios.\n")
        intervalo_em_minutos -= 1
        contador += 1

    print("✅ Fim da execução.")
