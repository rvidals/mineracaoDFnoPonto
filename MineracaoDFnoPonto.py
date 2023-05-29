import requests 
import pandas as pd
from datetime import datetime
import time 

"""
 @name
      COEA/DEPAT/IPEDF
 
 @description
     Devido a necessidade de montar uma base de dados com as principais informações das operadoras de ônibus que atuam no 
     transporte urbano do Distrito Federal, presente no site https://dfnoponto.semob.df.gov.br/veiculos/onlineMap.html,
     foi desenvolvido este scrpit em Python 3.7.6, que realiza a mineração de dados da api das operadoras 
     https://www.sistemas.dftrans.df.gov.br/service/gps/operacoes e salva em um arquivo de texto em formato csv que será
     lido por um SIG. 

 @author
      Rogerio Vidal de Siqueira
 
 @contact
      rogerio.siqueira@ipe.df.gov.br
 
 @version
      1.0.0 - Mineração de dados da API presente no DFnoPonto.
 
"""

def get_operacaoDFTRANS_API():
    r = requests.get('https://www.sistemas.dftrans.df.gov.br/service/gps/operacoes').json()

    lista_df = []
    for i in range(len(r)):
      id = r[i]['operadora']['id']
      nome = r[i]['operadora']['nome']
      sigla = r[i]['operadora']['sigla']
      razaoSocial = r[i]['operadora']['razaoSocial']
      numero = [r[i]['veiculos'][x]['numero'] for x in range(len(r[i]['veiculos']))]
      linha = [r[i]['veiculos'][x]['linha'] for x in range(len(r[i]['veiculos']))    ]
      horario = [r[i]['veiculos'][x]['horario'] for x in range(len(r[i]['veiculos']))    ]
      lat = [r[i]['veiculos'][x]['localizacao']['latitude'] for x in range(len(r[i]['veiculos']))    ]
      long = [r[i]['veiculos'][x]['localizacao']['longitude'] for x in range(len(r[i]['veiculos']))    ]
      try:
        veloc_uni = [ r[i]['veiculos'][x]['velocidade']['unidade'] for x in range(len(r[i]['veiculos']))    ]
        veloc_valor = [ r[i]['veiculos'][x]['velocidade']['valor'] for x in range(len(r[i]['veiculos']))    ]
      except:
          veloc_uni = None
          veloc_valor = None
      codigoImei = [ r[i]['veiculos'][x]['codigoImei'] for x in range(len(r[i]['veiculos']))    ]
      sentido = [ r[i]['veiculos'][x]['sentido'] for x in range(len(r[i]['veiculos']))    ]
      direcao = [ r[i]['veiculos'][x]['direcao'] for x in range(len(r[i]['veiculos']))    ]
      valid = [ r[i]['veiculos'][x]['valid'] for x in range(len(r[i]['veiculos']))    ]

    
      df = pd.DataFrame({'Id_Operadora':id, 'Nome_Empresa': nome, 'Sigla':sigla ,
                        'Razao_Social':razaoSocial , 'id_Veiculo':numero, 
                        'Linha':linha, 'Horario_Operacao':horario, 'Latitude':lat, 
                        'Longitude':long, 'Velocidade_Unidade':veloc_uni, 'Velocidade_Valor':veloc_valor, 
                        'CódigoImei':codigoImei, 'Sentido':sentido, 'Direcao':direcao, 'Validade':valid})
      lista_df.append(df)

    df = pd.concat(lista_df) 
    return df

def salvar_csv(df):
  df_copy = df.copy()
  df_copy = df_copy.reset_index() #Resetar index do DF
  #Transformar horários. No arquivo horiginal está tudo em segundos
  df_copy['Horario_Operacao'] = pd.to_datetime(df_copy['Horario_Operacao'].astype(str).str[:10], unit='s')
  #DATA e HORA da extração
  df_copy['Data_Extracao'] = datetime.now().strftime("%Y-%m-%d")
  df_copy['Hora_Extracao'] = datetime.now().strftime("%H:%M:%S")

  date = datetime.now().strftime("%Y%m%d%H%M%S")
  df_copy.to_csv('OperacaoTRANSDF' + date + '.csv', 
                 index=False, encoding='utf-8')


if __name__ == '__main__':
  #Constantes para interagir no prompt
  UP = '\033[1A'
  CLEAR = '\x1b[2K'

  i = 60 # pode mudar para o intervalo (SEGUNDOS) necessário 
  j = 1 # Contador que aparecerá no prompt
  l = []
  while i>0:
      time.sleep(60)  #Espaço para fazer a requisição na API
      df = get_operacaoDFTRANS_API()
      l.append(df)
      print(f"60/{j}")
      print(UP, end=CLEAR)
      i-=1
      j+=1
  df = pd.concat(l) #Concatenar DataFrames
  
  salvar_csv(df) #Salvar

  print("Requisição Salva!!!")
