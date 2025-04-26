
# Retenta ler um CSV se falhar download
def ler_csv(*args, **kwargs):
  max_tentativas = 5
  intervalo = 2
  tentativas = 0
  while tentativas < max_tentativas:
      try:
          df = pd.read_csv(*args, **kwargs)
          return df
      except Exception as e:
          tentativas += 1
          print(f"Tentativa {tentativas} falhou: {e}")
          time.sleep(intervalo)
  print(f"Falha após {max_tentativas} tentativas.")
  return None

# Coleta dados da API do Banco Central (SGS)
def coleta_bcb_sgs(codigo, nome, data_inicio = "01/01/2000", data_fim = (pd.to_datetime("today") + pd.offsets.DateOffset(months = 36)).strftime("%d/%m/%Y")):

  url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=csv&dataInicial={data_inicio}&dataFinal={data_fim}"

  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = ler_csv(filepath_or_buffer = url, sep = ";", decimal = ",")
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    return (
        resposta
        .rename(columns = {"valor": nome})
        .assign(data = lambda x: pd.to_datetime(x.data, format = "%d/%m/%Y"))
        .set_index("data")
    )

# Coleta dados da API do Banco Central (ODATA)
def coleta_bcb_odata(codigo, nome):

  url = codigo

  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = ler_csv(
        filepath_or_buffer = url,
        sep = ",", decimal = ",",
        converters = {"Data": lambda x: pd.to_datetime(x)}
        )
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    return resposta.rename(columns = {"Mediana": nome})

# Coleta dados da API do IPEA (IPEADATA)
def coleta_ipeadata(codigo, nome):

  url = f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')"
  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = pd.read_json(url)
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    return (
        pd.DataFrame.from_records(resposta["value"])
        .rename(columns = {"VALVALOR": nome, "VALDATA": "data"})
        .filter(["data", nome])
      )

# Coleta dados da API do IBGE (SIDRA)
def coleta_ibge_sidra(codigo, nome):

  url = f"{codigo}?formato=json"
  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = pd.read_json(url)
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    df = (
        resposta
        .rename(columns = {"D3C": "data", "V": nome})
        .filter(["data", nome])
      )
    df = df[-df[nome].isin(["Valor", "...", "-"])]
    df[nome] = pd.to_numeric(df[nome])
    return df

# Coleta dados da API do FRED
def coleta_fred(codigo, nome):

  url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={codigo}"

  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = ler_csv(
        filepath_or_buffer = url,
        converters = {"DATE": lambda x: pd.to_datetime(x)}
        )
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    return resposta.rename(columns = {"DATE": "data", codigo: nome})

# Coleta dados via link da IFI
def coleta_ifi(codigo, nome):

  try:
    print(f"Coletando a série {codigo} ({nome})")
    resposta = pd.read_excel(
        io = codigo,
        sheet_name = "Hiato do Produto",
        names = ["data", "lim_inf", nome, "lim_sup"],
        skiprows = 2
        )
  except:
    raise Exception(f"Falha na coleta da série {codigo} ({nome})")
  else:
    return resposta
