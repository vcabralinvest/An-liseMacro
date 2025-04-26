# Planilha de metadados
df_metadados = pd.read_excel(
    io = "https://docs.google.com/spreadsheets/d/1x8Ugm7jVO7XeNoxiaFPTPm1mfVc3JUNvvVqVjCioYmE/export?format=xlsx",
    sheet_name = "Metadados"
    )


# Coleta dados do BCB/SGS
input_bcb_sgs = (
    df_metadados
    .query("Fonte == 'BCB/SGS' and `Forma de Coleta` == 'API'")
    .reset_index(drop = True)
)

df_bruto_bcb_sgs = {"Diária": [], "Mensal": [], "Trimestral": [], "Anual": []}

for serie in input_bcb_sgs.index:
  ser = input_bcb_sgs.iloc[serie]
  df_temp = coleta_bcb_sgs(
      codigo = ser["Input de Coleta"],
      nome = ser["Identificador"]
      )
  df_bruto_bcb_sgs[ser["Frequência"]].append(df_temp)


# Coleta dados do BCB/ODATA
input_bcb_odata = (
    df_metadados
    .query("Fonte == 'BCB/ODATA' and `Forma de Coleta` == 'API'")
    .reset_index(drop = True)
)

df_bruto_bcb_odata = []

for serie in input_bcb_odata.index:
  ser = input_bcb_odata.iloc[serie]
  df_temp = coleta_bcb_odata(
      codigo = ser["Input de Coleta"],
      nome = ser["Identificador"]
      )
  df_bruto_bcb_odata.append(df_temp)


# Coleta dados do IPEADATA
input_ipeadata = (
    df_metadados
    .query("Fonte == 'IPEADATA' and `Forma de Coleta` == 'API'")
    .reset_index(drop = True)
)

df_bruto_ipeadata = {"Diária": [], "Mensal": []}

for serie in input_ipeadata.index:
  ser = input_ipeadata.iloc[serie]
  df_temp = coleta_ipeadata(
      codigo = ser["Input de Coleta"],
      nome = ser["Identificador"]
      )
  df_bruto_ipeadata[ser["Frequência"]].append(df_temp)


# Coleta dados do IBGE/SIDRA
input_sidra = (
    df_metadados
    .query("Fonte == 'IBGE/SIDRA' and `Forma de Coleta` == 'API'")
    .reset_index(drop = True)
)

df_bruto_ibge_sidra = {"Mensal": [], "Trimestral": []}

for serie in input_sidra.index:
  ser = input_sidra.iloc[serie]
  df_temp = coleta_ibge_sidra(
      codigo = ser["Input de Coleta"],
      nome = ser["Identificador"]
      )
  df_bruto_ibge_sidra[ser["Frequência"]].append(df_temp)


# Coleta dados do FRED
input_fred = (
    df_metadados
    .query("Fonte == 'FRED' and `Forma de Coleta` == 'API'")
    .reset_index(drop = True)
)

df_bruto_fred = {"Diária": [], "Mensal": [], "Trimestral": []}

for serie in input_fred.index:
  ser = input_fred.iloc[serie]
  df_temp = coleta_fred(
      codigo = ser["Input de Coleta"],
      nome = ser["Identificador"]
      )
  df_bruto_fred[ser["Frequência"]].append(df_temp)


# Coleta dados do IFI
input_ifi = (
    df_metadados
    .query("Fonte == 'IFI'")
    .reset_index(drop = True)
)

df_bruto_ifi = coleta_ifi(input_ifi["Input de Coleta"][0], input_ifi["Identificador"][0])
