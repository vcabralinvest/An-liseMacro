# Cria pasta dados se não existir
pasta = "dados"
if not os.path.exists(pasta):
  os.makedirs(pasta)


# Diária
df_diaria = (
    df_tratado_bcb_sgs["Diária"]
    .join(
        other=df_tratado_ipeadata["Diária"].reset_index().assign(
            data=lambda x: pd.to_datetime(x['data'].dt.strftime("%Y-%m-%d"))
        ).set_index("data"),
        how="outer"
    )
    .join(other=df_tratado_fred["Diária"], how="outer")
    .reset_index()
    .assign(data=lambda x: pd.to_datetime(x['data']))
    .query("data >= @pd.to_datetime('2000-01-01')")
    .set_index('data')
)

df_diaria.to_parquet(f"{pasta}/df_diaria.parquet")

# Mensal
temp_lista = [
    df_tratado_bcb_sgs["Mensal"],
    df_tratado_bcb_odata_mensal,
    df_tratado_ipeadata["Mensal"],
    df_tratado_ibge_sidra["Mensal"],
    df_tratado_fred["Mensal"]
]

df_mensal = (
  temp_lista[0]
  .join(other = temp_lista[1:], how = "outer")
  .query("index >= @pd.to_datetime('2000-01-01')")
  )
df_mensal.to_parquet(f"{pasta}/df_mensal.parquet")

# Trimestral
temp_lista = [
    df_tratado_bcb_sgs["Trimestral"],
    df_tratado_bcb_odata_pib.set_index("data"),
    df_tratado_ibge_sidra["Trimestral"],
    df_tratado_fred["Trimestral"],
    df_tratado_ifi
]

df_trimestral = (
  temp_lista[0]
  .join(other = temp_lista[1:], how = "outer")
  .reset_index()
  .assign(data=lambda x: pd.to_datetime(x['data']))
  .set_index('data')
  .query("index >= @pd.to_datetime('2000-01-01')")
)

df_trimestral.to_parquet(f"{pasta}/df_trimestral.parquet")

# Anual
df_anual = (
  df_tratado_bcb_sgs["Anual"]
  .query("index >= @pd.to_datetime('2000-01-01')")
)

df_anual.to_parquet(f"{pasta}/df_anual.parquet")
