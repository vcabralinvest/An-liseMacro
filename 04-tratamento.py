# Cruza dados do BCB/SGS
df_tratado_bcb_sgs = df_bruto_bcb_sgs.copy()

for f in df_tratado_bcb_sgs.items():
  df_temp = f[1][0]
  for df in f[1][1:]:
    df_temp = df_temp.join(other = df, how = "outer")
  df_tratado_bcb_sgs[f[0]] = df_temp

# Agrega dados de frequência diária para mensal por média ou início de mês
df_tratado_bcb_sgs["Mensal"] = df_tratado_bcb_sgs["Mensal"].join(
    other = (
        df_tratado_bcb_sgs["Diária"]
        .filter(input_bcb_sgs.query("Identificador != 'selic'")["Identificador"].to_list())
        .resample("MS")
        .mean()
        .join(
            other = (
                df_tratado_bcb_sgs["Diária"]
                .filter(["selic"])
                .reset_index()
                .assign(data = lambda x: x.data.dt.to_period("M").dt.to_timestamp())
                .groupby("data")
                .head(1)
                .set_index("data")
            ),
            how = "outer"
        )
        .query("index >= '2000-01-01'")
    ),
    how = "outer"
)


# Filtra expectativas curto prazo ~1 mês à frente e agrega pela média
df_tratado_bcb_odata_ipca_cp = (
    df_bruto_bcb_odata[0]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(30, "D")).astype(int),
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp()
        )
    .query("horizonte == 1")
    .groupby(["data"], as_index = False)["expec_ipca_top5_curto_prazo"]
    .mean()
)

# Filtra expectativas médio prazo ~6 mês à frente e agrega pela média
df_tratado_bcb_odata_ipca_mp = (
    df_bruto_bcb_odata[1]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(30, "D")).astype(int),
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp()
        )
    .query("horizonte == 6")
    .groupby(["data"], as_index = False)["expec_ipca_top5_medio_prazo"]
    .mean()
)

# Filtra expectativas longo prazo ~1 ano à frente e agrega pela média
df_tratado_bcb_odata_selic = (
    df_bruto_bcb_odata[2]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%Y"),
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp(),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(365, "D")).astype(int)
        )
    .query("horizonte == 1")
    .groupby(["data"], as_index = False)["expec_selic"]
    .mean()
)

# Filtra expectativas curto prazo ~1 mês à frente e agrega pela média
df_tratado_bcb_odata_cambio = (
    df_bruto_bcb_odata[3]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(30, "D")).astype(int),
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp()
        )
    .query("horizonte == 1")
    .groupby(["data"], as_index = False)["expec_cambio"]
    .mean()
)

# Filtra expectativas curto prazo ~12 meses à frente e agrega pela média
df_tratado_bcb_odata_ipca_lp = (
    df_bruto_bcb_odata[4]
    .assign(data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp())
    .groupby(["data"], as_index = False)["expec_ipca_12m"]
    .mean()
)

# Filtra expectativas médio prazo ~9 meses à frente e agrega pela média
df_tratado_bcb_odata_pib = (
    df_bruto_bcb_odata[5]
    .assign(
        DataReferencia = lambda x: pd.PeriodIndex(
            x.DataReferencia.str.replace(r"(\d{1})/(\d{4})", r"\2-Q\1", regex = True),
            freq = "Q"
            ).to_timestamp(),
        data = lambda x: x.Data.dt.to_period("Q").dt.to_timestamp(),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(30, "D")).astype(int)
      )
    .query("horizonte == 9")
    .groupby(["data"], as_index = False)["expec_pib"]
    .mean()
)

# Filtra expectativas longo prazo ~1 ano à frente e agrega pela média
df_tratado_bcb_odata_primario = (
    df_bruto_bcb_odata[6]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%Y"),
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp(),
        horizonte = lambda x: ((x.DataReferencia - x.Data) / np.timedelta64(365, "D")).astype(int)
        )
    .query("horizonte == 1")
    .groupby(["data"], as_index = False)["expec_primario"]
    .mean()
)

# Cruza dados de mesma frequência
df_tratado_bcb_odata_lista = [
    df_tratado_bcb_odata_ipca_mp,
    df_tratado_bcb_odata_ipca_lp,
    df_tratado_bcb_odata_selic,
    df_tratado_bcb_odata_cambio,
    df_tratado_bcb_odata_primario
  ]

df_tratado_bcb_odata_mensal = df_tratado_bcb_odata_ipca_cp.set_index("data")

for df in df_tratado_bcb_odata_lista:
  df_tratado_bcb_odata_mensal = df_tratado_bcb_odata_mensal.join(
      other = df.set_index("data"),
      how = "outer"
      )


# Cruza dados do IPEADATA
df_tratado_ipeadata = df_bruto_ipeadata.copy()

for f in df_tratado_ipeadata.items():
  df_temp = f[1][0].assign(data = lambda x: pd.to_datetime(x.data, utc = True)).set_index("data")
  for df in f[1][1:]:
    df_temp = df_temp.join(
        other = df.assign(data = lambda x: pd.to_datetime(x.data, utc = True)).set_index("data"),
        how = "outer"
        )
  df_tratado_ipeadata[f[0]] = df_temp

# Agrega dados de frequência diária para mensal por média
df_tratado_ipeadata["Mensal"] = (
    df_tratado_ipeadata["Mensal"]
    .reset_index()
    .assign(data = lambda x: x.data.dt.to_period("M").dt.to_timestamp())
    .set_index("data")
    .join(
        other = (
            df_tratado_ipeadata["Diária"]
            .reset_index()
            .assign(data = lambda x: x.data.dt.to_period("M").dt.to_timestamp())
            .set_index("data")
            .resample("MS")
            .mean()
        ),
        how = "outer"
      )
    .query("index >= '2000-01-01'")
)


# Cruza dados do IBGE/SIDRA
df_tratado_ibge_sidra = df_bruto_ibge_sidra.copy()

for f in df_tratado_ibge_sidra.items():
  df_temp = (
      f[1][0]
      .iloc[1:]
      .assign(
          data = lambda x: pd.PeriodIndex(
            x.data.str.replace(r"(\d{4})(\d{1})(\d{1})", r"\1-\2\3" if f[0] == "Mensal" else r"\1-Q\3", regex = True),
            freq = "M" if f[0] == "Mensal" else "Q"
            ).to_timestamp()
        )
      .set_index("data")
  )
  for df in f[1][1:]:
    df_temp = df_temp.join(
        other = (
            df
            .iloc[1:]
            .assign(
                data = lambda x: pd.PeriodIndex(
                  x.data.str.replace(r"(\d{4})(\d{1})(\d{1})", r"\1-\2\3" if f[0] == "Mensal" else r"\1-Q\3", regex = True),
                  freq = "M" if f[0] == "Mensal" else "Q"
                  ).to_timestamp()
              )
            .set_index("data")
        ),
        how = "outer"
        )
  df_tratado_ibge_sidra[f[0]] = df_temp


# Cruza dados do FRED
df_tratado_fred = df_bruto_fred.copy()

for f in df_tratado_fred.items():
  df_temp = f[1][0].set_index("observation_date")
  for df in f[1][1:]:
    df_temp = df_temp.join(
        other = df.set_index("observation_date"),
        how = "outer"
        )
  df_temp = df_temp.rename_axis(index='data')  
  df_tratado_fred[f[0]] = df_temp

# Agrega dados de frequência diária para mensal por média
df_tratado_fred["Mensal"] = (
    df_tratado_fred["Mensal"]
    .set_index(pd.to_datetime(df_tratado_fred["Mensal"].index))
    .join(
        other = (
            df_tratado_fred["Diária"]
            .set_index(pd.to_datetime(df_tratado_fred["Diária"].index))
            .resample("MS")
            .mean()
        ),
        how = "outer"
      )
    .query("index >= '2000-01-01'")
)


# Representa em porcentagem dados do IFI
df_tratado_ifi = (
    df_bruto_ifi
    .assign(hiato_produto = lambda x: x.hiato_produto.mul(100))
    .query("data >= '2000-01-01'")
    .drop(labels = ["lim_inf", "lim_sup"], axis = "columns")
    .set_index("data")
)
