# Bibliotecas
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from sklearn.linear_model import Ridge
from sklearn.preprocessing import PowerTransformer
import pandas as pd
import numpy as np
import os


# Definições e configurações globais
h = 12 # horizonte de previsão
inicio_treino = pd.to_datetime("2004-01-01") # amostra inicial de treinamento
semente = 1984 # semente para reprodução


# Função para transformar dados, conforme definido nos metadados
def transformar(x, tipo):

  switch = {
      "1": lambda x: x,
      "2": lambda x: x.diff(),
      "3": lambda x: x.diff().diff(),
      "4": lambda x: np.log(x),
      "5": lambda x: np.log(x).diff(),
      "6": lambda x: np.log(x).diff().diff()
  }

  if tipo not in switch:
      raise ValueError("Tipo inválido")

  return switch[tipo](x)


# Planilha de metadados
metadados = (
    pd.read_excel(
        io = "https://docs.google.com/spreadsheets/d/1x8Ugm7jVO7XeNoxiaFPTPm1mfVc3JUNvvVqVjCioYmE/export?format=xlsx",
        sheet_name = "Metadados",
        dtype = str,
        index_col = "Identificador"
        )
    .filter(["Transformação"])
)


# Importa dados online
dados_brutos = pd.read_parquet("dados/df_mensal.parquet")


# Converte frequência
dados_tratados = dados_brutos.asfreq("MS")

# Separa Y
y = dados_tratados.ipca.dropna()

# Separa X
x = dados_tratados.drop(labels = "ipca", axis = "columns").copy()

# Concatena saldo do CAGED antigo com novo
x = (
    x
    .assign(saldo_caged = transformar(x.saldo_caged_antigo.combine_first(x.saldo_caged_novo), "5"))
    .drop(labels = ["saldo_caged_antigo", "saldo_caged_novo"], axis = "columns")
)

# Computa transformações
for col in x.drop(labels = "saldo_caged", axis = "columns").columns.to_list():
  x[col] = transformar(x[col], metadados.loc[col, "Transformação"])

# Filtra amostra
y = y[y.index >= inicio_treino]
x_alem_de_y = x.query("index >= @y.index.max()")
x = x.query("index >= @inicio_treino and index <= @y.index.max()")

# Conta por coluna proporção de NAs em relação ao nº de obs. do IPCA
prop_na = x.isnull().sum() / y.shape[0]

# Remove variáveis que possuem mais de 20% de NAs
x = x.drop(labels = prop_na[prop_na >= 0.2].index.to_list(), axis = "columns")

# Preenche NAs restantes com a vizinhança
x = x.bfill().ffill()

# Adiciona dummies sazonais
dummies_sazonais = (
    pd.get_dummies(y.index.month_name())
    .astype(int)
    .drop(labels = "December", axis = "columns")
    .set_index(y.index)
)
x = x.join(other = dummies_sazonais, how = "outer")

# Seleção final de variáveis
x_reg = [
    "expec_ipca_top5_curto_prazo",
    "ic_br",
    "cambio_brl_eur",
    "ipc_s"
    ] + dummies_sazonais.columns.to_list()
# + 1 lag


# Reestima melhor modelo com amostra completa
forecaster = ForecasterAutoreg(
    regressor = Ridge(random_state = semente),
    lags = 1,
    transformer_y = PowerTransformer(),
    transformer_exog = PowerTransformer()
    )
forecaster.fit(y, x[x_reg])


# Período de previsão fora da amostra
periodo_previsao = pd.date_range(
    start = forecaster.last_window.index[0] + pd.offsets.MonthBegin(1),
    end = forecaster.last_window.index[0] + pd.offsets.MonthBegin(h),
    freq = "MS"
    )

# Coleta dados de expectativas de inflação (expec_ipca_top5_curto_prazo)
dados_focus_exp_ipca = (
    pd.read_csv(
        filepath_or_buffer = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Mensais?$filter=Indicador%20eq%20'IPCA'%20and%20tipoCalculo%20eq%20'C'%20and%20Data%20ge%20'{periodo_previsao.min().strftime('%Y-%m-%d')}'&$format=text/csv",
        decimal = ",",
        converters = {
            "Data": pd.to_datetime,
            "DataReferencia": lambda x: pd.to_datetime(x, format = "%m/%Y")
            }
        ))

# Data do relatório Focus usada para construir cenário para expectativas de inflação
data_focus_exp_ipca = (
    dados_focus_exp_ipca
    .query("DataReferencia in @periodo_previsao")
    .Data
    .value_counts()
    .to_frame()
    .reset_index()
    .query("count == @h").query("Data == Data.max()")
    .Data
    .to_list()[0]
)

# Constrói cenário para expectativas de inflação (expec_ipca_top5_curto_prazo)
dados_cenario_exp_ipca = (
    dados_focus_exp_ipca
    .query("DataReferencia in @periodo_previsao")
    .query("Data == @data_focus_exp_ipca")
    .set_index("DataReferencia")
    .filter(["Mediana"])
    .rename(columns = {"Mediana": "expec_ipca_top5_curto_prazo"})
)

# Constrói cenário para commodities (ic_br)
dados_cenario_ic_br = (
    x
    .filter(["ic_br"])
    .dropna()
    .query("index >= @inicio_treino")
    .assign(mes = lambda x: x.index.month_name())
    .groupby(["mes"], as_index = False)
    .ic_br
    .median()
    .set_index("mes")
    .join(
        other = (
            periodo_previsao
            .rename("data")
            .to_frame()
            .assign(mes = lambda x: x.data.dt.month_name())
            .drop("data", axis = "columns")
            .reset_index()
            .set_index("mes")
        ),
        how = "outer"
    )
    .set_index("data")
)

# Coleta dados de expectativas do câmbio (cambio_brl_eur)
dados_focus_cambio = (
    pd.read_csv(
        filepath_or_buffer = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Mensais?$filter=Indicador%20eq%20'C%C3%A2mbio'%20and%20tipoCalculo%20eq%20'M'%20and%20Data%20ge%20'{forecaster.last_window.index[0].strftime('%Y-%m-%d')}'&$format=text/csv",
        decimal = ",",
        converters = {
            "Data": pd.to_datetime,
            "DataReferencia": lambda x: pd.to_datetime(x, format = "%m/%Y")
            }
        ))

# Data do relatório Focus usada para construir cenário para câmbio
data_focus_cambio = (
    dados_focus_cambio
    .query("DataReferencia in @periodo_previsao or DataReferencia == @forecaster.last_window.index[0]")
    .Data
    .value_counts()
    .to_frame()
    .reset_index()
    .query("count == @h+1").query("Data == Data.max()")
    .Data
    .to_list()[0]
)

# Constrói cenário para câmbio (cambio_brl_eur)
dados_cenario_cambio = (
    dados_focus_cambio
    .query("DataReferencia in @periodo_previsao or DataReferencia == @forecaster.last_window.index[0]")
    .query("Data == @data_focus_cambio")
    .set_index("DataReferencia")
    .filter(["Mediana"])
    .rename(columns = {"Mediana": "cambio_brl_eur"})
    .assign(
        cambio_brl_eur = lambda x: transformar(x.cambio_brl_eur, metadados.loc["cambio_brl_eur"].iloc[0])
        )
    .dropna()
)

# Constrói cenário para prévia de preços (ipc_s)
dados_cenario_ipc_s = (
    x
    .filter(["ipc_s"])
    .dropna()
    .query("index >= @inicio_treino")
    .assign(mes = lambda x: x.index.month_name())
    .groupby(["mes"], as_index = False)
    .ipc_s
    .median()
    .set_index("mes")
    .join(
        other = (
            periodo_previsao
            .rename("data")
            .to_frame()
            .assign(mes = lambda x: x.data.dt.month_name())
            .drop("data", axis = "columns")
            .reset_index()
            .set_index("mes")
        ),
        how = "outer"
    )
    .set_index("data")
)

# Junta cenários e gera dummies sazonais
dados_cenarios = (
    dados_cenario_exp_ipca
    .join(
        other = [
            dados_cenario_ic_br,
            dados_cenario_cambio,
            dados_cenario_ipc_s,
            (
                pd.get_dummies(dados_cenario_exp_ipca.index.month_name())
                .astype(int)
                .drop(labels = "December", axis = "columns")
                .set_index(dados_cenario_exp_ipca.index)
            )
            ],
        how = "outer"
        )
)

# Produz previsões
previsao = forecaster.predict_interval(
    steps = h,
    exog = dados_cenarios,
    n_boot = 5000,
    random_state = semente
    )

# Salvar previsões
pasta = "previsao"
if not os.path.exists(pasta):
  os.makedirs(pasta)
  
pd.concat(
    [y.rename("IPCA"),
     previsao.pred.rename("Previsão"),
     previsao.lower_bound.rename("Intervalo Inferior"),
     previsao.upper_bound.rename("Intervalo Superior"),
    ],
    axis = "columns"
    ).to_parquet("previsao/ipca.parquet")
