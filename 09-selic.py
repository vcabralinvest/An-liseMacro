# Bibliotecas
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from sklearn.ensemble import VotingRegressor
from sklearn.linear_model import Ridge, BayesianRidge
from sklearn.svm import LinearSVR
from sklearn.preprocessing import PowerTransformer
import statsmodels.api as sm
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
dados_brutos_m = pd.read_parquet("dados/df_mensal.parquet")
dados_brutos_a = pd.read_parquet("dados/df_anual.parquet")

# Converte frequência
dados_tratados = (
    dados_brutos_m
    .asfreq("MS")
    .join(
        other = dados_brutos_a.asfreq("MS").ffill(),
        how = "outer"
        )
    .rename_axis("data", axis = "index")
)

# Separa Y
y = dados_tratados.selic.dropna()

# Separa X
x = dados_tratados.drop(labels = "selic", axis = "columns").copy()

# Cria variáveis para modelos teóricos
x_teorico = (
    x
    .copy()
    .join(other = y, how = "outer")
    .assign(
        selic_lag1 = lambda x: x.selic.shift(1),
        selic_lag2 = lambda x: x.selic.shift(2),
        pib_potencial = lambda x: sm.tsa.filters.hpfilter(x.pib_acum12m.ffill(), 14400)[1],
        pib_hiato = lambda x: (x.pib_acum12m / x.pib_potencial - 1) * 100,
        pib_hiato_lag1 = lambda x: x.pib_hiato.shift(1),
        inflacao_hiato = lambda x: x.expec_ipca_12m - x.meta_inflacao.shift(-12)
    )
    .filter([
        "selic_lag1",
        "selic_lag2",
        "pib_hiato",
        "pib_hiato_lag1",
        "inflacao_hiato"
        ])
)

# Computa transformações
for col in x.drop(labels = ["saldo_caged_antigo", "saldo_caged_novo"], axis = "columns").columns.to_list():
  x[col] = transformar(x[col], metadados.loc[col, "Transformação"])

# Filtra amostra
y = y[y.index >= inicio_treino]
x_alem_de_y = x.query("index >= @y.index.max()")
x = x.query("index >= @inicio_treino and index <= @y.index.max()")
x_teorico = x_teorico.query("index >= @inicio_treino and index <= @y.index.max()")

# Conta por coluna proporção de NAs em relação ao nº de obs. de Y
prop_na = x.isnull().sum() / y.shape[0]

# Remove variáveis que possuem mais de 20% de NAs
x = x.drop(labels = prop_na[prop_na >= 0.2].index.to_list(), axis = "columns")

# Preenche NAs restantes com a vizinhança
x = x.bfill().ffill()
x_teorico = x_teorico.bfill().ffill()


# Reestima melhor modelo com amostra completa
forecaster = ForecasterAutoreg(
    regressor = VotingRegressor([
        ("bayes", BayesianRidge()),
        ("svr", LinearSVR(random_state = semente, dual = True, max_iter = 100000)),
        ("ridge", Ridge(random_state = semente))
        ]),
    lags = 2,
    transformer_y = PowerTransformer(),
    transformer_exog = PowerTransformer()
    )
forecaster.fit(y, x_teorico)

# Período de previsão fora da amostra
periodo_previsao = pd.date_range(
    start = forecaster.last_window.index[1] + pd.offsets.MonthBegin(1),
    end = forecaster.last_window.index[1] + pd.offsets.MonthBegin(h),
    freq = "MS"
    )

# Constrói cenários constantes (selic_lag1, selic_lag2, pib_hiato, pib_hiato_lag1)
dados_cenario_constante = (
    x_teorico
    .drop("inflacao_hiato", axis = "columns")
    .join(
        other = periodo_previsao.rename("data").to_frame(),
        how = "outer"
        )
    .ffill()
    .query("index >= @periodo_previsao.min()")
    .drop("data", axis = "columns")
)

# Coleta dados de expectativas de inflação (expec_ipca_12m)
dados_focus_expec_ipca_12m = (
    pd.read_csv(
        filepath_or_buffer = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoInflacao12Meses?$filter=Indicador%20eq%20'IPCA'%20and%20Suavizada%20eq%20'S'%20and%20baseCalculo%20eq%200%20and%20Data%20ge%20'{(periodo_previsao.min() - pd.offsets.MonthBegin(3)).strftime('%Y-%m-%d')}'&$format=text/csv",
        decimal = ",",
        converters = {"Data": pd.to_datetime}
        )
    )

# Constrói cenários para Hiato da inflação (inflacao_hiato)
dados_cenario_inflacao_hiato = (
    dados_focus_expec_ipca_12m
    .assign(
        data = lambda x: x.Data.dt.to_period("M").dt.to_timestamp(),
        expec_ipca_12m = lambda x: x.Mediana
        )
    .groupby("data", as_index = False)
    .expec_ipca_12m
    .mean()
    .set_index("data")
    .join(
        other = (
            pd.concat([
                periodo_previsao.to_series(),
                (periodo_previsao + pd.offsets.MonthBegin(h)).to_series()
                ])
            .index
            .rename("data")
            .to_frame()
        ),
        how = "outer"
        )
    .ffill()
    .query("index >= @periodo_previsao.min()")
    .drop("data", axis = "columns")
    .join(other = dados_tratados.filter(["meta_inflacao"]).ffill(), how = "left")
    .ffill()
    .assign(inflacao_hiato = lambda x: x.expec_ipca_12m - x.meta_inflacao.shift(-h))
    .query("index <= @periodo_previsao.max()")
    .filter(["inflacao_hiato"])
)

# Junta cenários
dados_cenarios = dados_cenario_constante.join(
    other = dados_cenario_inflacao_hiato,
    how = "outer"
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
    [y.rename("Selic"),
     previsao.pred.rename("Previsão"),
     previsao.lower_bound.rename("Intervalo Inferior"),
     previsao.upper_bound.rename("Intervalo Superior"),
    ],
    axis = "columns"
    ).to_parquet("previsao/selic.parquet")
