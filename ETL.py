import pandas as pd
import duckdb

# Carregar os dados
train_df = pd.read_csv('/home/davi/pyproject/portifolio/projeto_series_temporais-1/data/train.csv')
features_df = pd.read_csv('/home/davi/pyproject/portifolio/projeto_series_temporais-1/data/features.csv')
stores_df = pd.read_csv('/home/davi/pyproject/portifolio/projeto_series_temporais-1/data/stores.csv')

# Exemplo de como usar SQL para juntar os dados
query = """
SELECT
    tr.Store,
    st.Type,
    st.Size,
    tr.Dept,
    tr.Date,
    tr.Weekly_Sales,
    tr.IsHoliday,
    ft.Temperature,
    ft.Fuel_Price,
    ft.CPI,
    ft.Unemployment
FROM train_df tr
LEFT JOIN stores_df st ON tr.Store = st.Store
LEFT JOIN features_df ft ON tr.Store = ft.Store AND tr.Date = ft.Date
"""
df_full = duckdb.query(query).to_df()

# Converter a coluna de data
df_full['Date'] = pd.to_datetime(df_full['Date'])

# Lidar com valores nulos (ex: preencher com a mediana)
# Em PySpark: df.fillna({'CPI': media_cpi})
df_full['CPI'] = df_full['CPI'].fillna(df_full['CPI'].median())
df_full['Unemployment'] = df_full['Unemployment'].fillna(df_full['Unemployment'].median())

# Agrupar dados para ter uma única série temporal (vendas totais do Walmart)
df_total_sales = df_full.groupby('Date')['Weekly_Sales'].sum().reset_index()
df_total_sales = df_total_sales.set_index('Date')
df_total_sales.sort_index(inplace=True)

print("ETL Concluído. Dados prontos para análise.")
print(df_total_sales.head())