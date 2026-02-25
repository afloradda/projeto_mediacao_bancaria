# Guia Rápido de Uso

## Carregar Dados

import pandas as pd
import numpy as np
from pathlib import Path

CAMINHO = Path('dados_prontos_plotagem/agregados')
df_estados = pd.read_csv(CAMINHO / 'estados_agregado.csv')
df_municipios_sp = pd.read_csv(CAMINHO / 'municipios_sp_agregado.csv')
df_instituicoes = pd.read_csv(CAMINHO / 'instituicoes_financeiras_sp.csv')

## Top 10

top_10 = df_estados.nlargest(10, 'total_reclamacoes')
print(top_10[['uf', 'total_reclamacoes', 'nota_media']])

## Filtros

sp = df_estados[df_estados['uf'] == 'SP']
campinas = df_municipios_sp[df_municipios_sp['municipio'] == 'CAMPINAS']
agibank = df_instituicoes[df_instituicoes['instituicao'].str.contains('Agibank', case=False, na=False)]

## Estatísticas NumPy (com NaN)

media = np.nanmean(df_instituicoes['nota_media'].values)
mediana = np.nanmedian(df_estados['tempo_medio'].values)
std = np.nanstd(df_estados['reclamacoes_100k'].values)

## Classificação com np.where

taxa = df_estados['reclamacoes_100k'].values
df_estados['criticidade'] = np.where(
    taxa > 2000, 'CRÍTICO',
    np.where(taxa > 1500, 'ALTO',
    np.where(taxa > 1000, 'MODERADO', 'BAIXO'))
)

## Normalização

def normalizar(arr):
    arr_clean = arr[~np.isnan(arr)]
    return (arr_clean - arr_clean.min()) / (arr_clean.max() - arr_clean.min())

df_estados['taxa_norm'] = normalizar(df_estados['reclamacoes_100k'].values)

## Análise Agibank

agibank = df_instituicoes[df_instituicoes['instituicao'].str.contains('Agibank', case=False, na=False)].iloc[0]
nota_setor = np.nanmean(df_instituicoes['nota_media'].values)
print(f"Agibank: {agibank['nota_media']:.2f} vs Setor: {nota_setor:.2f}")
