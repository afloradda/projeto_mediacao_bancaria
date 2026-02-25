# Dicionário de Dados

## estados_agregado.csv

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| uf | string | Sigla do estado |
| regiao | string | Região do Brasil |
| total_reclamacoes | int | Total de reclamações |
| nota_media | float | Nota média (1-5) |
| tempo_medio | float | Tempo médio (dias) |
| populacao | int | População IBGE 2022 |
| reclamacoes_100k | float | Taxa por 100k hab |

## municipios_sp_agregado.csv

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| municipio | string | Nome do município |
| total_reclamacoes | int | Total de reclamações |
| nota_media | float | Nota média (1-5) |
| tempo_medio | float | Tempo médio (dias) |
| populacao | int | População IBGE 2022 |
| reclamacoes_100k | float | Taxa por 100k hab |

## instituicoes_financeiras_sp.csv

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| instituicao | string | Nome da instituição |
| total_reclamacoes | int | Total de reclamações |
| nota_media | float | Nota média (1-5) - 128 nulos |
| tempo_medio | float | Tempo médio (dias) - 73 nulos |
| pct_resolvido | float | Percentual resolvido |
| segmento | string | Tipo de instituição |

## Observações

- Nota: 1 (péssimo) a 5 (excelente)
- Tempo: em dias corridos
- Taxa/100k: (total/populacao) * 100000
- Valores nulos: usar np.nanmean(), np.nanmedian()
