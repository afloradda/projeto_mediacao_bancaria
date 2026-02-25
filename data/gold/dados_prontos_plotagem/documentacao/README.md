# Dados Prontos para Plotagem - Projeto Agibank

**Data de Criação:** 25/02/2026 11:18

## Estrutura de Pastas

dados_prontos_plotagem/
├── agregados/
├── normalizados_completos/
├── documentacao/
└── validacao/

## Bases Agregadas (CSV)

### 1. estados_agregado.csv
- Registros: 27
- Total Reclamações: 2,567,095
- Nota Média: 2.67/5.0

### 2. municipios_sp_agregado.csv
- Registros: 636
- Total Reclamações: 646,854

### 3. instituicoes_financeiras_sp.csv
- Registros: 533
- Total Reclamações: 394,166

## Como Usar

import pandas as pd
from pathlib import Path

CAMINHO = Path('dados_prontos_plotagem/agregados')
df_estados = pd.read_csv(CAMINHO / 'estados_agregado.csv')
df_municipios_sp = pd.read_csv(CAMINHO / 'municipios_sp_agregado.csv')
df_instituicoes = pd.read_csv(CAMINHO / 'instituicoes_financeiras_sp.csv')

## Destaques Agibank

- Nome: Banco Agibank (Agiplan)
- Ranking SP: 20º de 533 (Top 3.8%)
- Nota Média: 1.83/5.0
- Tempo Médio: 6.7 dias
- Total Reclamações: 3.961
