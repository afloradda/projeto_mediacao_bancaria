# Documentação - Configuração e Carregamento de Dados

**Data de criação:** 24/02/2026 10:09:52

---

## 1. CONFIGURAÇÃO DO AMBIENTE

- **Diretório de trabalho:** `c:\Users\caroline.coutinho\projeto_mediacao_bancaria\analises\gold`
- **Raiz do projeto:** `c:\Users\caroline.coutinho\projeto_mediacao_bancaria\analises\gold`
- **Pasta de saída:** `c:\Users\caroline.coutinho\projeto_mediacao_bancaria\analises\gold\output`
- **Pasta dados Censo:** `C:\Users\caroline.coutinho\projeto_mediacao_bancaria\data\gold\limpo\brasil`

---

## 2. BIBLIOTECAS IMPORTADAS

| Biblioteca | Versão |
|------------|--------|
| Pandas | 2.3.3 |
| NumPy | 2.3.5 |
| Matplotlib | 3.10.7 |
| Seaborn | 0.13.2 |
| Plotly | 6.5.1 |
| Altair | 6.0.0 |
| Python | 3.13.5 |

---

## 3. MÓDULOS CUSTOMIZADOS CARREGADOS

### 3.1 `lib.carregamento`
Funções para carregar bases de dados:
- `carregar_base_silver()`
- `carregar_base_gold_sp()`
- `carregar_base_agibank()`
- `carregar_base_setorial()`
- `carregar_base_filtrada()`
- `listar_arquivos_disponiveis()`
- `info_base()`

### 3.2 `lib.cores`
Configurações de temas e paletas Agibank:
- `aplicar_tema_agibank()`
- `configurar_plotly()`
- Paletas de cores (CORES_AGIBANK, PALETA_CATEGORICA, etc.)

### 3.3 `lib.visualizacoes`
Funções para gráficos customizados:
- `grafico_barras()`
- `grafico_linha()`
- `grafico_pizza()`
- `grafico_boxplot()`
- `grafico_heatmap()`
- `grafico_distribuicao()`
- `grafico_comparativo_barras()`

---

## 4. CONFIGURAÇÕES APLICADAS

### 4.1 Configurações Gerais
- Warnings desabilitados
- Seed NumPy: 42 (reprodutibilidade)

### 4.2 Configurações Pandas
- Exibição de todas as colunas
- Máximo de 100 linhas
- Formato de float: 2 casas decimais
- Largura automática
- Máximo de 50 caracteres por coluna

### 4.3 Configurações NumPy
- Precisão: 2 casas decimais
- Supressão de notação científica
- Largura de linha: 120 caracteres

### 4.4 Configurações Visuais
- **Matplotlib/Seaborn:** Tema Agibank (tamanho: grande), DPI 300
- **Plotly:** Configurações customizadas Agibank
- **Altair:** Renderização padrão habilitada

---

## 5. CONSTANTES DEFINIDAS

- **Ano de análise:** 2025
- **Janela de anos:** 2024 - 2025
- **Datas de referência:** 12 períodos mensais

---

## 6. BASES DE DADOS CARREGADAS

### 6.1 BASE AGIBANK
- **Registros:** 4,006
- **Colunas:** 32
- **Fonte:** `sp_agibank_only_v1.csv`
- **Descrição:** Reclamações específicas do Agibank em SP

**Colunas principais:**

- `regiao`
- `uf`
- `cidade`
- `sexo`
- `faixa_etaria`
- `ano_abertura`
- `mes_abertura`
- `data_abertura`
- `data_resposta`
- `data_finalizacao`
- `prazo_resposta`
- `tempo_resposta`
- `nome_fantasia`
- `segmento_de_mercado`
- `area`
- `assunto`
- `grupo_problema`
- `problema`
- `como_comprou_contratou`
- `procurou_empresa`
- `respondida`
- `situacao`
- `avaliacao_reclamacao`
- `nota_do_consumidor`
- `data_source`
- `file_origin`
- `processed_at`
- `file_month`
- `is_agibank`
- `quality_score`
- `cidade_suspeita_gold`
- `cidade_ranking`

---

### 6.2 BASE SETORIAL (SETOR FINANCEIRO)
- **Registros:** 45
- **Colunas:** 3
- **Fonte:** `sp_setorial_v1.csv`
- **Descrição:** Reclamações do setor financeiro em SP

**Colunas principais:**

- `segmento_de_mercado`
- `total_reclamacoes`
- `reclamacoes_agibank`

---

### 6.3 BASE SP COMPLETO
- **Registros:** 649,557
- **Colunas:** 32
- **Fonte:** `sp_completo_v1.csv`
- **Descrição:** Todas as reclamações de SP (todos os setores)

**Colunas principais:**

- `regiao`
- `uf`
- `cidade`
- `sexo`
- `faixa_etaria`
- `ano_abertura`
- `mes_abertura`
- `data_abertura`
- `data_resposta`
- `data_finalizacao`
- `prazo_resposta`
- `tempo_resposta`
- `nome_fantasia`
- `segmento_de_mercado`
- `area`
- `assunto`
- `grupo_problema`
- `problema`
- `como_comprou_contratou`
- `procurou_empresa`
- `respondida`
- `situacao`
- `avaliacao_reclamacao`
- `nota_do_consumidor`
- `data_source`
- `file_origin`
- `processed_at`
- `file_month`
- `is_agibank`
- `quality_score`
- `cidade_suspeita_gold`
- `cidade_ranking`

---

### 6.4 BASE CENSO 2022
- **Registros:** 27
- **Colunas:** 8
- **Fonte:** `censo2022_estados_completo.csv`
- **Descrição:** Dados demográficos dos estados brasileiros

**Colunas principais:**

- `cod_ibge`
- `uf`
- `sigla`
- `regiao`
- `populacao_2022`
- `area_km2`
- `densidade_hab_km2`
- `percentual_pop_brasil`

---

## 7. RESUMO DAS BASES

| Base | Registros | Colunas |
|------|-----------|---------|
| Agibank | 4,006 | 32 |
| Setorial (Financeiro) | 45 | 3 |
| SP Completo | 649,557 | 32 |
| Censo 2022 | 27 | 8 |


---

## 8. PRÓXIMOS PASSOS

1. Análise exploratória das bases
2. Cruzamento de dados demográficos com reclamações
3. Benchmarking Agibank vs Setor
4. Análises geográficas e temporais
5. Identificação de padrões e insights

---

**Notebook:** `analises_pre.ipynb`  
**Autor:** Análise de Mediação Bancária  
**Projeto:** Mediação Bancária - Agibank
