
╔══════════════════════════════════════════════════════════════╗
║           CENSO 2022 - ARQUIVOS ORGANIZADOS                  ║
╚══════════════════════════════════════════════════════════════╝

📁 ESTRUTURA DE PASTAS:
═══════════════════════════════════════════════════════════════

📂 brasil/
   Dados nacionais e por estado

   • censo2022_brasil_limpo.csv
     → População total do Brasil (1 registro)

   • censo2022_estados_brasil.csv
     → 27 estados com dados básicos

   • censo2022_brasil_e_estados.csv
     → Brasil + 27 estados (28 registros)

   • censo2022_estados_completo.csv ⭐ RECOMENDADO
     → Estados com TODOS os indicadores:
       - População
       - Área territorial
       - Densidade demográfica
       - Percentual do Brasil

   • censo2022_resumo_regioes_brasil.csv
     → Agregação por 5 regiões geográficas

───────────────────────────────────────────────────────────────

📂 sao_paulo/
   Dados do estado de São Paulo

   • censo2022_sao_paulo_limpo.csv
     → Dados gerais do estado

   • municipios_sp_limpo.csv
     → 645 municípios (dados básicos)

   • censo2022_sp_consolidado.csv ⭐ RECOMENDADO
     → Municípios com contexto completo

   • censo2022_ranking_municipios.csv
     → Ranking de municípios por área

   • censo2022_resumo_regioes_administrativas.csv
     → Agregação por Região Administrativa (16 RAs)

   • censo2022_resumo_drs.csv
     → Agregação por DRS - Saúde (17 DRS)

───────────────────────────────────────────────────────────────

📂 documentacao/
   Relatórios e documentação

   • INDICE_ARQUIVOS.txt
     → Lista completa de arquivos

   • RELATORIO_LIMPEZA.txt
     → Relatório do processo de limpeza

═══════════════════════════════════════════════════════════════

🎯 COMO USAR:

1️⃣ Para análises do Brasil:
   import pandas as pd
   df = pd.read_csv('data/limpo/brasil/censo2022_estados_completo.csv')

2️⃣ Para análises de São Paulo:
   df = pd.read_csv('data/limpo/sao_paulo/censo2022_sp_consolidado.csv')

3️⃣ Para análises por região:
   df = pd.read_csv('data/limpo/brasil/censo2022_resumo_regioes_brasil.csv')

═══════════════════════════════════════════════════════════════

📊 ESTATÍSTICAS:

🇧🇷 BRASIL:
   • População: 203.080.756 habitantes
   • Estados: 27 UFs
   • Regiões: 5

🏙️ SÃO PAULO:
   • População: 44.411.238 habitantes (21,87% do Brasil)
   • Municípios: 645
   • Área: 248.219 km²
   • Densidade: 178,92 hab/km²

═══════════════════════════════════════════════════════════════

✅ PADRÃO DE QUALIDADE:
   • Encoding: UTF-8 com BOM
   • Separador: vírgula (,)
   • Sem valores nulos críticos
   • Tipos de dados validados
   • Documentação completa

═══════════════════════════════════════════════════════════════
Gerado em: 2026-02-23 16:11:45
═══════════════════════════════════════════════════════════════
