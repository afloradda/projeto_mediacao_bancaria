"""
DAG Gold - Recortes específicos para análise de negócio - Foco SP
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import sys

sys.path.append('..')
from config.settings import SP_CITIES_CONFIG, AGE_GROUPS_CONFIG, BUSINESS_SECTORS_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_silver_data():
    """Task 1: Carregar dados da camada Silver"""
    logger.info("   Carregando dados da camada Silver...")

    version = 1  # Versão que acabou de ser testada
    silver_file = f"../data/silver/consumidor_gov_silver_v{version}.csv"

    if not Path(silver_file).exists():
        raise FileNotFoundError(f"❌ Arquivo Silver não encontrado: {silver_file}")
    
    df = pd.read_csv(silver_file, sep=';', encoding='utf-8')
    logger.info(f"✅ Dados carregados: {len(df):,} registros, {len(df.columns)} colunas")

    return df


def clean_sp_cities(sp_df):
    """Task 2.5: Limpeza específica das cidades de SP"""
    logger.info("🧹 Limpando cidades de SP...")
    
    original_cities = sp_df['cidade'].nunique()
    original_records = len(sp_df)
    
    # 1. CORREÇÃO DE ENCODING (específico para SP)
    logger.info("   🔧 Corrigindo caracteres corrompidos...")
    
    sp_city_corrections = {
        'Cafel?ndia': 'Cafelândia',
        'Guai?ara': 'Guaiçara', 
        'Paragua?u Paulista': 'Paraguaçu Paulista',
        'Igaraí': 'Igaraí',  # Pode estar correto
        'Juritis': 'Juritis',  # Verificar se existe
        'Marcondésia': 'Marcondésia',  # Verificar se existe
        'Jurucê': 'Jurucê',  # Verificar se existe
        'Lageado de Araçaíba': 'Lageado',  # Possível erro
        'Monte Verde Paulista': 'Monte Verde',  # Possível erro
        'Aparecida de Monte Alto': 'Monte Alto',  # Possível erro
        # Adicionar mais conforme necessário
    }
    
    # Aplicar correções específicas
    corrections_applied = 0
    for wrong, correct in sp_city_corrections.items():
        mask = sp_df['cidade'] == wrong
        if mask.sum() > 0:
            sp_df.loc[mask, 'cidade'] = correct
            corrections_applied += mask.sum()
            logger.info(f"      ✅ '{wrong}' → '{correct}': {mask.sum()} registros")
    
    logger.info(f"   📊 Correções aplicadas: {corrections_applied} registros")
    
    # 2. IDENTIFICAR CIDADES SUSPEITAS (baixa frequência)
    logger.info("   🔍 Identificando cidades suspeitas...")
    
    city_counts = sp_df['cidade'].value_counts()
    
    # Cidades com muito poucos registros (≤ 3) são suspeitas
    suspicious_cities = city_counts[city_counts <= 3].index.tolist()
    
    # Marcar registros suspeitos
    sp_df['cidade_suspeita_gold'] = sp_df['cidade'].isin(suspicious_cities)
    
    suspicious_records = sp_df['cidade_suspeita_gold'].sum()
    logger.info(f"   🚨 Cidades suspeitas identificadas: {len(suspicious_cities)}")
    logger.info(f"   🚨 Registros suspeitos: {suspicious_records:,}")
    
    if len(suspicious_cities) > 0:
        logger.info("   📋 Algumas cidades suspeitas:")
        for city in suspicious_cities[:10]:
            count = city_counts[city]
            logger.info(f"      • '{city}': {count} registros")
    
    # 3. CRIAR DATASET LIMPO (sem suspeitas)
    logger.info("   ✂️ Criando dataset limpo...")
    
    clean_df = sp_df[sp_df['cidade_suspeita_gold'] == False].copy()
    
    clean_cities = clean_df['cidade'].nunique()
    clean_records = len(clean_df)
    
    # Estatísticas finais
    cities_removed = original_cities - clean_cities
    records_removed = original_records - clean_records
    
    logger.info(f"   📊 RESULTADO DA LIMPEZA:")
    logger.info(f"      • Cidades: {original_cities} → {clean_cities} (-{cities_removed})")
    logger.info(f"      • Registros: {original_records:,} → {clean_records:,} (-{records_removed:,})")
    logger.info(f"      • Diferença do esperado: {clean_cities - 645:+}")
    
    # Verificar se chegamos perto dos 645 municípios
    if clean_cities <= 645:
        logger.info("   ✅ Quantidade de cidades dentro do esperado!")
    elif clean_cities <= 660:  # Margem de 15 cidades
        logger.warning(f"   ⚠️ Ainda {clean_cities - 645} cidades acima do esperado (margem aceitável)")
    else:
        logger.warning(f"   ⚠️ Ainda {clean_cities - 645} cidades acima do esperado - revisar limpeza")
    
    logger.info("✅ Limpeza de cidades SP concluída")
    
    return clean_df, sp_df  # Retorna limpo + original com flags


def verification_sp_cities(df):
    """Task 2: Verificação e validação das cidades de SP"""
    logger.info("🔍 Verificando cidades de São Paulo...")
    
    # Filtrar registros de SP
    sp_mask = df['uf'] == 'SP'
    sp_data = df[sp_mask].copy()
    
    if len(sp_data) == 0:
        logger.warning("⚠️ Nenhum registro de SP encontrado!")
        return df, pd.DataFrame()
    
    logger.info(f"📊 Registros SP encontrados: {len(sp_data):,}")
    
    # Análise inicial das cidades de SP
    sp_cities = sp_data['cidade'].value_counts()
    total_sp_cities = len(sp_cities)
    
    logger.info(f"🏙️ Cidades únicas em SP (antes da limpeza): {total_sp_cities:,}")
    
    # Validação dos 645 municípios
    MAX_SP_CITIES = 645
    if total_sp_cities > MAX_SP_CITIES:
        excess_cities = total_sp_cities - MAX_SP_CITIES
        logger.warning(f"⚠️ ATENÇÃO: {total_sp_cities:,} cidades encontradas (esperado: máx {MAX_SP_CITIES})")
        logger.warning(f"⚠️ Excesso: +{excess_cities} cidades - iniciando limpeza...")
    else:
        logger.info(f"✅ Quantidade de cidades dentro do esperado (≤{MAX_SP_CITIES})")
    
    logger.info(f"📋 Top 10 cidades SP:")
    for city, count in sp_cities.head(10).items():
        logger.info(f"   • {city}: {count:,}")
    
    # ✅ NOVA ETAPA: Limpeza específica de SP
    clean_sp_df, original_sp_df = clean_sp_cities(sp_data)
    
    # Adicionar dados limpos de volta ao DataFrame principal
    df['is_sp_clean'] = False
    df.loc[clean_sp_df.index, 'is_sp_clean'] = True
    
    logger.info(f"✅ Registros SP limpos: {len(clean_sp_df):,}")
    
    return df, clean_sp_df  # Retorna DataFrame limpo de SP


def clipping_regional(df, clean_sp_df):
    """Task 3: Recorte Regional - Foco São Paulo"""
    logger.info("🗺️ Criando recorte regional - São Paulo...")
    
    # Usar dados já limpos
    sp_df = clean_sp_df.copy()
    
    if len(sp_df) == 0:
        logger.error("❌ Nenhum dado limpo de SP para análise!")
        return pd.DataFrame(), pd.DataFrame()
    
    logger.info("📊 Criando métricas regionais...")
    
    # ✅ CORREÇÃO: Usar APENAS colunas adequadas
    city_ranking = sp_df.groupby('cidade').agg({
        'cidade': 'count',                    # Total de reclamações
        'is_agibank': 'sum'                  # Quantas são Agibank
    }).round(2)
    
    city_ranking.columns = ['total_reclamacoes', 'reclamacoes_agibank']
    
    # Adicionar percentual Agibank por cidade
    city_ranking['percentual_agibank'] = (
        city_ranking['reclamacoes_agibank'] / city_ranking['total_reclamacoes'] * 100
    ).round(2)
    
    # Calcular taxa de resposta manualmente
    response_rate = sp_df.groupby('cidade')['respondida'].apply(
        lambda x: (x == 'S').mean() * 100  # Percentual de 'S' 
    ).round(2)
    
    city_ranking['taxa_resposta_pct'] = response_rate
    
    # Ordenar por total de reclamações
    city_ranking = city_ranking.sort_values('total_reclamacoes', ascending=False)
    
    # Adicionar ranking de cidades ao dataset principal
    city_ranking_dict = city_ranking.reset_index().reset_index().set_index('cidade')['index'] + 1
    sp_df['cidade_ranking'] = sp_df['cidade'].map(city_ranking_dict)
    
    logger.info(f"✅ Recorte regional criado: {len(sp_df):,} registros")
    logger.info(f"🏙️ Cidades analisadas: {len(city_ranking):,}")
    
    return sp_df, city_ranking


def clipping_age(sp_df):
    """Task 4: Recorte Etário - Perfil do consumidor"""
    logger.info("👥 Criando recorte etário...")
    
    if 'faixa_etaria' not in sp_df.columns:
        logger.warning("⚠️ Coluna 'faixa_etaria' não encontrada")
        return sp_df, pd.DataFrame(), pd.DataFrame()
    
    # ✅ CORREÇÃO: Usar APENAS colunas adequadas
    age_analysis = sp_df.groupby('faixa_etaria').agg({
        'faixa_etaria': 'count',
        'is_agibank': 'sum'
    }).round(2)
    
    age_analysis.columns = ['total_reclamacoes', 'reclamacoes_agibank']
    
    # Calcular percentuais
    age_analysis['percentual_total'] = (
        age_analysis['total_reclamacoes'] / age_analysis['total_reclamacoes'].sum() * 100
    ).round(2)
    
    age_analysis['percentual_agibank'] = (
        age_analysis['reclamacoes_agibank'] / age_analysis['total_reclamacoes'] * 100
    ).round(2)
    
    # Taxa de resposta por faixa etária
    age_response_rate = sp_df.groupby('faixa_etaria')['respondida'].apply(
        lambda x: (x == 'S').mean() * 100
    ).round(2)
    
    age_analysis['taxa_resposta_pct'] = age_response_rate
    
    # Ordenar por total de reclamações
    age_analysis = age_analysis.sort_values('total_reclamacoes', ascending=False)
    
    # Análise específica Agibank por idade (simplificada)
    agibank_age = sp_df[sp_df['is_agibank'] == True].groupby('faixa_etaria').agg({
        'faixa_etaria': 'count'
    })
    
    agibank_age.columns = ['reclamacoes_agibank']
    
    logger.info(f"✅ Análise etária criada: {len(age_analysis)} faixas etárias")
    logger.info("📊 Top 3 faixas etárias:")
    
    for age_group, data in age_analysis.head(3).iterrows():
        logger.info(f"   • {age_group}: {data['total_reclamacoes']:,} reclamações ({data['percentual_total']}%)")
    
    return sp_df, age_analysis, agibank_age


def clipping_sectoral(sp_df):
    """Task 5: Recorte Setorial - Análise de mercado e problemas"""
    logger.info("🏢 Criando recorte setorial...")
    
    sectoral_results = {}
    
    # 1. Análise por Segmento de Mercado
    if 'segmento_de_mercado' in sp_df.columns:
        logger.info("   📊 Analisando segmentos de mercado...")
        
        segment_analysis = sp_df.groupby('segmento_de_mercado').agg({
            'segmento_de_mercado': 'count',
            'is_agibank': 'sum'
        })
        
        segment_analysis.columns = ['total_reclamacoes', 'reclamacoes_agibank']
        segment_analysis = segment_analysis.sort_values('total_reclamacoes', ascending=False)
        
        sectoral_results['segments'] = segment_analysis
    
    # 2. Análise por Área bancária
    if 'area' in sp_df.columns:
        logger.info("   🏦 Analisando área bancária...")
        
        # Filtrar apenas área bancária
        banking_mask = sp_df['area'].str.contains(
            'banco.*financeira.*administradora.*cartão', 
            case=False, na=False, regex=True
        )
        
        banking_df = sp_df[banking_mask].copy()
        
        if len(banking_df) > 0:
            # Análise comparativa entre bancos
            bank_comparison = banking_df.groupby('nome_fantasia').agg({
                'nome_fantasia': 'count',
                'is_agibank': 'sum'
            })
            
            bank_comparison.columns = ['total_reclamacoes', 'reclamacoes_agibank']
            bank_comparison = bank_comparison.sort_values('total_reclamacoes', ascending=False)
            
            sectoral_results['banking_comparison'] = bank_comparison
            
            logger.info(f"      🏦 Bancos analisados: {len(bank_comparison)}")
    
    # 3. Análise por Problema
    if 'problema' in sp_df.columns:
        logger.info("   ⚠️ Analisando tipos de problemas...")
        
        problem_analysis = sp_df.groupby('problema').agg({
            'problema': 'count',
            'is_agibank': 'sum'
        })
        
        problem_analysis.columns = ['total_ocorrencias', 'ocorrencias_agibank']
        problem_analysis = problem_analysis.sort_values('total_ocorrencias', ascending=False)
        
        sectoral_results['problems_general'] = problem_analysis
        
        logger.info(f"      ⚠️ Tipos de problemas identificados: {len(problem_analysis)}")
    
    logger.info("✅ Análise setorial concluída")
    return sp_df, sectoral_results


def save_gold_outputs(sp_df, city_ranking, age_analysis, agibank_age, sectoral_results):
    """Task 6: Salvar todos os recortes Gold"""
    logger.info("💾 Salvando recortes Gold...")
    
    version = 1
    gold_path = Path("../data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    # 1. Dataset principal SP
    main_path = gold_path / f"sp_consumidor_completo_v{version}.csv"
    sp_df.to_csv(main_path, index=False, encoding='utf-8', sep=';')
    outputs['dataset_principal'] = main_path
    logger.info(f"    Dataset SP: {len(sp_df):,} registros")
    
    # 2. Recorte Regional (ranking cidades)
    regional_path = gold_path / f"sp_ranking_cidades_v{version}.csv"
    city_ranking.to_csv(regional_path, encoding='utf-8')
    outputs['recorte_regional'] = regional_path
    logger.info(f"    Ranking cidades: {len(city_ranking)} cidades")
    
    # 3. Recorte Etário
    age_path = gold_path / f"sp_analise_etaria_v{version}.csv"
    age_analysis.to_csv(age_path, encoding='utf-8')
    outputs['recorte_etario'] = age_path
    logger.info(f"    Análise etária: {len(age_analysis)} faixas")
    
    # 4. Recortes Setoriais
    for sector_name, sector_data in sectoral_results.items():
        if not sector_data.empty:
            sector_path = gold_path / f"sp_setorial_{sector_name}_v{version}.csv"
            sector_data.to_csv(sector_path, encoding='utf-8')
            outputs[f'setorial_{sector_name}'] = sector_path
            logger.info(f"    Setorial {sector_name}: {len(sector_data)} registros")
    
    # 5. Dataset apenas Agibank SP
    agibank_sp = sp_df[sp_df['is_agibank'] == True].copy()
    if len(agibank_sp) > 0:
        agibank_path = gold_path / f"sp_agibank_only_v{version}.csv"
        agibank_sp.to_csv(agibank_path, index=False, encoding='utf-8', sep=';')
        outputs['agibank_sp'] = agibank_path
        logger.info(f"    Agibank SP: {len(agibank_sp):,} registros")
    
    logger.info("✅ Todos os recortes salvos")
    return outputs


def gold_dag():
    """DAG principal da camada Gold - Recortes SP"""
    logger.info("🚀 Iniciando DAG Gold - Foco São Paulo...")
    start_time = datetime.now()
    
    try:
        # Pipeline Gold ATUALIZADO
        df = load_silver_data()
        df, clean_sp_df = verification_sp_cities(df)  # ← Retorna dados limpos
        sp_df, city_ranking = clipping_regional(df, clean_sp_df)  # ← Passa dados limpos
        sp_df, age_analysis, agibank_age = clipping_age(sp_df)
        sp_df, sectoral_results = clipping_sectoral(sp_df)
        outputs = save_gold_outputs(sp_df, city_ranking, age_analysis, agibank_age, sectoral_results)
        
        # Relatório final
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 70)
        logger.info(" RELATÓRIO GOLD DAG - RECORTES SÃO PAULO")
        logger.info(f" Duração: {duration}")
        logger.info(f" Registros SP processados: {len(sp_df):,}")
        logger.info(f" Registros Agibank SP: {sp_df['is_agibank'].sum():,}")
        logger.info(f" Cidades SP analisadas: {len(city_ranking):,}")
        logger.info(f" Arquivos Gold gerados: {len(outputs)}")
        logger.info("✅ Gold DAG concluído - Recortes prontos para análise!")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no Gold DAG: {str(e)}")
        raise

if __name__ == "__main__":
    gold_dag()