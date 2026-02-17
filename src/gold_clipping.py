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
    
    # Análise das cidades de SP
    sp_cities = sp_data['cidade'].value_counts()
    total_sp_cities = len(sp_cities)
    
    logger.info(f"🏙️ Cidades únicas em SP: {total_sp_cities:,}")
    
    
    MAX_SP_CITIES = 645
    if total_sp_cities > MAX_SP_CITIES:
        excess_cities = total_sp_cities - MAX_SP_CITIES
        logger.warning(f"⚠️ ATENÇÃO: {total_sp_cities:,} cidades encontradas (esperado: máx {MAX_SP_CITIES})")
        logger.warning(f"⚠️ Excesso: +{excess_cities} cidades - possíveis inconsistências!")
        
        # Mostrar cidades com poucos registros (provavelmente incorretas)
        low_frequency_cities = sp_cities[sp_cities <= 5]  # Cidades com ≤ 5 registros
        if len(low_frequency_cities) > 0:
            logger.warning(f"⚠️ Cidades com poucos registros (≤5): {len(low_frequency_cities)}")
            logger.warning("   Possíveis erros de digitação:")
            for city, count in low_frequency_cities.head(10).items():
                logger.warning(f"      • '{city}': {count} registros")
    else:
        logger.info(f"✅ Quantidade de cidades dentro do esperado (≤{MAX_SP_CITIES})")
    
    logger.info(f"📋 Top 10 cidades SP:")
    for city, count in sp_cities.head(10).items():
        logger.info(f"   • {city}: {count:,}")
    
    # Identificar possíveis cidades suspeitas em SP
    suspicious_sp = sp_data[sp_data['cidade_suspeita'] == True]
    if len(suspicious_sp) > 0:
        logger.warning(f"⚠️ Cidades suspeitas em SP: {len(suspicious_sp):,}")
        suspicious_cities = suspicious_sp['cidade'].value_counts()
        for city, count in suspicious_cities.head(5).items():
            logger.warning(f"   • {city}: {count:,}")
    
    # Criar flag de validação para SP
    df['is_sp_validated'] = sp_mask & (~df['cidade_suspeita'])
    
    validated_sp = df[df['is_sp_validated'] == True]
    logger.info(f"✅ Registros SP validados: {len(validated_sp):,}")
    
    # ✅ NOVA MÉTRICA: Cidades validadas vs total
    validated_cities = validated_sp['cidade'].nunique()
    logger.info(f"🏙️ Cidades SP validadas: {validated_cities:,} de {total_sp_cities:,}")
    
    return df, validated_sp

def clipping_regional(df):
    """Task 3: Recorte Regional - Foco São Paulo"""
    logger.info("    Criando recorte regional - São Paulo...")
    
    # Filtrar dados validados de SP
    sp_df = df[df['is_sp_validated'] == True].copy()
    
    if len(sp_df) == 0:
        logger.error("❌ Nenhum dado validado de SP para análise!")
        return pd.DataFrame()
    
    # Adicionar métricas regionais
    logger.info("    Criando métricas regionais...")
    
    # Ranking de cidades por volume de reclamações
    city_ranking = sp_df.groupby('cidade').agg({
        'cidade': 'count',
        'is_agibank': 'sum',
        'foi_respondida': 'mean',
        'tempo_resposta_dias': 'mean'
    }).round(2)
    
    city_ranking.columns = ['total_reclamacoes', 'reclamacoes_agibank', 'taxa_resposta', 'tempo_medio_resposta']
    city_ranking = city_ranking.sort_values('total_reclamacoes', ascending=False)
    
    # Adicionar percentual Agibank por cidade
    city_ranking['percentual_agibank'] = (
        city_ranking['reclamacoes_agibank'] / city_ranking['total_reclamacoes'] * 100
    ).round(2)
    
    # Adicionar ranking de cidades ao dataset principal
    sp_df['cidade_ranking'] = sp_df['cidade'].map(
        city_ranking.reset_index().reset_index().set_index('cidade')['index'] + 1
    )
    
    logger.info(f"✅ Recorte regional criado: {len(sp_df):,} registros")
    logger.info(f"   Cidades analisadas: {len(city_ranking):,}")
    
    return sp_df, city_ranking

def clipping_age(sp_df):
    """Task 4: Recorte Etário - Perfil do consumidor"""
    logger.info("    Criando recorte etário...")
    
    if 'faixa_etaria' not in sp_df.columns:
        logger.warning("⚠️ Coluna 'faixa_etaria' não encontrada")
        return sp_df, pd.DataFrame()
    
    # Análise por faixa etária
    age_analysis = sp_df.groupby('faixa_etaria').agg({
        'faixa_etaria': 'count',
        'is_agibank': 'sum',
        'foi_respondida': 'mean',
        'tempo_resposta_dias': 'mean',
        'resolvido': 'mean' if 'resolvido' in sp_df.columns else lambda x: None
    }).round(2)
    
    age_analysis.columns = ['total_reclamacoes', 'reclamacoes_agibank', 'taxa_resposta', 'tempo_medio_resposta', 'taxa_resolucao']
    age_analysis = age_analysis.sort_values('total_reclamacoes', ascending=False)
    
    # Calcular percentuais
    age_analysis['percentual_total'] = (
        age_analysis['total_reclamacoes'] / age_analysis['total_reclamacoes'].sum() * 100
    ).round(2)
    
    age_analysis['percentual_agibank'] = (
        age_analysis['reclamacoes_agibank'] / age_analysis['total_reclamacoes'] * 100
    ).round(2)
    
    # Análise específica Agibank por idade
    agibank_age = sp_df[sp_df['is_agibank'] == True].groupby('faixa_etaria').agg({
        'faixa_etaria': 'count',
        'foi_respondida': 'mean',
        'tempo_resposta_dias': 'mean'
    }).round(2)
    
    agibank_age.columns = ['reclamacoes_agibank', 'taxa_resposta_agibank', 'tempo_resposta_agibank']
    
    logger.info(f"✅ Análise etária criada: {len(age_analysis)} faixas etárias")
    logger.info("    Top 3 faixas etárias:")
    
    for age_group, data in age_analysis.head(3).iterrows():
        logger.info(f"   • {age_group}: {data['total_reclamacoes']:,} reclamações ({data['percentual_total']}%)")
    
    return sp_df, age_analysis, agibank_age

def clipping_sectoral(sp_df):
    """Task 5: Recorte Setorial - Análise de mercado e problemas"""
    logger.info("    Criando recorte setorial...")
    
    sectoral_results = {}
    
    # 1. Análise por Segmento de Mercado
    if 'segmento_de_mercado' in sp_df.columns:
        logger.info("        Analisando segmentos de mercado...")
        
        segment_analysis = sp_df.groupby('segmento_de_mercado').agg({
            'segmento_de_mercado': 'count',
            'is_agibank': 'sum',
            'foi_respondida': 'mean'
        }).round(2)
        
        segment_analysis.columns = ['total_reclamacoes', 'reclamacoes_agibank', 'taxa_resposta']
        segment_analysis = segment_analysis.sort_values('total_reclamacoes', ascending=False)
        
        sectoral_results['segments'] = segment_analysis
    
    # 2. Análise por Área
    if 'area' in sp_df.columns:
        logger.info("       Analisando áreas de negócio...")
        
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
                'is_agibank': 'sum',
                'foi_respondida': 'mean',
                'tempo_resposta_dias': 'mean'
            }).round(2)
            
            bank_comparison.columns = ['total_reclamacoes', 'is_agibank_flag', 'taxa_resposta', 'tempo_medio_resposta']
            bank_comparison = bank_comparison.sort_values('total_reclamacoes', ascending=False)
            
            # Identificar posição do Agibank
            agibank_banks = bank_comparison[bank_comparison['is_agibank_flag'] > 0]
            
            sectoral_results['banking_comparison'] = bank_comparison
            sectoral_results['agibank_position'] = agibank_banks
            
            logger.info(f"       Bancos analisados: {len(bank_comparison)}")
            logger.info(f"       Registros Agibank no setor: {agibank_banks['total_reclamacoes'].sum():,}")
    
    # 3. Análise por Problema
    if 'problema' in sp_df.columns:
        logger.info("   ⚠️ Analisando tipos de problemas...")
        
        # Problemas gerais
        problem_analysis = sp_df.groupby('problema').agg({
            'problema': 'count',
            'is_agibank': 'sum'
        })
        
        problem_analysis.columns = ['total_ocorrencias', 'ocorrencias_agibank']
        problem_analysis = problem_analysis.sort_values('total_ocorrencias', ascending=False)
        
        # Problemas específicos do Agibank
        agibank_problems = sp_df[sp_df['is_agibank'] == True]['problema'].value_counts()
        
        sectoral_results['problems_general'] = problem_analysis
        sectoral_results['problems_agibank'] = agibank_problems
        
        logger.info(f"      Tipos de problemas identificados: {len(problem_analysis)}")
        logger.info(f"      Problemas específicos Agibank: {len(agibank_problems)}")
    
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
    logger.info(" Iniciando DAG Gold - Foco São Paulo...")
    start_time = datetime.now()
    
    try:
        # Pipeline Gold
        df = load_silver_data()
        df, validated_sp = verification_sp_cities(df)
        sp_df, city_ranking = clipping_regional(df)
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