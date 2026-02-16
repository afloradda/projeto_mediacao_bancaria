import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime
import sys

sys.path.append('..')
from config.settings import QUALITY_CHECKS, AGIBANK_FILTERS, TEMPORAL_COLUMNS_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(messages)s')
logger = logging.getLogger(__name__)


def load_bronze_data():
    """Task 1: Carregar dados da camada Bronze..."""
    logger.info("Carregando dados da camada Bronze...")

    version = 2
    bronze_file = f"../data/silver/consumidor_gov_bronze_v{version}.csv"

    if not Path(bronze_file).exists():
        raise FileNotFoundError(f"Arquivo Bronze não encontrado: {bronze_file}")
    
    df = pd.read_csv(bronze_file, sep=';', encoding='utf-8')
    logger.info(f"✅ Dados carregados: {len(df)} registros, {len(df.columns)} colunas")

    return df

def standardize_column_names(df):
    """Task 2: Padronizar nomes das colunas"""
    logger.info("Padronizando nomes das colunas...")

    original_columns = df.columns.tolist()

    
    df.columns = df.columns.str.strip()

    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('__+', '_', regex=True)

    df.columns = df.columns.str.normalize('NFD').str.encode('ascii', errors='ignore').str.decode('ascii')
    # Exemplo transformação: Região -> Regia\u0303o -> b'Regiao' -> Regiao

    df.columns = df.columns.str.lower()

    column_mapping = dict(zip(original_columns, df.columns)) # zip() combina as listas e dict transforma em dicionário 

    key_changes = {}
    for old, new in column_mapping.items():
        if old != new:
            key_changes[old] = new
    # Equivalente a: key_changes = {old: new for old, new in column_mapping.items() if old != new}

    if key_changes:
        logger.info("Principais mudanças nas colunas:")
        for old, new in list (key_changes.items())[:5]:
            logger.info(f"  {old} -> {new}")
    
    logger.info(f"✅ Colunas padronizadas: {len(df.columns)} colunas")
    return df

def convert_temporal_columns(df):
    """Task 3: Converter colunas temporais"""
    logger.info("Convertendo colunas temporais...")

    datetime_columns = TEMPORAL_COLUMNS_CONFIG['datetime_columns']
    conversion_stats = {}

    for col in datetime_columns:
        if col not in df.columns:
            logger.warning(f"   Coluna '{col}' não encontrada")
            continue

        logger.info(f"  Convertendo: {col}")

        non_null_before = df[col].notna().sum()

        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(['', 'nan', 'NaN', 'NULL', 'null', 'None'], pd.NaT)

        try:
            if col == 'processed_at':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

            non_null_after = df[col].notna().sum()
            success_rate = (non_null_after / non_null_before * 100) if non_null_before > 0 else 0

            conversion_stats[col] = {
                'before': non_null_before,
                'after': non_null_after,
                'success_rate': success_rate
            }

            logger.info(f"  {non_null_before:,} -> {non_null_after:,} ({success_rate:.1f}% sucesso)")
        
        except Exception as e:
            logger.error(f" Erro convertendo '{col}': {str(e)}")
            conversion_stats[col] = {'before': non_null_before, 'after': 0, 'success_rate': 0}

    sucessful = sum(1 for stats in conversion_stats.values() if stats['success_rate'] > 0)
    logger.info(f"✅ Conversão temporais: {sucessful}/{len(conversion_stats)} bem-sucessidas")

    return df, conversion_stats

def convert_categorical_columns(df):
    """Task 4: Converter colunas apropriadas para category"""
    logger.info("🏷️ Convertendo colunas categóricas...")
    
    # Candidatas automáticas (baixa cardinalidade)
    categorical_candidates = ['regiao', 'uf', 'sexo', 'respondida', 'situacao', 'data_source']
    
    for col in categorical_candidates:
        if col in df.columns:
            unique_count = df[col].nunique()
            total_count = len(df)
            ratio = unique_count / total_count
            
            # Se menos de 1% de valores únicos, converter para category
            if ratio < 0.01:
                df[col] = df[col].astype('category')
                logger.info(f"   🏷️ {col}: {unique_count} categorias ({ratio:.2%})")
    
    return df

def final_cleanup(df):
    """Task 5: Limpeza final - duplicatas"""
    logger.info("🧹 Limpeza final...")
    
    original_rows = len(df)
    df_clean = df.drop_duplicates()
    duplicates_removed = original_rows - len(df_clean)
    
    if duplicates_removed > 0:
        logger.info(f"   🗑️ Duplicatas finais removidas: {duplicates_removed:,}")
    
    return df_clean

def silver_dag():
    """DAG principal da camada silver"""
    logger.info("Iniciando DAG Silver...")
    start_time = datetime.now()
    version = 0

    try: 
        # Pipeline Silver
        logger.info("Carregando dados Bronze...")
        df = load_bronze_data()

        if df:
            logger.info("   Padronizando colunas...")
            df = standardize_column_names(df)

            logger.info("   Convertendo colunas temporais...")
            df, conversion_stats = convert_temporal_columns(df)

            logger.info("   Convertendo colunas categóricas...")
            df = convert_categorical_columns(df)

            logger.info("   Limpeza final (eliminação de duplicatas)...")
            df = final_cleanup(df)


            # Salvar resultado Silver
            output_path = f"../data/silver/consumidor_gov_silver_v{version + 1}.csv"
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False, encoding='utf-8', sep=';')

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("-"*70)
            logger.info("RELATÓRIO SILVER DAG \n")
            logger.info(f"Duração: {duration}")
            logger.info(f"Registros processados: {len(df):,}")
            logger.info(f"Colunas finais: {len(df.columns)}")
            logger.info(f"\nRegistros Agibank: {df['is_agibank'].sum():,}")
            logger.info(f"\nConversões temporais: {sum(1 for s in conversion_stats.values() if s['success_rate'] > 0)}")
            logger.info(f"\nArquivo salvo: {output_path}")
            logger.info(f"✅    Silver DAG concluído    ")
            logger.info("-"*70)
        
        return df, output_path  # Retornar df e caminho para usar no Gold

    except Exception as e:
        logger.error(f"Error DAG Silver: {str(e)}")
        raise


if __name__ == "__main__":
    silver_dag()
