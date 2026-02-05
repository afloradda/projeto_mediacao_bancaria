import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime
import sys

sys.path.append('..')
from config.settings import QUALITY_CHECKS, PROCESSING_CONFIG, AGIBANK_FILTERS, CONSUMIDOR_GOV_DELETE_COLUMNS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_files():
    
    logger.info("Validando arquivos disponíveis...")

    consumidor_files = glob.glob("../data/bronze/consumidor_gov/*.csv") 
    print(f"Temos {len(consumidor_files)} arquivos do Consumidor.gov")

    if len(consumidor_files) == 0:
        raise FileNotFoundError("Nenhum arquivo Consumidor.gov encontrado!")
    
    for file in consumidor_files:
        if "2025" not in Path(file).name:
            logger.warning(f"Arquivo pode não ser de 2025: {Path(file).name}")

    logger.info(f"Validação de arquivos concluída:")
    return consumidor_files


def explore_data_structure(file_path): 
    
    logger.info(f"Explorando estrutura: {Path(file_path).name}")

    sample_df = pd.read_csv(file_path, sep=';', encoding='utf-8')

    info = {
        'columns': list(sample_df.columns),
        'dtypes': sample_df.dtypes.to_dict(),
        'sample_shape': sample_df.shape,
        'file_name': Path(file_path).name
    }

    logger.info(f"  Colunas: {len(info['columns'])}")
    logger.info(f"  Primeiras colunas: {info['columns'][:5]}")

    return info


def delete_columns_dispensaveis(df, file_path):
    logger.info(f"Deletando colunas dispensáveis dentro do dataframe: {Path(file_path).name}")

    columns_to_delete = CONSUMIDOR_GOV_DELETE_COLUMNS['columns']

    existing_columns = [col for col in columns_to_delete if col in df.columns]
    missing_columns = [col for col in columns_to_delete if col not in df.columns]

    if existing_columns:
        df_cleaned = df.drop(columns=existing_columns)
        logger.info(f"Deletadas: {len(existing_columns)}x \n  Colunas: {existing_columns}")
    else: 
        df_cleaned = df.copy()
        logger.info(" Nenhuma coluna para deletar encontrada.")

    if missing_columns:
        logger.info(f"Colunas não encontradas: {missing_columns}")

    original_cols = len(df.columns)
    final_cols = len(df_cleaned.columns)
    logger.info(f"  Colunas: {original_cols} -> {final_cols}")

    return df_cleaned
    

def add_metadata_columns(df, file_path, source_type):
    """Task 3: Adicionar colunas de metadados"""
    logger.info("Adicionando metadados...")

    file_name = Path(file_path).name

    df['data_source'] = source_type
    df['file_origin'] = file_name
    df['processed_at'] = datetime.now()

    if source_type == 'consumidor_gov':
        try:
            month_year = file_name.split('2025-')[1].split('.')[0]
            df['file_month'] = f"{month_year}/2025"
        except:
            df['file_month'] = 'Desconhecido'

    df['is_agibank'] = False

    logger.info("Metadados adicionados com sucessso")
    logger.info(df.columns[-5:])
    return df


def filter_agibank_records(df):
    """Task 4: Identificar e marcar registros do Agibank"""
    logger.info("Identificando registros Agibank...")

    possible_company_cols = ['Nome Fantasia']
    company_col = None

    for col in possible_company_cols:
        if col in df.columns:
            company_col = col
            break

    if company_col:
        agibank_mask = df[company_col].str.contains(
            '|'.join(AGIBANK_FILTERS['bank_names']),
            case=False,
            na=False
        )

        df['is_agibank'] = agibank_mask
        agibank_count = agibank_mask.sum()

        logger.info(f"Registros Agibank encontrados: {agibank_count}")
        logger.info(f"Percentual Agibank: {(agibank_count/len(df)*100):.2f}%")
    else:
        logger.warning("Coluna de empresa não identificada")

    return df

def clean_duplicates(df, file_name):

    logger.info(f"  Limpando duplicatas: {file_name}")

    original_rows = len(df)

    df_cleaned = df.drop_duplicates()
    duplicates_removed = original_rows - len(df_cleaned)

    logger.info(f"   Duplicatas removidas: {duplicates_removed}")

    return df_cleaned


def quality_check(df, file_name):
    """Task 5: Verificação de qualidade dos dados"""
    logger.info(f"Executando verificações de qualidade: {file_name}")

    issues = []

    # Check 1: Linhas mínimas
    if len(df) < QUALITY_CHECKS['min_rows_expected']:
        issues.append(f"Poucas linhas: {len(df)} < {QUALITY_CHECKS['min_rows_expected']}")

    # Check 2: Percentual de nulos por coluna
    null_percentages = df.isnull().mean()
    high_null_cols = null_percentages[null_percentages > QUALITY_CHECKS['max_null_percentage']]

    if not high_null_cols.empty:
        issues.append(f"Colunas com muitos nulos: {list(high_null_cols)}")

    # Check 3: Duplicatas
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        issues.append(f"Registros duplicados: {duplicates}")

    if issues:
        for issue in issues:
            logger.warning(f"{issue}")
    else:
        logger.info("Todos os checks de qualidade passaram")

    df['quality_score'] = 1.0 - (len(issues) * 0.1)

    return df, issues


def process_consumidor_gov():
    """Task 6: Processamento completo Consumidor.gov"""
    logger.info("Iniciando processamento Consumidor.gov...")

    consumidor_files = glob.glob("../data/bronze/consumidor_gov/*.csv")
    all_dataframes = []
    all_issues = []

    for file_path in sorted(consumidor_files):
        logger.info(f"   Processando: {Path(file_path).name}")

        try:
            # 1. Ler arquivo
            df = pd.read_csv(file_path, sep=';', encoding='utf-8')
            original_rows = len(df)

            # 2. Deletar colunas dispensáveis
            df = delete_columns_dispensaveis(df, file_path)
            
            # 3. Adicionar metadados
            df = add_metadata_columns(df, file_path, 'consumidor_gov')
            
            # 4. Identificar Agibank
            df = filter_agibank_records(df)
            
            # 5. Limpeza de duplicatas e nulos
            df = clean_duplicates(df, Path(file_path).name)
            
            # 6. Verificações de qualidade
            df, issues = quality_check(df, Path(file_path).name)

            all_issues.extend(issues)
            all_dataframes.append(df)

            logger.info(f"   ✅ {Path(file_path).name}: {original_rows} → {len(df)} registros")

        except Exception as e:
            logger.error(f"   Erro processando {Path(file_path).name}: {str(e)}")
            continue

    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Consumidor.gov processado: {len(combined_df)} registros totais")
        
        # Limpeza final de duplicatas entre arquivos
        original_combined = len(combined_df)
        combined_df = combined_df.drop_duplicates()
        final_combined = len(combined_df)
        
        if original_combined != final_combined:
            logger.info(f"  Duplicatas entre arquivos removidas: {original_combined - final_combined}")
        
        return combined_df, all_issues
    else:
        raise Exception("Nenhum arquivo foi processado com sucesso!")


def save_bronze_output(df, output_path):
    """Task 7: Salvar dados processados"""
    logger.info(f"Salvando dados bronze: {output_path}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False, encoding='utf-8', sep=';')

    logger.info(f"Total de registros: {len(df)}")
    logger.info(f"Registro Agibank: {df['is_agibank'].sum()}")
    logger.info(f"Colunas: {len(df.columns)}")

    return True


def bronze_dag():
    """DAG principal da camada bronze"""
    logger.info("Iniciando DAG Bronze...")
    start_time = datetime.now()
    version = 1

    try:
        consumidor_files = validate_files()

        if consumidor_files:
            explore_data_structure(consumidor_files[0])

            df_consumidor, issues = process_consumidor_gov()

            save_bronze_output(df_consumidor, f"../data/silver/consumidor_gov_bronze_v{version+1}.csv")

            end_time = datetime.now()
            duration = end_time - start_time

            logger.info("-"*70)
            logger.info("RELATÓRIO BRONZE DAG\n")
            logger.info(f"Duração: {duration}")
            logger.info(f"Registros processados: {len(df_consumidor)}")
            logger.info(f"Registros Agibank: {df_consumidor['is_agibank'].sum()}")
            logger.info(f"Issues de qualidade: {len(issues)}")
            logger.info("\nDAG Bronze concluida com sucesso!")
            logger.info("-"*70)

    except Exception as e:
        logger.error(f"Erro DAG Bronze: {str(e)}")
        raise

if __name__ == "__main__":
    bronze_dag()