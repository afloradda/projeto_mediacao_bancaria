import pandas as pd
from pathlib import Path

CAMINHO_SILVER = Path('data/silver/consumidor_gov_silver_v2.csv')

def carregar_base_silver(caminho: str = None) -> pd.DataFrame:
    if caminho is None:
        caminho = CAMINHO_SILVER
    
    df = pd.read_csv(caminho)
    
    print(f"Base carregada com sucesso!")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    return df

def carregar_base_filtrada(filtro_agibank: bool = None, ano: int = None) -> pd.DataFrame:
    df = carregar_base_silver()
    
    if filtro_agibank is not None:
        df = df[df['is_agibank'] == filtro_agibank]
        print(f"Filtrado is_agibank={filtro_agibank}: {len(df):,} registros")
    
    if ano is not None:
        df = df[df['Ano Abertura'] == ano]
        print(f"Filtrado ano={ano}: {len(df):,} registros")
    
    return df

def info_base(df: pd.DataFrame):
    print("=" * 80)
    print("INFORMACOES DA BASE")
    print("=" * 80)
    print(f"\nTotal de registros: {len(df):,}")
    print(f"Total de colunas: {len(df.columns)}")
    print(f"Periodo: {df['Ano Abertura'].min()} a {df['Ano Abertura'].max()}")
    print(f"Memoria utilizada: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print("\nColunas disponiveis:")
    for col in df.columns:
        print(f"  - {col}")