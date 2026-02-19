import pandas as pd
from pathlib import Path


RAIZ_PROJETO = Path(__file__).parent.parent
CAMINHO_DATA = RAIZ_PROJETO / 'data'
CAMINHO_SILVER = CAMINHO_DATA / 'silver'
CAMINHO_GOLD = CAMINHO_DATA / 'gold'

ARQUIVO_SILVER_PADRAO = 'consumidor_gov_silver_v1.csv'
ARQUIVO_SP_COMPLETO = 'sp_consumidor_completo_v1.csv'
ARQUIVO_SP_AGIBANK = 'sp_agibank_only_v1.csv'


def carregar_base_silver(caminho: str = None) -> pd.DataFrame:
    """Carrega base Silver (Brasil completo)"""
    if caminho is None:
        caminho = CAMINHO_SILVER / ARQUIVO_SILVER_PADRAO
    else:
        caminho = Path(caminho)
    
    print(f"Carregando de: {caminho}")
    
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")
    
    df = pd.read_csv(
        caminho,
        sep=';',
        on_bad_lines='skip',
        encoding='utf-8',
        low_memory=False
    )
    
    print(f"Base carregada com sucesso!")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    return df


def carregar_base_gold_sp(caminho: str = None) -> pd.DataFrame:
    """Carrega base Gold (Sao Paulo completo)"""
    if caminho is None:
        caminho = CAMINHO_GOLD / ARQUIVO_SP_COMPLETO
    else:
        caminho = Path(caminho)
    
    print(f"Carregando SP (Gold) de: {caminho}")
    
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")
    
    df = pd.read_csv(
        caminho,
        encoding='utf-8',
        low_memory=False
    )
    
    print(f"Base SP carregada com sucesso!")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    return df


def carregar_base_agibank(caminho: str = None) -> pd.DataFrame:
    """Carrega base Agibank (Gold)"""
    if caminho is None:
        caminho = CAMINHO_GOLD / ARQUIVO_SP_AGIBANK
    else:
        caminho = Path(caminho)
    
    print(f"Carregando Agibank de: {caminho}")
    
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")
    
    df = pd.read_csv(
        caminho,
        encoding='utf-8',
        low_memory=False
    )
    
    print(f"Base Agibank carregada com sucesso!")
    print(f"Registros: {len(df):,}")
    print(f"Colunas: {len(df.columns)}")
    
    return df


def carregar_base_filtrada(filtro_agibank: bool = None, ano: int = None) -> pd.DataFrame:
    """Carrega base Silver com filtros aplicados"""
    df = carregar_base_silver()
    
    if filtro_agibank is not None:
        if 'is_agibank' in df.columns:
            df = df[df['is_agibank'] == filtro_agibank].copy()
            print(f"Filtrado is_agibank={filtro_agibank}: {len(df):,} registros")
        else:
            print(f"Coluna 'is_agibank' nao encontrada. Tentando por 'nome_fantasia'...")
            if 'nome_fantasia' in df.columns:
                df = df[df['nome_fantasia'].str.contains('Agibank', case=False, na=False)].copy()
                print(f"Filtrado por nome_fantasia: {len(df):,} registros")
    
    if ano is not None:
        if 'ano_abertura' in df.columns:
            df = df[df['ano_abertura'] == ano].copy()
            print(f"Filtrado ano={ano}: {len(df):,} registros")
        else:
            print(f"Coluna 'ano_abertura' nao encontrada")
    
    return df


def listar_arquivos_disponiveis():
    """Lista arquivos CSV disponiveis nas camadas Silver e Gold"""
    print("="*80)
    print("ARQUIVOS DISPONIVEIS")
    print("="*80)
    
    print(f"\nSILVER ({CAMINHO_SILVER}):")
    if CAMINHO_SILVER.exists():
        arquivos_silver = list(CAMINHO_SILVER.glob('*.csv'))
        if arquivos_silver:
            for arquivo in arquivos_silver:
                tamanho_mb = arquivo.stat().st_size / (1024**2)
                print(f"   {arquivo.name:<50} {tamanho_mb:>8.1f} MB")
        else:
            print("   Nenhum arquivo CSV encontrado")
    else:
        print("   Pasta nao existe")
    
    print(f"\nGOLD ({CAMINHO_GOLD}):")
    if CAMINHO_GOLD.exists():
        arquivos_gold = list(CAMINHO_GOLD.glob('*.csv'))
        if arquivos_gold:
            for arquivo in arquivos_gold:
                tamanho_mb = arquivo.stat().st_size / (1024**2)
                print(f"   {arquivo.name:<50} {tamanho_mb:>8.1f} MB")
        else:
            print("   Nenhum arquivo CSV encontrado")
    else:
        print("   Pasta nao existe")
    
    print("="*80)


def info_base(df: pd.DataFrame):
    """Exibe informacoes resumidas do DataFrame"""
    print("=" * 80)
    print("INFORMACOES DA BASE")
    print("=" * 80)
    print(f"\nTotal de registros: {len(df):,}")
    print(f"Total de colunas: {len(df.columns)}")
    
    if 'ano_abertura' in df.columns:
        anos = df['ano_abertura'].dropna()
        if len(anos) > 0:
            print(f"Periodo: {int(anos.min())} a {int(anos.max())}")
    
    memoria_mb = df.memory_usage(deep=True).sum() / (1024**2)
    print(f"Memoria utilizada: {memoria_mb:.2f} MB")
    print("=" * 80)