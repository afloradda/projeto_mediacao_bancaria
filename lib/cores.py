import seaborn as sns
import matplotlib.pyplot as plt
from typing import List

CORES_AGIBANK = {
    'azul_principal': '#0064f5',
    'azul_medio': '#0053b0',
    'azul_escuro': '#000f44',
    'verde': '#77df40',
    'verde_claro': '#c5ff90',
    'amarelo': '#ffd600',
    'branco': '#ffffff'
}

PALETA_CATEGORICA = [
    CORES_AGIBANK['azul_principal'],
    CORES_AGIBANK['verde'],
    CORES_AGIBANK['amarelo'],
    CORES_AGIBANK['azul_medio'],
    CORES_AGIBANK['verde_claro'],
    CORES_AGIBANK['azul_escuro']
]

PALETA_AZUL = [
    CORES_AGIBANK['azul_escuro'],
    CORES_AGIBANK['azul_medio'],
    CORES_AGIBANK['azul_principal']
]

PALETA_VERDE = [
    CORES_AGIBANK['verde'],
    CORES_AGIBANK['verde_claro']
]

def get_cor(nome: str) -> str:
    return CORES_AGIBANK.get(nome, CORES_AGIBANK['azul_principal'])

def configurar_estilo(tamanho: str = 'medio', paleta: List[str] = None):
    tamanhos = {
        'pequeno': (8, 5),
        'medio': (12, 6),
        'grande': (16, 8)
    }
    
    if paleta is None:
        paleta = PALETA_CATEGORICA
    
    sns.set_theme(
        style='whitegrid',
        palette=paleta,
        font_scale=1.1
    )
    
    plt.rcParams['figure.figsize'] = tamanhos.get(tamanho, tamanhos['medio'])
    plt.rcParams['figure.dpi'] = 100
    plt.rcParams['axes.facecolor'] = 'white'
    plt.rcParams['figure.facecolor'] = 'white'
    plt.rcParams['grid.alpha'] = 0.3

def aplicar_tema_agibank(tamanho: str = 'medio'):
    configurar_estilo(tamanho, PALETA_CATEGORICA)
    print(f"Tema Agibank aplicado - Tamanho: {tamanho}")

aplicar_tema_agibank()