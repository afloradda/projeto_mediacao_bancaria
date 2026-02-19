# lib/cores.py

import seaborn as sns
import matplotlib.pyplot as plt
from typing import List

# ============================================================
# PALETAS DE CORES AGIBANK
# ============================================================

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

# ============================================================
# PALETAS PARA PLOTLY (mesmo esquema de cores)
# ============================================================

PLOTLY_COLORS = CORES_AGIBANK
PLOTLY_PALETTE = PALETA_CATEGORICA
PLOTLY_SCALE_AZUL = PALETA_AZUL
PLOTLY_SCALE_VERDE = PALETA_VERDE

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def get_cor(nome: str) -> str:
    """Retorna cor do dicionario CORES_AGIBANK"""
    return CORES_AGIBANK.get(nome, CORES_AGIBANK['azul_principal'])


def configurar_estilo(tamanho: str = 'medio', paleta: List[str] = None):
    """Configura estilo dos graficos Matplotlib/Seaborn"""
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
    """Aplica tema visual Agibank aos graficos Matplotlib/Seaborn"""
    configurar_estilo(tamanho, PALETA_CATEGORICA)
    print(f"Tema Agibank aplicado - Tamanho: {tamanho}")


def configurar_plotly():
    """Configura tema Agibank para Plotly"""
    try:
        import plotly.express as px
        
        px.defaults.template = "plotly_white"
        px.defaults.color_discrete_sequence = PLOTLY_PALETTE
        px.defaults.width = 900
        px.defaults.height = 500
        
        print("Tema Agibank aplicado ao Plotly")
        return True
    except ImportError:
        print("Plotly nao esta instalado")
        return False


# ============================================================
# AUTO-APLICAR TEMA MATPLOTLIB
# ============================================================

aplicar_tema_agibank()