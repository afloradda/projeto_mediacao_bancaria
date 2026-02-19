# lib/visualizacao.py

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from lib.cores import CORES_AGIBANK, PALETA_CATEGORICA, PALETA_AZUL, PALETA_VERDE


def grafico_barras(df: pd.DataFrame, x: str, y: str, titulo: str, 
                   paleta: list = None, horizontal: bool = False, top_n: int = None):
    """
    Gera gráfico de barras
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y: Nome da coluna para eixo Y
        titulo: Título do gráfico
        paleta: Lista de cores (opcional)
        horizontal: Se True, gera barras horizontais
        top_n: Limita aos N primeiros registros
    """
    if paleta is None:
        paleta = PALETA_CATEGORICA
    
    df_plot = df.copy()
    if top_n:
        df_plot = df_plot.head(top_n)
    
    plt.figure(figsize=(12, 6))
    
    if horizontal:
        sns.barplot(data=df_plot, y=x, x=y, palette=paleta)
        plt.xlabel(y)
        plt.ylabel(x)
    else:
        sns.barplot(data=df_plot, x=x, y=y, palette=paleta)
        plt.xlabel(x)
        plt.ylabel(y)
    
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()


def grafico_linha(df: pd.DataFrame, x: str, y: str, titulo: str, 
                  cor: str = None, hue: str = None):
    """
    Gera gráfico de linhas
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y: Nome da coluna para eixo Y
        titulo: Título do gráfico
        cor: Cor da linha (opcional)
        hue: Coluna para múltiplas linhas (opcional)
    """
    if cor is None:
        cor = CORES_AGIBANK['azul_principal']
    
    plt.figure(figsize=(12, 6))
    
    if hue:
        sns.lineplot(data=df, x=x, y=y, hue=hue, palette=PALETA_CATEGORICA, marker='o')
    else:
        sns.lineplot(data=df, x=x, y=y, color=cor, marker='o')
    
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.xlabel(x)
    plt.ylabel(y)
    plt.tight_layout()
    plt.show()


def grafico_pizza(valores: pd.Series, titulo: str, top_n: int = 10):
    """
    Gera gráfico de pizza
    
    Args:
        valores: Series com valores para o gráfico
        titulo: Título do gráfico
        top_n: Número de fatias a exibir
    """
    dados = valores.head(top_n)
    
    plt.figure(figsize=(10, 10))
    plt.pie(dados.values, labels=dados.index, autopct='%1.1f%%', 
            colors=PALETA_CATEGORICA, startangle=90)
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()


def grafico_boxplot(df: pd.DataFrame, x: str, y: str, titulo: str):
    """
    Gera gráfico boxplot
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna categórica
        y: Nome da coluna numérica
        titulo: Título do gráfico
    """
    plt.figure(figsize=(12, 6))
    sns.boxplot(data=df, x=x, y=y, palette=PALETA_CATEGORICA)
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.xlabel(x)
    plt.ylabel(y)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def grafico_heatmap(df: pd.DataFrame, titulo: str, fmt: str = '.0f'):
    """
    Gera heatmap
    
    Args:
        df: DataFrame com dados para heatmap
        titulo: Título do gráfico
        fmt: Formato dos valores nas células
    """
    plt.figure(figsize=(12, 8))
    sns.heatmap(df, annot=True, fmt=fmt, cmap='Blues', cbar=True)
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.show()


def grafico_distribuicao(df: pd.DataFrame, coluna: str, titulo: str):
    """
    Gera histograma com curva de densidade
    
    Args:
        df: DataFrame com os dados
        coluna: Nome da coluna numérica
        titulo: Título do gráfico
    """
    plt.figure(figsize=(12, 6))
    sns.histplot(data=df, x=coluna, kde=True, color=CORES_AGIBANK['azul_principal'])
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.xlabel(coluna)
    plt.ylabel('Frequencia')
    plt.tight_layout()
    plt.show()


def grafico_comparativo_barras(df: pd.DataFrame, x: str, y1: str, y2: str, 
                               titulo: str, labels: list = None):
    """
    Gera gráfico de barras comparativo (lado a lado)
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y1: Nome da primeira coluna numérica
        y2: Nome da segunda coluna numérica
        titulo: Título do gráfico
        labels: Lista com nomes das legendas [y1, y2]
    """
    if labels is None:
        labels = [y1, y2]
    
    x_pos = range(len(df))
    width = 0.35
    
    plt.figure(figsize=(12, 6))
    plt.bar([p - width/2 for p in x_pos], df[y1], width, 
            label=labels[0], color=CORES_AGIBANK['azul_principal'])
    plt.bar([p + width/2 for p in x_pos], df[y2], width, 
            label=labels[1], color=CORES_AGIBANK['verde'])
    
    plt.xlabel(x)
    plt.ylabel('Valores')
    plt.title(titulo, fontsize=14, fontweight='bold')
    plt.xticks(x_pos, df[x], rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()