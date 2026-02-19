# lib/visualizacoes.py

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path

# Imports relativos corrigidos
try:
    from .cores import CORES_AGIBANK, PALETA_CATEGORICA, PALETA_AZUL, PALETA_VERDE
except ImportError:
    from cores import CORES_AGIBANK, PALETA_CATEGORICA, PALETA_AZUL, PALETA_VERDE


def grafico_barras(df: pd.DataFrame, x: str, y: str, titulo: str, 
                   paleta: list = None, horizontal: bool = False, top_n: int = None,
                   rotacao: int = 45, figsize: tuple = (12, 6), salvar: str = None):
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
        rotacao: Ângulo de rotação dos labels do eixo X
        figsize: Tamanho da figura (largura, altura)
        salvar: Caminho para salvar o gráfico (opcional)
    """
    if paleta is None:
        paleta = PALETA_CATEGORICA
    
    df_plot = df.copy()
    if top_n:
        df_plot = df_plot.head(top_n)
    
    plt.figure(figsize=figsize)
    
    if horizontal:
        ax = sns.barplot(data=df_plot, y=x, x=y, palette=paleta)
        plt.xlabel(y, fontsize=12)
        plt.ylabel(x, fontsize=12)
    else:
        ax = sns.barplot(data=df_plot, x=x, y=y, palette=paleta)
        plt.xlabel(x, fontsize=12)
        plt.ylabel(y, fontsize=12)
        plt.xticks(rotation=rotacao, ha='right')
    
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_linha(df: pd.DataFrame, x: str, y: str, titulo: str, 
                  cor: str = None, hue: str = None, marker: str = 'o',
                  figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de linhas
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y: Nome da coluna para eixo Y
        titulo: Título do gráfico
        cor: Cor da linha (opcional)
        hue: Coluna para múltiplas linhas (opcional)
        marker: Estilo do marcador
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    if cor is None:
        cor = CORES_AGIBANK['azul_principal']
    
    plt.figure(figsize=figsize)
    
    if hue:
        sns.lineplot(data=df, x=x, y=y, hue=hue, palette=PALETA_CATEGORICA, 
                     marker=marker, linewidth=2.5)
        plt.legend(title=hue, bbox_to_anchor=(1.05, 1), loc='upper left')
    else:
        sns.lineplot(data=df, x=x, y=y, color=cor, marker=marker, linewidth=2.5)
    
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel(x, fontsize=12)
    plt.ylabel(y, fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_pizza(valores: pd.Series, titulo: str, top_n: int = 10, 
                  figsize: tuple = (10, 10), salvar: str = None):
    """
    Gera gráfico de pizza
    
    Args:
        valores: Series com valores para o gráfico
        titulo: Título do gráfico
        top_n: Número de fatias a exibir
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    dados = valores.head(top_n)
    
    # Se houver mais dados, agrupa o resto em "Outros"
    if len(valores) > top_n:
        outros = valores.iloc[top_n:].sum()
        dados = pd.concat([dados, pd.Series({'Outros': outros})])
    
    plt.figure(figsize=figsize)
    plt.pie(dados.values, labels=dados.index, autopct='%1.1f%%', 
            colors=PALETA_CATEGORICA, startangle=90, textprops={'fontsize': 11})
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_boxplot(df: pd.DataFrame, x: str, y: str, titulo: str,
                    figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico boxplot
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna categórica
        y: Nome da coluna numérica
        titulo: Título do gráfico
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    sns.boxplot(data=df, x=x, y=y, palette=PALETA_CATEGORICA)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel(x, fontsize=12)
    plt.ylabel(y, fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_heatmap(df: pd.DataFrame, titulo: str, fmt: str = '.0f',
                    figsize: tuple = (12, 8), cmap: str = 'Blues', salvar: str = None):
    """
    Gera heatmap
    
    Args:
        df: DataFrame com dados para heatmap
        titulo: Título do gráfico
        fmt: Formato dos valores nas células
        figsize: Tamanho da figura
        cmap: Mapa de cores
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    sns.heatmap(df, annot=True, fmt=fmt, cmap=cmap, cbar=True, 
                linewidths=0.5, linecolor='white')
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_distribuicao(df: pd.DataFrame, coluna: str, titulo: str,
                         bins: int = 30, figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera histograma com curva de densidade
    
    Args:
        df: DataFrame com os dados
        coluna: Nome da coluna numérica
        titulo: Título do gráfico
        bins: Número de bins do histograma
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    sns.histplot(data=df, x=coluna, kde=True, color=CORES_AGIBANK['azul_principal'],
                 bins=bins, edgecolor='white', linewidth=0.5)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel(coluna, fontsize=12)
    plt.ylabel('Frequência', fontsize=12)
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_comparativo_barras(df: pd.DataFrame, x: str, y1: str, y2: str, 
                               titulo: str, labels: list = None,
                               figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de barras comparativo (lado a lado)
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y1: Nome da primeira coluna numérica
        y2: Nome da segunda coluna numérica
        titulo: Título do gráfico
        labels: Lista com nomes das legendas [y1, y2]
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    if labels is None:
        labels = [y1, y2]
    
    x_pos = range(len(df))
    width = 0.35
    
    plt.figure(figsize=figsize)
    plt.bar([p - width/2 for p in x_pos], df[y1], width, 
            label=labels[0], color=CORES_AGIBANK['azul_principal'])
    plt.bar([p + width/2 for p in x_pos], df[y2], width, 
            label=labels[1], color=CORES_AGIBANK['verde'])
    
    plt.xlabel(x, fontsize=12)
    plt.ylabel('Valores', fontsize=12)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xticks(x_pos, df[x], rotation=45, ha='right')
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_barras_empilhadas(df: pd.DataFrame, titulo: str, 
                              figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de barras empilhadas
    
    Args:
        df: DataFrame com dados (índice = categorias, colunas = subcategorias)
        titulo: Título do gráfico
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    df.plot(kind='bar', stacked=True, color=PALETA_CATEGORICA, 
            edgecolor='white', linewidth=0.5)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel('Categorias', fontsize=12)
    plt.ylabel('Valores', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Legenda', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_scatter(df: pd.DataFrame, x: str, y: str, titulo: str,
                    hue: str = None, size: str = None, 
                    figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de dispersão (scatter plot)
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y: Nome da coluna para eixo Y
        titulo: Título do gráfico
        hue: Coluna para colorir os pontos (opcional)
        size: Coluna para variar o tamanho dos pontos (opcional)
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    sns.scatterplot(data=df, x=x, y=y, hue=hue, size=size, 
                    palette=PALETA_CATEGORICA, alpha=0.7)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel(x, fontsize=12)
    plt.ylabel(y, fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


# Função auxiliar para criar pasta de output
def criar_pasta_output(caminho: str = 'output'):
    """Cria pasta para salvar gráficos se não existir"""
    pasta = Path(caminho)
    pasta.mkdir(exist_ok=True, parents=True)
    return pasta

def grafico_barras_empilhadas(df: pd.DataFrame, titulo: str, 
                              figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de barras empilhadas
    
    Args:
        df: DataFrame com dados (índice = categorias, colunas = subcategorias)
        titulo: Título do gráfico
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    df.plot(kind='bar', stacked=True, color=PALETA_CATEGORICA, 
            edgecolor='white', linewidth=0.5, ax=plt.gca())
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel('Categorias', fontsize=12)
    plt.ylabel('Valores', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Legenda', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def grafico_scatter(df: pd.DataFrame, x: str, y: str, titulo: str,
                    hue: str = None, size: str = None, 
                    figsize: tuple = (12, 6), salvar: str = None):
    """
    Gera gráfico de dispersão (scatter plot)
    
    Args:
        df: DataFrame com os dados
        x: Nome da coluna para eixo X
        y: Nome da coluna para eixo Y
        titulo: Título do gráfico
        hue: Coluna para colorir os pontos (opcional)
        size: Coluna para variar o tamanho dos pontos (opcional)
        figsize: Tamanho da figura
        salvar: Caminho para salvar o gráfico (opcional)
    """
    plt.figure(figsize=figsize)
    sns.scatterplot(data=df, x=x, y=y, hue=hue, size=size, 
                    palette=PALETA_CATEGORICA, alpha=0.7)
    plt.title(titulo, fontsize=14, fontweight='bold', pad=20)
    plt.xlabel(x, fontsize=12)
    plt.ylabel(y, fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    if salvar:
        plt.savefig(salvar, dpi=300, bbox_inches='tight')
        print(f"✅ Gráfico salvo em: {salvar}")
    
    plt.show()


def criar_pasta_output(caminho: str = 'output'):
    """Cria pasta para salvar gráficos se não existir"""
    pasta = Path(caminho)
    pasta.mkdir(exist_ok=True, parents=True)
    return pasta

if __name__ == "__main__":
    print("Módulo de visualizações carregado com sucesso!")
    print(f"Funções disponíveis:")
    print("  - grafico_barras")
    print("  - grafico_linha")
    print("  - grafico_pizza")
    print("  - grafico_boxplot")
    print("  - grafico_heatmap")
    print("  - grafico_distribuicao")
    print("  - grafico_comparativo_barras")
    print("  - grafico_barras_empilhadas")
    print("  - grafico_scatter")

    