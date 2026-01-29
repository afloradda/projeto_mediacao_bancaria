"""
Configurações do projeto de Mediação Bancária - Agibank
"""

# ==========================================
# FONTES DE DADOS
# ==========================================

DATA_SOURCES = {
    'consumidor_gov': {
        'name': 'Consumidor.gov.br',
        'base_url': 'https://dados.mj.gov.br/dataset/',
        'file_pattern': '*.csv',
        'encoding': 'utf-8',
        'separator': ';'
    },
    'sindec': {
        'name': 'SINDEC',
        'base_url': 'https://dados.mj.gov.br/dataset/', 
        'file_pattern': '*.csv',
        'encoding': 'utf-8'
    }
}

# ==========================================
# VALIDAÇÕES DE QUALIDADE
# ==========================================

QUALITY_CHECKS = {
    'max_null_percentage': 0.3,  # Máximo 30% de nulos por coluna
    'min_rows_expected': 100,    # Mínimo de linhas esperadas
    'date_range_validation': True
}

# ==========================================
# CONFIGURAÇÕES DE PROCESSAMENTO
# ==========================================

PROCESSING_CONFIG = {
    'target_year': 2025,
    'date_format': '%Y-%m-%d',
    'chunk_size': 10000
}

# ==========================================
# FILTROS ESPECÍFICOS AGIBANK
# ==========================================

AGIBANK_FILTERS = {
    'bank_names': [
        'Banco Agibank',
        'Banco Agibank (Agiplan)',
        'AGIBANK',
        'AGI BANK', 
        'BANCO AGIBANK',
        'AGIBANK S.A.'
    ]
}