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
# DELETE COLUMNS
# ==========================================

CONSUMIDOR_GOV_DELETE_COLUMNS = {
    'columns': [
        'Data Análise',
        'Data Recusa',
        'Prazo Analise Gestor',
        'Análise da Recusa', 
        'Interação com Judiciario', 
        'Último Complemento Consumidor', 
        'Canal de Origem', 
        'Gestor'
    ]
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

# ==========================================
# COLUNAS TEMPORAIS
# ==========================================

TEMPORAL_COLUMNS_CONFIG = {
    'datetime_columns': [
        'data_abertura',
        'data_resposta', 
        'data_finalizacao',
        'prazo_resposta',
        'processed_at'
    ],
    'default_format': '%d/%m/%Y',  # Formato padrão brasileiro
    'processed_at_format': '%Y-%m-%d %H:%M:%S.%f'
}

# ==========================================
# CONFIGURAÇÕES GOLD - RECORTES SP
# ==========================================


SP_CITIES_CONFIG = {
    'max_cities': 645,                    
    'validate_suspicious': True,
    'min_records_per_city': 5,           
    'show_low_frequency': True
}

AGE_GROUPS_CONFIG = {
    'target_groups': ['entre 31 a 40 anos', 'entre 41 a 50 anos', 'entre 21 a 30 anos']
}

BUSINESS_SECTORS_CONFIG = {
    'banking_keywords': ['banco', 'financeira', 'administradora', 'cartão'],
    'focus_problems': ['cobrança', 'atendimento', 'produto', 'serviço']
}