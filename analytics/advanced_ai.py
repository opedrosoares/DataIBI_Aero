
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional

def generate_correlation_analysis(pasta_parquet: str, ultimo_ano: int) -> Dict[str, Any]:
    """Gera análise de correlação entre variáveis aeroportuárias"""
    
    # Simulação de análise de correlação
    variables = ['Passageiros', 'Cargas', 'Voos_Domesticos', 'Voos_Internacionais', 'Atrasos']
    
    # Matriz de correlação simulada
    correlation_matrix = np.array([
        [1.00, 0.85, 0.92, 0.78, -0.45],
        [0.85, 1.00, 0.76, 0.82, -0.38],
        [0.92, 0.76, 1.00, 0.45, -0.52],
        [0.78, 0.82, 0.45, 1.00, -0.29],
        [-0.45, -0.38, -0.52, -0.29, 1.00]
    ])
    
    # Insights das correlações
    insights = []
    
    # Correlações mais fortes
    max_corr = np.max(correlation_matrix[np.triu_indices_from(correlation_matrix, k=1)])
    insights.append(f"Correlação mais forte: {max_corr:.2f}")
    
    # Correlações negativas
    negative_corrs = correlation_matrix[correlation_matrix < 0]
    insights.append(f"Identificadas {len(negative_corrs)} correlações negativas")
    
    return {
        "matrix": correlation_matrix,
        "variables": variables,
        "insights": insights
    }

def perform_cluster_analysis(pasta_parquet: str, ultimo_ano: int) -> Dict[str, Any]:
    """Realiza análise de clusters de aeroportos"""
    
    # Simulação de clustering
    aeroportos = ['GRU', 'CGH', 'BSB', 'GIG', 'SSA', 'REC', 'FOR', 'POA', 'CWB', 'BEL']
    
    # Características simuladas
    caracteristicas = {
        'volume_passageiros': np.random.uniform(100000, 2000000, len(aeroportos)),
        'voos_internacionais': np.random.uniform(0, 500, len(aeroportos)),
        'area_influencia': np.random.uniform(1, 10, len(aeroportos))
    }
    
    # Clusters simulados (3 grupos)
    clusters = [0, 0, 1, 0, 2, 2, 2, 1, 1, 2]  # Hub Principal, Regional, Local
    
    cluster_info = {
        0: {"nome": "Hub Principal", "descricao": "Alto volume, muitas conexões internacionais"},
        1: {"nome": "Hub Regional", "descricao": "Volume médio, foco em conexões domésticas"},
        2: {"nome": "Aeroporto Local", "descricao": "Menor volume, atende demanda local"}
    }
    
    return {
        "aeroportos": aeroportos,
        "clusters": clusters,
        "cluster_info": cluster_info,
        "caracteristicas": caracteristicas
    }

def analyze_performance_kpis(pasta_parquet: str, ultimo_ano: int) -> Dict[str, Any]:
    """Analisa KPIs de performance dos aeroportos"""
    
    # KPIs simulados
    kpis = {
        "eficiencia_operacional": {
            "valor_atual": 87.5,
            "meta": 90.0,
            "tendencia": "crescimento",
            "variacao": 2.3
        },
        "pontualidade": {
            "valor_atual": 82.1,
            "meta": 85.0,
            "tendencia": "declinio",
            "variacao": -1.2
        },
        "satisfacao_cliente": {
            "valor_atual": 4.2,
            "meta": 4.5,
            "tendencia": "crescimento",
            "variacao": 0.3
        },
        "ocupacao_media": {
            "valor_atual": 78.2,
            "meta": 80.0,
            "tendencia": "crescimento",
            "variacao": 5.1
        }
    }
    
    # Análise de gaps
    gaps = {}
    for kpi, dados in kpis.items():
        gap = dados["meta"] - dados["valor_atual"]
        gaps[kpi] = {
            "gap": gap,
            "percentual_meta": (dados["valor_atual"] / dados["meta"]) * 100
        }
    
    return {
        "kpis": kpis,
        "gaps": gaps,
        "score_geral": 85.2  # Score médio ponderado
    }

def generate_recommendations(pasta_parquet: str, ultimo_ano: int) -> List[Dict[str, Any]]:
    """Gera recomendações inteligentes baseadas em análise de dados"""
    
    recommendations = [
        {
            "categoria": "Infraestrutura",
            "prioridade": "Alta",
            "recomendacao": "Expansão da capacidade em GRU",
            "impacto": "Alto",
            "custo_estimado": "R$ 2.5 bilhões",
            "prazo": "24 meses",
            "roi_esperado": "18%"
        },
        {
            "categoria": "Operacional",
            "prioridade": "Média",
            "recomendacao": "Implementação de sistema preditivo de atrasos",
            "impacto": "Médio",
            "custo_estimado": "R$ 50 milhões",
            "prazo": "12 meses",
            "roi_esperado": "25%"
        },
        {
            "categoria": "Tecnologia",
            "prioridade": "Alta",
            "recomendacao": "Modernização dos sistemas de bagagem",
            "impacto": "Alto",
            "custo_estimado": "R$ 300 milhões",
            "prazo": "18 meses",
            "roi_esperado": "22%"
        },
        {
            "categoria": "Sustentabilidade",
            "prioridade": "Média",
            "recomendacao": "Instalação de painéis solares",
            "impacto": "Médio",
            "custo_estimado": "R$ 150 milhões",
            "prazo": "15 meses",
            "roi_esperado": "12%"
        }
    ]
    
    return recommendations

def calculate_investment_priority(recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Calcula score de priorização para investimentos"""
    
    for rec in recommendations:
        # Simulação de score baseado em impacto, custo e ROI
        impacto_score = {"Alto": 90, "Médio": 60, "Baixo": 30}[rec["impacto"]]
        prioridade_score = {"Alta": 90, "Média": 60, "Baixa": 30}[rec["prioridade"]]
        roi_score = float(rec["roi_esperado"].replace("%", "")) * 3
        
        score_final = (impacto_score + prioridade_score + roi_score) / 3
        rec["score_priorizacao"] = round(score_final, 1)
    
    # Ordenar por score
    recommendations.sort(key=lambda x: x["score_priorizacao"], reverse=True)
    
    return recommendations
