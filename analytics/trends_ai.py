
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional

def predict_future_trends(df_historico: pd.DataFrame, metric_type: str, months: int) -> Optional[Dict[str, Any]]:
    """Prediz tendências futuras usando análise temporal simulada"""
    
    if df_historico is None or df_historico.empty:
        return None
    
    # Simulação de previsão (substituir por modelo real de ML)
    last_value = df_historico['TotalValor'].iloc[-1]
    growth_rate = 0.05  # 5% ao ano
    
    predictions = []
    for i in range(1, months + 1):
        # Simulação com sazonalidade
        seasonal_factor = 1 + 0.1 * np.sin(2 * np.pi * i / 12)  # Padrão sazonal
        predicted_value = last_value * (1 + growth_rate * i / 12) * seasonal_factor
        predictions.append(predicted_value)
    
    return {
        "predictions": predictions,
        "confidence": 0.85,  # 85% de confiança
        "trend": "crescimento" if growth_rate > 0 else "declínio",
        "seasonal_pattern": True
    }

def analyze_growth_patterns(df_historico: pd.DataFrame, metric_type: str) -> List[str]:
    """Analisa padrões de crescimento nos dados históricos"""
    
    patterns = []
    
    if df_historico is not None and not df_historico.empty:
        # Calcular taxa de crescimento
        df_historico = df_historico.sort_values('ANO')
        growth_rates = df_historico['TotalValor'].pct_change().dropna()
        
        # Padrões identificados
        avg_growth = growth_rates.mean() * 100
        patterns.append(f"Taxa de crescimento média anual: {avg_growth:.1f}%")
        
        # Volatilidade
        volatility = growth_rates.std() * 100
        patterns.append(f"Volatilidade do crescimento: {volatility:.1f}%")
        
        # Tendência geral
        if avg_growth > 5:
            patterns.append("Tendência de crescimento forte identificada")
        elif avg_growth > 0:
            patterns.append("Tendência de crescimento moderado identificada")
        else:
            patterns.append("Tendência de declínio identificada")
        
        # Anomalias (ex: COVID-19)
        if any(growth_rates < -0.5):  # Queda > 50%
            patterns.append("Anomalia significativa detectada no período (possivelmente COVID-19)")
        
        # Recuperação
        if len(growth_rates) >= 2 and growth_rates.iloc[-1] > 0.1:
            patterns.append("Sinais de recuperação nos dados mais recentes")
    
    return patterns

def detect_anomalies(df_historico: pd.DataFrame, metric_type: str) -> List[Dict[str, Any]]:
    """Detecta anomalias nos dados históricos"""
    
    anomalies = []
    
    if df_historico is not None and not df_historico.empty:
        # Calcular Z-score para detectar outliers
        values = df_historico['TotalValor']
        z_scores = np.abs((values - values.mean()) / values.std())
        
        # Identificar anomalias (Z-score > 2)
        anomaly_indices = np.where(z_scores > 2)[0]
        
        for idx in anomaly_indices:
            year = df_historico.iloc[idx]['ANO']
            value = df_historico.iloc[idx]['TotalValor']
            z_score = z_scores.iloc[idx]
            
            anomalies.append({
                "year": int(year),
                "value": value,
                "z_score": z_score,
                "type": "outlier_baixo" if value < values.mean() else "outlier_alto"
            })
    
    return anomalies

def generate_trend_insights(df_historico: pd.DataFrame) -> List[str]:
    """Gera insights sobre tendências identificadas"""
    
    insights = []
    
    if df_historico is not None and not df_historico.empty:
        # Análise de crescimento
        total_growth = ((df_historico['TotalValor'].iloc[-1] - df_historico['TotalValor'].iloc[0]) / 
                       df_historico['TotalValor'].iloc[0]) * 100
        
        insights.append(f"Crescimento total no período: {total_growth:.1f}%")
        
        # Melhor e pior ano
        best_year = df_historico.loc[df_historico['TotalValor'].idxmax(), 'ANO']
        worst_year = df_historico.loc[df_historico['TotalValor'].idxmin(), 'ANO']
        
        insights.append(f"Melhor performance em {int(best_year)}")
        insights.append(f"Menor performance em {int(worst_year)}")
        
        # Tendência recente
        recent_trend = df_historico['TotalValor'].tail(3).pct_change().mean()
        if recent_trend > 0.05:
            insights.append("Tendência recente: Crescimento acelerado")
        elif recent_trend > 0:
            insights.append("Tendência recente: Crescimento moderado")
        else:
            insights.append("Tendência recente: Estabilização ou declínio")
    
    return insights
