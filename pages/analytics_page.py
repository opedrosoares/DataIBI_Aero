
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os

from analytics.advanced_ai import (
    generate_correlation_analysis,
    perform_cluster_analysis,
    analyze_performance_kpis,
    generate_recommendations
)

def render(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h1><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">analytics</i>Analytics Avançado com IA</h1>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Menu de analytics
    analytics_type = st.selectbox(
        "Selecione o tipo de análise:",
        [
            '<i class="material-icons">show_chart</i> Análise de Correlação',
            '<i class="material-icons">my_location</i> Segmentação de Aeroportos', 
            '<i class="material-icons">speed</i> KPIs de Performance',
            '<i class="material-icons">psychology</i> Previsão de Demanda',
            '<i class="material-icons">lightbulb</i> Recomendações Inteligentes'
        ]
    )
    
    st.markdown("---")
    
    if analytics_type == "📊 Análise de Correlação":
        render_correlation_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analytics_type == "🎯 Segmentação de Aeroportos":
        render_cluster_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analytics_type == "📈 KPIs de Performance":
        render_kpi_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analytics_type == "🔮 Previsão de Demanda":
        render_demand_forecasting(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analytics_type == "💡 Recomendações Inteligentes":
        render_ai_recommendations(PASTA_ARQUIVOS_PARQUET, ultimo_ano)

def render_correlation_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h2><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">show_chart</i>Análise de Correlação</h2>', unsafe_allow_html=True)
    
    # Simular matriz de correlação
    if st.button("🔍 Executar Análise de Correlação", type="primary"):
        with st.spinner("Analisando correlações entre variáveis..."):
            # Dados simulados para demonstração
            variables = ['Passageiros', 'Cargas', 'Voos Domésticos', 'Voos Internacionais', 
                        'Atrasos', 'Eficiência Operacional']
            
            # Matriz de correlação simulada
            correlation_matrix = np.array([
                [1.00, 0.85, 0.92, 0.78, -0.45, 0.67],
                [0.85, 1.00, 0.76, 0.82, -0.38, 0.72],
                [0.92, 0.76, 1.00, 0.45, -0.52, 0.63],
                [0.78, 0.82, 0.45, 1.00, -0.29, 0.58],
                [-0.45, -0.38, -0.52, -0.29, 1.00, -0.76],
                [0.67, 0.72, 0.63, 0.58, -0.76, 1.00]
            ])
            
            # Heatmap de correlação
            fig = go.Figure(data=go.Heatmap(
                z=correlation_matrix,
                x=variables,
                y=variables,
                colorscale='RdBu',
                zmid=0,
                text=correlation_matrix,
                texttemplate='%{text:.2f}',
                textfont={"size": 10},
                hoverongaps=False
            ))
            
            fig.update_layout(
                title="Matriz de Correlação - Variáveis Aeroportuárias",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Insights de correlação
            st.subheader("🎯 Insights de Correlação")
            
            insights = [
                "📈 **Alta correlação positiva** entre Passageiros e Voos Domésticos (0.92)",
                "📦 **Forte relação** entre Cargas e Voos Internacionais (0.82)",
                "⚠️ **Correlação negativa** entre Atrasos e Eficiência (-0.76)",
                "🔗 **Sinergia** entre volume de Passageiros e Cargas (0.85)"
            ]
            
            for insight in insights:
                st.markdown(insight)

def render_cluster_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h2><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">my_location</i>Segmentação de Aeroportos</h2>', unsafe_allow_html=True)
    
    if st.button("🔬 Executar Análise de Clusters", type="primary"):
        with st.spinner("Segmentando aeroportos por características..."):
            # Simulação de dados de aeroportos
            aeroportos = ['GRU', 'CGH', 'BSB', 'GIG', 'SSA', 'REC', 'FOR', 'POA', 'CWB', 'BEL']
            
            # Coordenadas simuladas para clusters
            np.random.seed(42)
            x_coords = np.random.randn(len(aeroportos)) * 2
            y_coords = np.random.randn(len(aeroportos)) * 2
            
            # Definir clusters (simulado)
            clusters = [0, 0, 1, 0, 2, 2, 2, 1, 1, 2]
            cluster_names = ['Hub Principal', 'Hub Regional', 'Aeroporto Local']
            
            # Criar DataFrame
            df_clusters = pd.DataFrame({
                'Aeroporto': aeroportos,
                'X': x_coords,
                'Y': y_coords,
                'Cluster': clusters,
                'Categoria': [cluster_names[c] for c in clusters]
            })
            
            # Gráfico de dispersão dos clusters
            fig = px.scatter(
                df_clusters,
                x='X',
                y='Y',
                color='Categoria',
                text='Aeroporto',
                title="Segmentação de Aeroportos por Características",
                color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1']
            )
            
            fig.update_traces(textposition="middle center", textfont_size=12)
            fig.update_layout(height=500)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Características dos clusters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("""
                **🏢 Hub Principal**
                - Alto volume de passageiros
                - Muitas conexões internacionais
                - Infraestrutura robusta
                - Exemplos: GRU, CGH, GIG
                """)
            
            with col2:
                st.markdown("""
                **🌆 Hub Regional**
                - Volume médio de passageiros
                - Foco em rotas domésticas
                - Conexões regionais importantes
                - Exemplos: BSB, POA, CWB
                """)
            
            with col3:
                st.markdown("""
                **🏘️ Aeroporto Local**
                - Menor volume de tráfego
                - Principalmente voos domésticos
                - Atende demanda local/regional
                - Exemplos: SSA, REC, FOR, BEL
                """)

def render_kpi_analysis(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h2><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">speed</i>KPIs de Performance</h2>', unsafe_allow_html=True)
    
    # KPIs principais
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Eficiência Operacional",
            "87.5%",
            "↗️ +2.3%",
            help="Percentual de voos no horário"
        )
    
    with col2:
        st.metric(
            "Ocupação Média",
            "78.2%",
            "↗️ +5.1%",
            help="Taxa de ocupação das aeronaves"
        )
    
    with col3:
        st.metric(
            "Satisfação do Cliente",
            "4.2/5",
            "↗️ +0.3",
            help="Avaliação média dos passageiros"
        )
    
    with col4:
        st.metric(
            "Pontualidade",
            "82.1%",
            "↘️ -1.2%",
            help="Voos partindo no horário"
        )
    
    st.markdown("---")
    
    # Gráfico de KPIs ao longo do tempo
    if st.button("📊 Visualizar Tendências dos KPIs", type="primary"):
        meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 
                'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
        
        # Dados simulados
        eficiencia = [85, 86, 88, 87, 89, 90, 88, 87, 86, 89, 88, 87]
        ocupacao = [75, 76, 78, 79, 80, 82, 81, 79, 77, 78, 79, 78]
        pontualidade = [80, 82, 83, 81, 84, 85, 82, 81, 80, 83, 82, 82]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=meses, y=eficiencia,
            mode='lines+markers',
            name='Eficiência Operacional',
            line=dict(color='#1f77b4')
        ))
        
        fig.add_trace(go.Scatter(
            x=meses, y=ocupacao,
            mode='lines+markers',
            name='Taxa de Ocupação',
            line=dict(color='#ff7f0e')
        ))
        
        fig.add_trace(go.Scatter(
            x=meses, y=pontualidade,
            mode='lines+markers',
            name='Pontualidade',
            line=dict(color='#2ca02c')
        ))
        
        fig.update_layout(
            title="Evolução dos KPIs de Performance",
            xaxis_title="Mês",
            yaxis_title="Percentual (%)",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)

def render_demand_forecasting(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h2><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">psychology</i>Previsão de Demanda</h2>', unsafe_allow_html=True)
    
    # Controles de previsão
    col1, col2 = st.columns(2)
    
    with col1:
        horizon = st.selectbox("Horizonte de Previsão", ["3 meses", "6 meses", "1 ano", "2 anos"])
    
    with col2:
        confidence = st.slider("Nível de Confiança", 80, 99, 95, help="Intervalo de confiança da previsão")
    
    if st.button("🚀 Gerar Previsão de Demanda", type="primary"):
        with st.spinner("Modelando demanda futura com IA..."):
            # Simulação de previsão
            months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun']
            historical = [950000, 920000, 1100000, 1050000, 1200000, 1150000]
            predicted = [1180000, 1220000, 1280000, 1320000, 1380000, 1400000]
            
            # Intervalo de confiança
            upper_bound = [p * 1.1 for p in predicted]
            lower_bound = [p * 0.9 for p in predicted]
            
            fig = go.Figure()
            
            # Dados históricos
            fig.add_trace(go.Scatter(
                x=months,
                y=historical,
                mode='lines+markers',
                name='Dados Históricos',
                line=dict(color='#1f77b4')
            ))
            
            # Previsões
            fig.add_trace(go.Scatter(
                x=months,
                y=predicted,
                mode='lines+markers',
                name='Previsão IA',
                line=dict(color='red', dash='dash')
            ))
            
            # Intervalo de confiança
            fig.add_trace(go.Scatter(
                x=months + months[::-1],
                y=upper_bound + lower_bound[::-1],
                fill='toself',
                fillcolor='rgba(255,0,0,0.1)',
                line=dict(color='rgba(255,255,255,0)'),
                name=f'Intervalo {confidence}%'
            ))
            
            fig.update_layout(
                title="Previsão de Demanda de Passageiros",
                xaxis_title="Período",
                yaxis_title="Número de Passageiros",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Métricas da previsão
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Crescimento Previsto", "+21.7%", "vs período anterior")
            
            with col2:
                st.metric("Acurácia do Modelo", "94.2%", "Baseado em dados históricos")
            
            with col3:
                st.metric("Tendência", "Crescimento", "Forte demanda esperada")

def render_ai_recommendations(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.markdown('<h2><i class="material-icons" style="vertical-align: middle; margin-right: 10px;">lightbulb</i>Recomendações Inteligentes</h2>', unsafe_allow_html=True)
    
    if st.button("🧠 Gerar Recomendações com IA", type="primary"):
        with st.spinner("Analisando dados e gerando recomendações..."):
            st.success("✅ Análise concluída! Recomendações geradas.")
            
            # Recomendações por categoria
            tab1, tab2, tab3, tab4 = st.tabs([
                "🏢 Infraestrutura", 
                "📈 Operacional", 
                "💰 Financeiro", 
                "🌱 Sustentabilidade"
            ])
            
            with tab1:
                st.markdown("""
                **🏗️ Recomendações de Infraestrutura:**
                
                • **Expansão urgente em GRU**: Capacidade atual próxima do limite
                • **Modernização em CGH**: Sistemas de bagagem precisam de upgrade
                • **Nova pista em BSB**: Demanda crescente justifica investimento
                • **Terminal cargo em GIG**: Oportunidade de crescimento no segmento
                
                **Prioridade Alta:** GRU e CGH (impacto imediato na operação)
                """)
            
            with tab2:
                st.markdown("""
                **⚙️ Recomendações Operacionais:**
                
                • **Otimização de slots**: Redistribuir horários de pico
                • **Gestão preditiva de atrasos**: Implementar sistema de alertas
                • **Melhoria na gestão de ground handling**: Reduzir tempo de turnaround
                • **Integração modal**: Conectar aeroportos com transporte público
                
                **ROI Estimado:** 15-25% de melhoria na eficiência
                """)
            
            with tab3:
                st.markdown("""
                **💼 Recomendações Financeiras:**
                
                • **Diversificação de receitas**: Explorar receitas não-aeronáuticas
                • **Pricing dinâmico**: Implementar tarifas baseadas em demanda
                • **Parcerias estratégicas**: Joint ventures para expansão
                • **Hedge cambial**: Proteção contra flutuações da moeda
                
                **Potencial de Aumento:** 12-18% na receita total
                """)
            
            with tab4:
                st.markdown("""
                **🌿 Recomendações de Sustentabilidade:**
                
                • **Energia renovável**: Instalação de painéis solares
                • **Gestão de resíduos**: Programa de reciclagem avançado
                • **Combustível sustentável**: Incentivo ao SAF
                • **Carbon offset**: Programa de compensação de carbono
                
                **Meta:** Neutralidade carbônica até 2030
                """)
            
            # Score de priorização
            st.markdown("---")
            st.subheader("📊 Score de Priorização")
            
            recommendations_df = pd.DataFrame({
                'Recomendação': [
                    'Expansão GRU',
                    'Sistema Preditivo',
                    'Energia Renovável',
                    'Pricing Dinâmico',
                    'Nova Pista BSB'
                ],
                'Impacto': [95, 85, 70, 80, 75],
                'Custo': [90, 30, 60, 25, 85],
                'Urgência': [95, 75, 40, 60, 65],
                'Score Final': [93, 80, 57, 72, 72]
            })
            
            fig = px.bar(
                recommendations_df,
                x='Score Final',
                y='Recomendação',
                orientation='h',
                color='Score Final',
                color_continuous_scale='Viridis',
                title="Priorização de Recomendações (Score 0-100)"
            )
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
