
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

from analytics.insights_ai import generate_automated_insights, generate_market_insights, generate_seasonal_insights
from chatbot_logic import (
    obter_aeroporto_mais_movimentado,
    obter_operador_mais_passageiros,
    obter_top_10_aeroportos,
    calcular_market_share,
    formatar_numero_br,
    aeroporto_nome_para_icao,
    operador_icao_para_nome
)

def render(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.title("<i class='fas fa-chart-bar'></i> Insights Automáticos com IA", unsafe_allow_html=True)
    st.markdown("---")
    
    # Controles
    col1, col2, col3 = st.columns(3)
    
    with col1:
        analysis_type = st.selectbox(
            "Tipo de Análise",
            ["Análise Geral", "Análise de Mercado", "Análise Sazonal", "Análise Comparativa"]
        )
    
    with col2:
        year_range = st.selectbox(
            "Período",
            [f"{ultimo_ano}", f"{ultimo_ano-1}-{ultimo_ano}", f"{ultimo_ano-2}-{ultimo_ano}"]
        )
    
    with col3:
        if st.button("<i class='fas fa-sync-alt'></i> Gerar Insights", type="primary"):
            st.session_state.refresh_insights = True
    
    st.markdown("---")
    
    # Gerar insights baseado no tipo selecionado
    if analysis_type == "Análise Geral":
        render_general_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analysis_type == "Análise de Mercado":
        render_market_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analysis_type == "Análise Sazonal":
        render_seasonal_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
    elif analysis_type == "Análise Comparativa":
        render_comparative_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano)

def render_general_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.subheader("<i class='fas fa-bullseye'></i> Insights Principais", unsafe_allow_html=True)
    
    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    
    # Aeroporto mais movimentado
    resultado_movimentado = obter_aeroporto_mais_movimentado(PASTA_ARQUIVOS_PARQUET, ano=ultimo_ano)
    if resultado_movimentado:
        aeroporto_nome = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() 
                              if icao == resultado_movimentado['aeroporto'].upper()), 
                             resultado_movimentado['aeroporto'])
        
        with col1:
            st.metric(
                "Aeroporto Líder",
                aeroporto_nome,
                f"{formatar_numero_br(resultado_movimentado['total_passageiros'])} passageiros"
            )
    
    # Operador líder
    resultado_operador = obter_operador_mais_passageiros(PASTA_ARQUIVOS_PARQUET, ano=ultimo_ano)
    if resultado_operador:
        operador_nome = operador_icao_para_nome.get(resultado_operador['operador'].upper(), 
                                                   resultado_operador['operador'])
        
        with col2:
            st.metric(
                "Operador Líder",
                operador_nome,
                f"{formatar_numero_br(resultado_operador['total_passageiros'])} passageiros"
            )
    
    # Total de aeroportos ativos
    top_aeroportos = obter_top_10_aeroportos(PASTA_ARQUIVOS_PARQUET, ano=ultimo_ano)
    with col3:
        st.metric(
            "Aeroportos Ativos",
            len(top_aeroportos),
            f"Top 10 principais"
        )
    
    with col4:
        st.metric(
            "Período Analisado",
            f"2019-{ultimo_ano}",
            f"{ultimo_ano - 2019 + 1} anos"
        )
    
    st.markdown("---")
    
    # Insights gerados por IA
    with st.container():
        st.subheader("<i class='fas fa-robot'></i> Análise Inteligente", unsafe_allow_html=True)
        
        if st.button("Gerar Insights com IA", key="generate_ai_insights"):
            with st.spinner("Analisando dados e gerando insights..."):
                insights = generate_automated_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano)
                
                if insights:
                    st.success("✅ Análise concluída!")
                    
                    # Exibir insights em cards
                    for i, insight in enumerate(insights):
                        with st.expander(f"💡 Insight {i+1}: {insight.get('title', 'Análise')}", expanded=True):
                            st.markdown(insight.get('content', ''))
                            
                            if 'data' in insight:
                                st.plotly_chart(insight['data'], use_container_width=True)
                else:
                    st.error("Não foi possível gerar insights no momento.")

def render_market_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.subheader("<i class='fas fa-chart-line'></i> Análise de Mercado", unsafe_allow_html=True)
    
    # Market share geral
    market_data = calcular_market_share(PASTA_ARQUIVOS_PARQUET, ano=ultimo_ano)
    
    if market_data and market_data['data']:
        # Gráfico de pizza para market share
        df_market = pd.DataFrame(market_data['data'])
        
        fig = px.pie(
            df_market,
            values='PaxShare',
            names='NR_AERONAVE_OPERADOR',
            title=f"Participação de Mercado - {ultimo_ano}",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=500)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Insights de mercado com IA
        with st.expander("🎯 Insights de Mercado", expanded=True):
            if st.button("Analisar Tendências de Mercado", key="market_analysis"):
                with st.spinner("Gerando análise de mercado..."):
                    market_insights = generate_market_insights(market_data, ultimo_ano)
                    
                    if market_insights:
                        for insight in market_insights:
                            st.markdown(f"• {insight}")
                    else:
                        st.info("Análise de mercado não disponível no momento.")

def render_seasonal_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.subheader("<i class='fas fa-calendar-alt'></i> Análise Sazonal", unsafe_allow_html=True)
    
    # Aqui você implementaria análise sazonal
    st.info("🚧 Análise sazonal em desenvolvimento. Em breve com insights sobre:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Sazonalidade de Passageiros:**
        - Picos de movimento por época
        - Tendências mensais
        - Impacto de feriados
        - Variações regionais
        """)
    
    with col2:
        st.markdown("""
        **Padrões Operacionais:**
        - Horários de maior movimento
        - Dias da semana mais movimentados
        - Variações de carga
        - Eficiência operacional
        """)
    
    # Simulação de dados sazonais
    if st.button("Simular Análise Sazonal", key="seasonal_demo"):
        months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 
                 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
        
        # Dados simulados para demonstração
        passengers = [850000, 780000, 920000, 1100000, 950000, 1200000,
                     1400000, 1300000, 1000000, 950000, 900000, 1100000]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=months,
            y=passengers,
            mode='lines+markers',
            name='Passageiros',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title="Padrão Sazonal de Passageiros (Simulado)",
            xaxis_title="Mês",
            yaxis_title="Número de Passageiros",
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)

def render_comparative_insights(PASTA_ARQUIVOS_PARQUET, ultimo_ano):
    st.subheader("<i class='fas fa-balance-scale'></i> Análise Comparativa", unsafe_allow_html=True)
    
    # Comparação ano a ano
    anos_comparacao = [ultimo_ano-1, ultimo_ano]
    
    dados_comparacao = []
    for ano in anos_comparacao:
        resultado = obter_aeroporto_mais_movimentado(PASTA_ARQUIVOS_PARQUET, ano=ano)
        if resultado:
            dados_comparacao.append({
                'Ano': ano,
                'Aeroporto': resultado['aeroporto'],
                'Passageiros': resultado['total_passageiros']
            })
    
    if dados_comparacao:
        df_comp = pd.DataFrame(dados_comparacao)
        
        # Gráfico de barras comparativo
        fig = px.bar(
            df_comp,
            x='Ano',
            y='Passageiros',
            title="Comparação Anual - Aeroporto Mais Movimentado",
            color='Ano',
            text='Passageiros'
        )
        
        fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Calcular crescimento
        if len(dados_comparacao) >= 2:
            crescimento = ((dados_comparacao[1]['Passageiros'] - dados_comparacao[0]['Passageiros']) / 
                          dados_comparacao[0]['Passageiros']) * 100
            
            st.metric(
                "Crescimento Anual",
                f"{crescimento:.1f}%",
                f"{formatar_numero_br(abs(dados_comparacao[1]['Passageiros'] - dados_comparacao[0]['Passageiros']))} passageiros"
            )
