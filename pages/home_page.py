
import streamlit as st
import base64
import os

def get_image_as_base64(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

def render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH):
    # CSS customizado para os cards
    st.markdown("""
    <style>
    .card {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border: 1px solid #e9ecef;
        height: 300px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        cursor: pointer;
        text-decoration: none;
        color: inherit;
    }
    
    .card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.15);
    }
    
    .card-icon {
        width: 80px;
        height: 80px;
        margin: 0 auto 1rem;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    .card-title {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .card-subtitle {
        font-size: 1rem;
        color: #6c757d;
        text-align: center;
        line-height: 1.4;
    }
    
    .welcome-section {
        text-align: center;
        margin-bottom: 3rem;
    }
    
    .welcome-title {
        font-size: 2.5rem;
        color: #2c3e50;
        margin-bottom: 1rem;
    }
    
    .welcome-subtitle {
        font-size: 1.2rem;
        color: #6c757d;
        margin-bottom: 2rem;
    }
    
    .stats-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin: 2rem 0;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Seção de boas-vindas
    st.markdown("""
    <div class="welcome-section">
        <h1 class="welcome-title">🛩️ Observatório Aeroportuário</h1>
        <p class="welcome-subtitle">
            Análise inteligente de movimentações aeroportuárias brasileiras com tecnologia de IA
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Estatísticas principais
    st.markdown(f"""
    <div class="stats-container">
        <h3>📊 Dados Disponíveis</h3>
        <div style="display: flex; justify-content: space-around; margin-top: 1rem;">
            <div>
                <h2>2019-{ultimo_ano}</h2>
                <p>Período de Análise</p>
            </div>
            <div>
                <h2>70+</h2>
                <p>Aeroportos Monitorados</p>
            </div>
            <div>
                <h2>24/7</h2>
                <p>Análise em Tempo Real</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Cards das páginas
    st.markdown("### 🚀 Explore nossas funcionalidades")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Card Chatbot
        if st.button("", key="card_chat", help="Chatbot Inteligente"):
            st.session_state.current_page = 'chat'
            st.rerun()
            
        st.markdown("""
        <div class="card" onclick="document.querySelector('[data-testid=\"stButton\"][title=\"Chatbot Inteligente\"] button').click()">
            <div>
                <div class="card-icon">
                    🤖
                </div>
                <div class="card-title">Chatbot Inteligente</div>
                <div class="card-subtitle">
                    Converse naturalmente sobre dados aeroportuários. 
                    Faça perguntas e receba respostas detalhadas com gráficos e análises.
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Card Trends
        if st.button("", key="card_trends", help="Análise de Tendências"):
            st.session_state.current_page = 'trends'
            st.rerun()
            
        st.markdown("""
        <div class="card" onclick="document.querySelector('[data-testid=\"stButton\"][title=\"Análise de Tendências\"] button').click()">
            <div>
                <div class="card-icon">
                    📈
                </div>
                <div class="card-title">Análise de Tendências</div>
                <div class="card-subtitle">
                    Descubra padrões e tendências históricas. 
                    Previsões com IA e detecção automática de anomalias.
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Card Insights
        if st.button("", key="card_insights", help="Insights Automáticos"):
            st.session_state.current_page = 'insights'
            st.rerun()
            
        st.markdown("""
        <div class="card" onclick="document.querySelector('[data-testid=\"stButton\"][title=\"Insights Automáticos\"] button').click()">
            <div>
                <div class="card-icon">
                    📊
                </div>
                <div class="card-title">Insights Automáticos</div>
                <div class="card-subtitle">
                    Relatórios inteligentes gerados automaticamente. 
                    Análises de mercado, sazonalidade e comparativas.
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Card Analytics
        if st.button("", key="card_analytics", help="Analytics Avançado"):
            st.session_state.current_page = 'analytics'
            st.rerun()
            
        st.markdown("""
        <div class="card" onclick="document.querySelector('[data-testid=\"stButton\"][title=\"Analytics Avançado\"] button').click()">
            <div>
                <div class="card-icon">
                    ⚡
                </div>
                <div class="card-title">Analytics Avançado</div>
                <div class="card-subtitle">
                    Análises estatísticas complexas, correlações, 
                    clustering e recomendações inteligentes com IA.
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Seção de recursos
    st.markdown("---")
    st.markdown("### ✨ Recursos Principais")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **🎯 Análise Inteligente**
        - Processamento de linguagem natural
        - Respostas contextualizadas
        - Gráficos interativos
        """)
    
    with col2:
        st.markdown("""
        **📈 Previsões com IA**
        - Modelos de machine learning
        - Detecção de anomalias
        - Projeções futuras
        """)
    
    with col3:
        st.markdown("""
        **📊 Visualizações Avançadas**
        - Dashboards interativos
        - Relatórios automáticos
        - Exportação de dados
        """)
