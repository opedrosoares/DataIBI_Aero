import streamlit as st
import base64
import os


def get_image_as_base64(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

def render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, ICON_PATH):
    
    # Define LOGO_WATERMARK_PATH for use in chart generation
    APP_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Display logo at the top
    if LOGO_PATH and os.path.exists(LOGO_PATH):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(LOGO_PATH, width=300)
        
    # CSS customizado para os cards
    st.markdown("""
    <style>
    .card {
        background: white;
        padding: 2rem;
        border-radius: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border: 1px solid #e9ecef;
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
        background: linear-gradient(135deg, #85bec0 0%, #5996c9 100%);
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
        background: linear-gradient(135deg, #85bec0 0%, #5996c9 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin: 2rem 0;
        text-align: center;
    }
    .stats-container h2, .stats-container h3 {
        color: #fff !important;
    }
    </style>
    """,
                unsafe_allow_html=True)

    # Seção de boas-vindas
    icon_base64 = get_image_as_base64(ICON_PATH)
    if icon_base64:
        st.markdown(
            f"""
            <div style="display: flex; align-items: center; justify-content: center; margin-bottom: 20px;">
                <img src="data:image/gif;base64,{icon_base64}" style="max-width: 100px; margin-right: 20px;filter: grayscale(0.6);">
                <h1 style="margin: 0px;color: #595a5c;">Observatório de Dados Aeroportuários</h1>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.title("🛩️ Observatório de Dados Aeroportuário")

    # Estatísticas principais
    st.markdown(f"""
    <div class="stats-container">
        <div style="display: flex; justify-content: space-around; margin-top: 1rem;">
            <div>
                <h2>2019-{ultimo_ano}</h2>
                <p>Período de Análise</p>
            </div>
            <div>
                <h2>70+</h2>
                <p>Aeroportos Monitorados</p>
            </div>
        </div>
    </div>
    """,
                unsafe_allow_html=True)

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
        """,
                    unsafe_allow_html=True)

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
        """,
                    unsafe_allow_html=True)

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
        """,
                    unsafe_allow_html=True)

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
        """,
                    unsafe_allow_html=True)

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
