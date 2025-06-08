
import streamlit as st
import base64
import os
from streamlit_card import card


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
        
    # CSS customizado
    st.markdown("""
    <style>
    .welcome-section {
        text-align: center;
        margin-bottom: 3rem;
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
    """, unsafe_allow_html=True)

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
    """, unsafe_allow_html=True)

    # Cards das páginas usando streamlit-card
    st.markdown("### 🚀 Explore nossas funcionalidades")

    col1, col2 = st.columns(2)

    with col1:
        # Card Chatbot
        chatbot_clicked = card(
            title="🤖 Chatbot Inteligente",
            text="Converse naturalmente sobre dados aeroportuários. Faça perguntas e receba respostas detalhadas com gráficos e análises.",
            image=None,
            styles={
                "card": {
                    "width": "100%",
                    "height": "200px",
                    "border-radius": "15px",
                    "box-shadow": "0 4px 15px rgba(0,0,0,0.1)",
                    "margin": "10px 0px",
                    "background": "linear-gradient(135deg, #85bec0 0%, #5996c9 100%) transparent"
                },
                "text": {
                    "color": "#fff",
                    "font-weight": "lighter",
                    "padding": "0 2em"
                },
                "title": {
                    "font-size": "1.3rem",
                    "font-weight": "bold",
                    "color": "#fff"
                },
                "filter": {
                    "background-color": "rgba(0, 0, 0, 0)"
                }
            },
            key="card_chat"
        )
        
        if chatbot_clicked:
            st.session_state.current_page = 'chat'
            st.rerun()

        # Card Trends
        trends_clicked = card(
            title="📈 Análise de Tendências",
            text="Descubra padrões e tendências históricas. Previsões com IA e detecção automática de anomalias.",
            image=None,
            styles={
                "card": {
                    "width": "100%",
                    "height": "200px",
                    "border-radius": "15px",
                    "box-shadow": "0 4px 15px rgba(0,0,0,0.1)",
                    "margin": "10px 0px",
                    "background": "linear-gradient(135deg, #85bec0 0%, #5996c9 100%) transparent"
                },
                "text": {
                    "color": "#fff",
                    "font-weight": "lighter",
                    "padding": "0 2em"
                },
                "title": {
                    "font-size": "1.3rem",
                    "font-weight": "bold",
                    "color": "#fff"
                },
                "filter": {
                    "background-color": "rgba(0, 0, 0, 0)"
                }
            },
            key="card_trends"
        )
        
        if trends_clicked:
            st.session_state.current_page = 'trends'
            st.rerun()

    with col2:
        # Card Insights
        insights_clicked = card(
            title="📊 Insights Automáticos",
            text="Relatórios inteligentes gerados automaticamente. Análises de mercado, sazonalidade e comparativas.",
            image=None,
            styles={
                "card": {
                    "width": "100%",
                    "height": "200px",
                    "border-radius": "15px",
                    "box-shadow": "0 4px 15px rgba(0,0,0,0.1)",
                    "margin": "10px 0px",
                    "background": "linear-gradient(135deg, #85bec0 0%, #5996c9 100%) transparent"
                },
                "text": {
                    "color": "#fff",
                    "font-weight": "lighter",
                    "padding": "0 2em"
                },
                "title": {
                    "font-size": "1.3rem",
                    "font-weight": "bold",
                    "color": "#fff"
                },
                "filter": {
                    "background-color": "rgba(0, 0, 0, 0)"
                }
            },
            key="card_insights"
        )
        
        if insights_clicked:
            st.session_state.current_page = 'insights'
            st.rerun()

        # Card Analytics
        analytics_clicked = card(
            title="⚡ Analytics Avançado",
            text="Análises estatísticas complexas, correlações, clustering e recomendações inteligentes com IA.",
            image=None,
            styles={
                "card": {
                    "width": "100%",
                    "height": "200px",
                    "border-radius": "15px",
                    "box-shadow": "0 4px 15px rgba(0,0,0,0.1)",
                    "margin": "10px 0px",
                    "background": "linear-gradient(135deg, #85bec0 0%, #5996c9 100%) transparent"
                },
                "text": {
                    "color": "#fff",
                    "font-weight": "lighter",
                    "padding": "0 2em"
                },
                "title": {
                    "font-size": "1.3rem",
                    "font-weight": "bold",
                    "color": "#fff"
                },
                "filter": {
                    "background-color": "rgba(0, 0, 0, 0)"
                }
            },
            key="card_analytics"
        )
        
        if analytics_clicked:
            st.session_state.current_page = 'analytics'
            st.rerun()

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
