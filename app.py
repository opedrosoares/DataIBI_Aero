import streamlit as st
import os
import sys
import pandas as pd
import random
import streamlit.components.v1 as components
import base64
from streamlit_mic_recorder import mic_recorder

# Adiciona o diretório atual ao sys.path para que os módulos possam ser importados
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importa as funções de lógica do chatbot
from chatbot_logic import (
    parse_pergunta_com_llm,
    consultar_movimentacoes_aeroportuarias,
    obter_aeroporto_mais_movimentado,
    obter_aeroporto_mais_voos_internacionais,
    obter_operador_mais_passageiros,
    obter_operador_mais_cargas,
    obter_principal_destino,
    obter_ultimo_ano_disponivel,
    formatar_numero_br,
    aeroporto_nome_para_icao,
    mes_numero_para_nome,
    operador_icao_para_nome,
    obter_operador_maiores_atrasos,
    calcular_market_share,
    obter_top_10_aeroportos,
    gerar_grafico_market_share,
    obter_historico_movimentacao,
    gerar_grafico_historico,
    reescrever_resposta_com_llm,
    transcrever_audio
)

# Importa as funções de banco de dados
from database_logic import init_db, save_conversation, get_all_conversations_as_df

# Importa as páginas
from pages import chat_page, insights_page, trends_page, analytics_page

# --- Função para codificar imagem ---
def get_image_as_base64(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# --- Configuração da página Streamlit ---
st.set_page_config(
    page_title="Observatório Aeroportuário - IBI",
    page_icon="images/favicon.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Inicializa o banco de dados ao iniciar o app
init_db()

# Caminho para a pasta de arquivos Parquet
PASTA_ARQUIVOS_PARQUET = 'dados_aeroportuarios_parquet'

# --- Obter o Último Ano Disponível ---
ultimo_ano = obter_ultimo_ano_disponivel(PASTA_ARQUIVOS_PARQUET)
if ultimo_ano is None:
    st.error("Não foi possível determinar o último ano disponível nos dados. Verifique a pasta de arquivos Parquet.")
    st.stop()

# --- Logo e CSS Personalizado ---
APP_DIR = os.path.dirname(os.path.abspath(__file__))
LOGO_PATH = os.path.join(APP_DIR, "images", "logo.svg")
LOGO_WATERMARK_PATH = os.path.join(APP_DIR, "images", "logo.png")
CHAT_ICON_PATH = os.path.join(APP_DIR, "images", "chat_page.gif")
INSIGHTS_ICON_PATH = os.path.join(APP_DIR, "images", "insights_page.gif")
ANALYTICS_ICON_PATH = os.path.join(APP_DIR, "images", "analytics_page.gif")
TRENDS_ICON_PATH = os.path.join(APP_DIR, "images", "trends_page.gif")

st.markdown(
    """
    <style>
    /* Design principal da página */
    .stApp {
        padding-bottom: 2rem;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background-color: #f8f9fa;
    }

    /* Main content area */
    .main .block-container {
        padding-top: 2rem;
        max-width: 1200px;
    }

    /* Navigation styling */
    .nav-item {
        padding: 0.5rem 1rem;
        margin: 0.2rem 0;
        border-radius: 0.5rem;
        cursor: pointer;
        transition: background-color 0.3s;
    }

    .nav-item:hover {
        background-color: #e9ecef;
    }

    .nav-item.active {
        background-color: #007bff;
        color: white;
    }

    /* Responsive design */
    @media (max-width: 768px) {
        .main .block-container {
            padding-top: 1rem;
        }
    }

    /* Title styling */
    h1, h2, h3 {
        color: #595a5c !important;
    }

    /* Primary button styling */
    .stButton > button[data-testid="baseButton-primary"] {
        background-color: #017fff !important;
        border-color: #017fff !important;
        color: white !important;
        justify-content: center;
    }

    .stButton > button[data-testid="baseButton-primary"]:hover {
        background-color: #017fff !important;
        border-color: #017fff !important;
    }

    /* Hide streamlit default elements */
    .stApp > footer {
        display: none;
    }

    #MainMenu {
        visibility: hidden;
    }

    .stDeployButton {
        display: none;
    }

    div[data-testid="stFullScreenFrame"] > div:first-child { margin: 0 auto; display: table; width: 100%; max-width: 700px; }
    .stImage { width: 100%; display: flex; justify-content: center; align-items: center; margin-top: 1rem; margin-bottom: 1rem; }
    .stImage img { width: 100%; max-width: 500px; height: auto; display: block; }
    .stButton > button { text-align: left; justify-content: flex-start; width: 100%; }

    .st-key-audio_recorder {
        position: fixed;
        z-index: 999;
        width: 40px;
        bottom: 50px;
        margin-left: 100px;
    }
    /* condition for screen size minimum of 736px */
    @media (max-width:736px) {
        .st-key-audio_recorder {
            position: fixed;
            z-index: 999;
            right: 10px;
            width: 40px;
            bottom: 50px;
        }
    }

    /* Primary button styling */
    .stButton > button[data-testid="stBaseButton-primary"] {
        background-color: #007bff !important;
        border-color: #007bff !important;
        color: white !important;
        justify-content: center;
    }
    .stButton > button[data-testid="stBaseButton-primary"]:hover {
        background-color: #0056b3 !important;
        border-color: #0056b3 !important;
    }

    /* Classe personalizada para o botão "Ver mais" */
    .st-key-show_more button {
        width: auto;
        border-radius: 50%;
    }
    .st-key-show_more .stButton {
        text-align: center;
    }
    .st-key-chat_input {
        padding-right: 3em;
    }
    div[data-testid="stSidebarNav"] {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# --- Barra Lateral com Navegação ---
with st.sidebar:
    # Logo na sidebar
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=200)

    # Menu de navegação
    pages = {
        "🤖 Chatbot": "chat",
        "📊 Insights Automáticos": "insights", 
        "📈 Análise de Tendências": "trends",
        "⚡ Analytics Avançado": "analytics"
    }

    # Inicializa a página atual
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'chat'

    # Botões de navegação
    for page_name, page_key in pages.items():
        if st.button(page_name, key=f"nav_{page_key}", use_container_width=True):
            st.session_state.current_page = page_key
            st.rerun()

    st.markdown("---")

    # Histórico de conversas (apenas na página do chat)
    if st.session_state.current_page == 'chat':
        st.markdown("### 🗒️ Histórico")
        history_df = get_all_conversations_as_df()
        if not history_df.empty:
            st.dataframe(history_df.tail(5), use_container_width=True, hide_index=True)
        else:
            st.info("Histórico vazio.")

# --- Display Logo in Main Content Area for non-chat pages ---
if st.session_state.current_page != 'chat':
    if LOGO_PATH and os.path.exists(LOGO_PATH):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(LOGO_PATH, width=300)

# --- Renderização da Página Atual ---
if st.session_state.current_page == 'chat':
    chat_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, CHAT_ICON_PATH)
elif st.session_state.current_page == 'insights':
    insights_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, INSIGHTS_ICON_PATH)
elif st.session_state.current_page == 'trends':
    trends_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, TRENDS_ICON_PATH)
elif st.session_state.current_page == 'analytics':
    analytics_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, ANALYTICS_ICON_PATH)