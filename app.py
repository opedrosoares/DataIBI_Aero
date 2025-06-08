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
LOGO_PATH = os.path.join(APP_DIR, "images", "logo.png")
ICON_PATH = os.path.join(APP_DIR, "images", "icone.gif")

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
    </style>
    """,
    unsafe_allow_html=True
)

# --- Barra Lateral Estilizada ---
with st.sidebar:
    # Logo na sidebar
    if os.path.exists(LOGO_PATH):
        st.image(LOGO_PATH, width=200)

    # Inicializa a página atual
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'chat'

    # Navegação estilizada com radio buttons
    st.markdown("### 🧭 Navegação")
    
    page_options = [
        "🤖 Chatbot",
        "📊 Insights Automáticos", 
        "📈 Análise de Tendências",
        "⚡ Analytics Avançado"
    ]
    
    page_mapping = {
        "🤖 Chatbot": "chat",
        "📊 Insights Automáticos": "insights", 
        "📈 Análise de Tendências": "trends",
        "⚡ Analytics Avançado": "analytics"
    }
    
    # Encontra o índice da página atual
    current_page_display = None
    for display_name, page_key in page_mapping.items():
        if page_key == st.session_state.current_page:
            current_page_display = display_name
            break
    
    selected_page = st.radio(
        "",
        page_options,
        index=page_options.index(current_page_display) if current_page_display else 0,
        key="page_selector"
    )
    
    # Atualiza a página se mudou
    if page_mapping[selected_page] != st.session_state.current_page:
        st.session_state.current_page = page_mapping[selected_page]
        st.rerun()

    st.markdown("---")

    # Histórico de conversas estilizado (apenas na página do chat)
    if st.session_state.current_page == 'chat':
        st.markdown("### 📋 Histórico de Conversas")
        
        history_df = get_all_conversations_as_df()
        if not history_df.empty:
            # Estilização do histórico
            st.markdown("""
            <style>
            .chat-history {
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 8px;
                margin: 5px 0;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Mostra últimas 5 conversas com estilo
            recent_conversations = history_df.tail(5)
            
            for idx, row in recent_conversations.iterrows():
                with st.container():
                    st.markdown(f"""
                    <div class="chat-history">
                        <small><strong>🕒 {row['timestamp']}</strong></small><br>
                        <strong>❓ Pergunta:</strong> {row['question'][:50]}{'...' if len(row['question']) > 50 else ''}<br>
                        <strong>💬 Resposta:</strong> {row['answer'][:50]}{'...' if len(row['answer']) > 50 else ''}
                    </div>
                    """, unsafe_allow_html=True)
                    
            if len(history_df) > 5:
                st.caption(f"Mostrando 5 de {len(history_df)} conversas")
        else:
            st.info("📝 Nenhuma conversa registrada ainda")

# --- Display Logo in Main Content Area ---
if st.session_state.current_page != 'chat':  # Chat page has its own logo display
    if LOGO_PATH and os.path.exists(LOGO_PATH):
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.image(LOGO_PATH, width=300)
        st.markdown("---")

# --- Renderização da Página Atual ---
if st.session_state.current_page == 'chat':
    chat_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH, ICON_PATH)
elif st.session_state.current_page == 'insights':
    insights_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH)
elif st.session_state.current_page == 'trends':
    trends_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH)
elif st.session_state.current_page == 'analytics':
    analytics_page.render(PASTA_ARQUIVOS_PARQUET, ultimo_ano, LOGO_PATH)