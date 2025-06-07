import streamlit as st
import os
import sys
import pandas as pd
import random
import streamlit.components.v1 as components
import base64

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
    reescrever_resposta_com_llm
)

# Importa as funções de banco de dados
from database_logic import init_db, save_conversation, get_all_conversations_as_df

# --- Função para codificar imagem ---
def get_image_as_base64(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# --- Configuração da página Streamlit ---
st.set_page_config(
    page_title="Chatbot de Movimentações Aeroportuárias - IBI",
    page_icon="images/favicon.png",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# Inicializa o banco de dados ao iniciar o app
init_db()

# --- Barra Lateral com Histórico de Conversas ---
with st.sidebar:
    st.title("🗒️ Histórico de Conversas")
    
    history_df = get_all_conversations_as_df()
    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True, hide_index=True)
    else:
        st.info("O histórico de conversas está vazio.")

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
        padding-bottom: 8rem; /* Adiciona espaço no final da página para a barra de chat */
    }
    div[data-testid="stFullScreenFrame"] > div:first-child { margin: 0 auto; display: table; width: 100%; max-width: 700px; }
    .stImage { width: 100%; display: flex; justify-content: center; align-items: center; margin-top: 1rem; margin-bottom: 1rem; }
    .stImage img { width: 100%; max-width: 500px; height: auto; display: block; }
    .stButton > button { text-align: left; justify-content: flex-start; width: 100%; }
    
    /* Esconde o rodapé padrão do Streamlit */
    .stApp > footer {
        display: none;
    }

    .stElementContainer.st-key-show_more button {
        width: auto;
        border-radius: 50%;
    }
    .stElementContainer.st-key-show_more .stButton {
        text-align: center;
    }
    
    </style>
    """,
    unsafe_allow_html=True
)
if os.path.exists(LOGO_PATH):
    st.image(LOGO_PATH)
else:
    st.warning(f"Logo não encontrada em: {LOGO_PATH}")

# --- Título e Descrição ---
icon_base64 = get_image_as_base64(ICON_PATH)
if icon_base64:
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; justify-content: center; margin-bottom: 20px;">
            <img src="data:image/gif;base64,{icon_base64}" style="max-width: 100px; margin-right: 20px;">
            <h1 style="margin: 0px;color: #595a5c;white-space: nowrap;">Chatbot de Movimentações Aeroportuárias</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.title("✈️ Chatbot de Movimentações Aeroportuárias")


st.markdown(
    f"""
    ---
    ##### Olá! Sou seu assistente virtual do *Observatório de Dados* do Instituto Brasileiro de Infraestrutura.
    Fui treinado com dados das movimentações aeroportuárias de **2019 à {ultimo_ano}**.
    Posso responder a perguntas como:
    """
)

# --- Perguntas de Exemplo ---
if 'shuffled_questions' not in st.session_state:
    top_10_icao = obter_top_10_aeroportos(PASTA_ARQUIVOS_PARQUET, ano=ultimo_ano)
    icao_para_nome_map = {v: k for k, v in aeroporto_nome_para_icao.items()}
    available_airports = [icao_para_nome_map.get(icao, icao).title() for icao in top_10_icao]
    num_airports_needed = 7
    if len(available_airports) >= num_airports_needed:
        random_airports = random.sample(available_airports, num_airports_needed)
    elif available_airports:
        random_airports = random.choices(available_airports, k=num_airports_needed)
    else:
        random_airports = ["Brasília", "Guarulhos", "Congonhas", "Galeão", "Salvador", "Recife", "Fortaleza"]

    a1, a2, a3, a4, a5, a6, a7 = random_airports

    preset_questions = [
        f"Quantos passageiros desembarcaram em {a1} no mês de janeiro de {ultimo_ano}?",
        f"Qual foi o total de carga movimentada em {a2} durante o ano de {ultimo_ano}?",
        "Como tem evoluído a movimentação de passageiros no Brasil ao longo do tempo?",
        f"Que companhias aéreas operam atualmente em {a6}?",
        f"Mostre um gráfico da movimentação de passageiros no aeroporto de {a7}.",
        f"Qual companhia aérea liderou o transporte de passageiros em {ultimo_ano}?",
        f"Qual operador foi responsável pelo maior volume de cargas transportadas em {a3} em {ultimo_ano}?",
        f"Para qual destino mais voaram os passageiros partindo de {a4} em {ultimo_ano}?",
        "Qual é o destino nacional mais visitado?",
        f"Qual empresa teve o maior número de atrasos em {a5} durante {ultimo_ano}?",
        f"Qual operador registrou mais atrasos no Brasil em {ultimo_ano}?"
    ]
    random.shuffle(preset_questions)
    st.session_state.shuffled_questions = preset_questions

if 'preset_prompt' not in st.session_state: st.session_state.preset_prompt = ""
if 'process_preset_prompt' not in st.session_state: st.session_state.process_preset_prompt = False
if 'show_all_questions' not in st.session_state: st.session_state.show_all_questions = False

questions_to_show = st.session_state.shuffled_questions
questions_limit = len(questions_to_show) if st.session_state.show_all_questions else 3

for i, q in enumerate(questions_to_show[:questions_limit]):
    if st.button(q, key=f"q_button_{i}", use_container_width=True):
        st.session_state.preset_prompt = q
        st.session_state.process_preset_prompt = True
        st.rerun()

if not st.session_state.show_all_questions and len(questions_to_show) > 3:
    if st.button("➕", key="show_more", use_container_width=True):
        st.session_state.show_all_questions = True
        st.rerun()

# --- Interface do Chat ---
if "messages" not in st.session_state: st.session_state.messages = []
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if isinstance(message["content"], tuple):
            text_content, image_content = message["content"]
            st.markdown(text_content)
            if image_content:
                st.image(image_content, use_container_width=True)
        else:
            st.markdown(message["content"])

# --- Lógica de Processamento Centralizada ---
def process_input(prompt):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            resposta_chatbot_texto = ""
            resposta_chatbot_imagem = None
            parametros = parse_pergunta_com_llm(prompt)
            
            feedback_usuario = []
            intencoes = [k for k, v in parametros.items() if k.startswith('intencao_') and v]
            if not parametros or (not any(parametros.get(k) for k in ["aeroporto", "ano", "mes"]) and not intencoes):
                feedback_usuario.append("Não consegui extrair informações relevantes da sua pergunta.")
            
            if feedback_usuario:
                resposta_chatbot_texto = f"Desculpe. {' '.join(feedback_usuario)} Por favor, tente novamente de forma mais clara."
            else:
                if parametros.get('intencao_historico_movimentacao'):
                    tipo_consulta = "cargas" if parametros.get('intencao_carga') else "passageiros"
                    local = "Brasil"
                    aeroporto_filtro = parametros.get('aeroporto')
                    if aeroporto_filtro:
                        local = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == aeroporto_filtro.upper()), aeroporto_filtro)
                    df_historico = obter_historico_movimentacao(
                        PASTA_ARQUIVOS_PARQUET,
                        tipo_consulta=tipo_consulta,
                        aeroporto=aeroporto_filtro
                    )
                    if df_historico is not None:
                        dados_texto = []
                        for _, row in df_historico.iterrows():
                            dados_texto.append(f"- Ano {row['ANO']}: {formatar_numero_br(row['TotalValor'])}")
                        resposta_chatbot_texto = f"Dados da evolução de {tipo_consulta} para **{local}**:\n" + "\n".join(dados_texto)
                        resposta_chatbot_imagem = gerar_grafico_historico(df_historico, tipo_consulta, local, logo_path=LOGO_PATH)
                    else:
                        resposta_chatbot_texto = f"Não encontrei dados para gerar o histórico de {tipo_consulta} para **{local}**."
                elif parametros.get('intencao_market_share'):
                    resultado_share = calcular_market_share(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'), mes=parametros.get('mes'), aeroporto=parametros.get('aeroporto'))
                    if resultado_share and resultado_share['data']:
                        local_str = "no Brasil"
                        if resultado_share['aeroporto']:
                            nome_aeroporto = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_share['aeroporto'].upper()), resultado_share['aeroporto'])
                            local_str = f"no Aeroporto de {nome_aeroporto}"
                        ano_resp = resultado_share.get('ano', ultimo_ano)
                        periodo_str = f"para o ano de {ano_resp}"
                        if resultado_share['mes']:
                            periodo_str += f" e mês de {mes_numero_para_nome.get(resultado_share['mes'], '')}"
                        
                        resposta_chatbot_texto = f"**Aqui está a participação de mercado {local_str} ({periodo_str.replace('para o ', '')})**\n\n"
                        lista_operadores = []
                        for op in resultado_share['data']:
                            nome_operador = operador_icao_para_nome.get(op['NR_AERONAVE_OPERADOR'], op['NR_AERONAVE_OPERADOR'])
                            lista_operadores.append(f"- **{nome_operador}**: {op['VooShare']:.1f}% dos voos e {op['PaxShare']:.1f}% dos passageiros.")
                        resposta_chatbot_texto += "\n".join(lista_operadores)
                        resposta_chatbot_imagem = gerar_grafico_market_share(resultado_share['data'], logo_path=LOGO_PATH)
                elif parametros.get('intencao_mais_movimentado'):
                    resultado_ranking = obter_aeroporto_mais_movimentado(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'))
                    if resultado_ranking:
                        aeroporto_nome = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                        total_passageiros_formatado = formatar_numero_br(resultado_ranking['total_passageiros'])
                        resposta_chatbot_texto = f"No ano de {resultado_ranking['ano']}, o aeroporto mais movimentado do Brasil foi **{aeroporto_nome}**, com um total de **{total_passageiros_formatado}** passageiros."
                elif parametros.get('intencao_mais_voos_internacionais'):
                    resultado_ranking = obter_aeroporto_mais_voos_internacionais(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'))
                    if resultado_ranking:
                        aeroporto_nome = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                        total_voos_formatado = formatar_numero_br(resultado_ranking['total_voos'])
                        resposta_chatbot_texto = f"No ano de {resultado_ranking['ano']}, o aeroporto com mais voos internacionais foi **{aeroporto_nome}**, com **{total_voos_formatado}** voos."
                elif parametros.get('intencao_maior_operador_pax'):
                    resultado_operador = obter_operador_mais_passageiros(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'), aeroporto=parametros.get('aeroporto'))
                    if resultado_operador:
                        nome_operador = operador_icao_para_nome.get(resultado_operador['operador'].upper(), resultado_operador['operador'].upper())
                        total_pax_formatado = formatar_numero_br(resultado_operador['total_passageiros'])
                        local_str = f"no Brasil em {resultado_operador['ano']}"
                        if resultado_operador.get('aeroporto'):
                            nome_aeroporto = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_operador['aeroporto'].upper()), resultado_operador['aeroporto'])
                            local_str = f"no aeroporto de {nome_aeroporto} em {resultado_operador['ano']}"
                        resposta_chatbot_texto = f"A empresa que mais transportou passageiros {local_str} foi a **{nome_operador}**, com um total de **{total_pax_formatado}** passageiros."
                elif parametros.get('intencao_maior_operador_carga'):
                    resultado_operador = obter_operador_mais_cargas(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'), aeroporto=parametros.get('aeroporto'))
                    if resultado_operador:
                        nome_operador = operador_icao_para_nome.get(resultado_operador['operador'].upper(), resultado_operador['operador'].upper())
                        total_cargas_formatado = formatar_numero_br(resultado_operador['total_cargas'])
                        local_str = f"no Brasil em {resultado_operador['ano']}"
                        if resultado_operador.get('aeroporto'):
                            nome_aeroporto = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_operador['aeroporto'].upper()), resultado_operador['aeroporto'])
                            local_str = f"no aeroporto de {nome_aeroporto} em {resultado_operador['ano']}"
                        resposta_chatbot_texto = f"A empresa que mais transportou cargas {local_str} foi a **{nome_operador}**, com um total de **{total_cargas_formatado}** kg de cargas."
                elif parametros.get('intencao_principal_destino'):
                    resultado_destino = obter_principal_destino(PASTA_ARQUIVOS_PARQUET, aeroporto_origem=parametros.get('aeroporto'), ano=parametros.get('ano'))
                    if resultado_destino and resultado_destino.get('destino_icao'):
                        destino_nome = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_destino['destino_icao'].upper()), resultado_destino['destino_icao'].upper())
                        total_voos_formatado = formatar_numero_br(resultado_destino['total_voos'])
                        local_str = f"no Brasil em {resultado_destino['ano']}"
                        if resultado_destino.get('aeroporto_origem'):
                            nome_aeroporto_origem = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_destino['aeroporto_origem'].upper()), resultado_destino['aeroporto_origem'])
                            local_str = f"para o aeroporto de **{nome_aeroporto_origem}** em {resultado_destino['ano']}"
                        resposta_chatbot_texto = f"O principal destino {local_str}, foi **{destino_nome}**, com um total de **{total_voos_formatado}** voos."
                elif parametros.get('intencao_maiores_atrasos'):
                    resultado_atrasos = obter_operador_maiores_atrasos(PASTA_ARQUIVOS_PARQUET, ano=parametros.get('ano'), aeroporto=parametros.get('aeroporto'))
                    if resultado_atrasos:
                        nome_operador = operador_icao_para_nome.get(resultado_atrasos['operador'].upper(), resultado_atrasos['operador'].upper())
                        total_minutos_atraso = resultado_atrasos['total_minutos_atraso']
                        horas = int(total_minutos_atraso // 60)
                        minutos = int(total_minutos_atraso % 60)
                        local_str = f"no Brasil em {resultado_atrasos['ano']}"
                        if resultado_atrasos.get('aeroporto'):
                            nome_aeroporto = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_atrasos['aeroporto'].upper()), resultado_atrasos['aeroporto'])
                            local_str = f"no aeroporto de **{nome_aeroporto}** em {resultado_atrasos['ano']}"
                        resposta_chatbot_texto = f"A empresa com maiores atrasos {local_str} foi a **{nome_operador}**, com um total de **{horas} horas e {minutos} minutos** de atraso."
                else: 
                    tipo_consulta_db = "passageiros"
                    if parametros.get('intencao_carga'): tipo_consulta_db = "carga"
                    
                    resultados_df = consultar_movimentacoes_aeroportuarias(
                        PASTA_ARQUIVOS_PARQUET, aeroporto=parametros.get('aeroporto'), ano=parametros.get('ano'), mes=parametros.get('mes'),
                        tipo_movimento=parametros.get('tipo_movimento'), natureza=parametros.get('natureza'), tipo_consulta=tipo_consulta_db
                    )
                    
                    total_valor = resultados_df['TotalValor'].iloc[0] if resultados_df is not None and not resultados_df.empty else None

                    if total_valor is not None and pd.notna(total_valor):
                        resposta_semantica = "No "
                        if parametros.get('mes'):
                            resposta_semantica += f"mês de {mes_numero_para_nome.get(parametros['mes'], '')} de "
                        if parametros.get('ano'):
                            resposta_semantica += f"{parametros['ano']}, "
                        if parametros.get('aeroporto'):
                            aeroporto_nome = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == parametros['aeroporto'].upper()), parametros['aeroporto'])
                            if not parametros.get('mes') and not parametros.get('ano'):
                                resposta_semantica = f"O aeroporto de {aeroporto_nome} "
                            else:
                                resposta_semantica += f"o aeroporto de {aeroporto_nome} "
                        
                        valor_formatado = formatar_numero_br(total_valor)

                        if tipo_consulta_db == "passageiros":
                            verbo = "recebeu" if parametros.get('tipo_movimento') == 'P' else ("registrou" if parametros.get('tipo_movimento') == 'D' else "movimentou")
                            resposta_semantica += f"{verbo} um total de **{valor_formatado}** passageiros"
                        else:
                            resposta_semantica += f"movimentou um total de **{valor_formatado}** kg de cargas"
                        
                        if parametros.get('tipo_movimento'):
                            resposta_semantica += f" em {'pousos' if parametros['tipo_movimento'] == 'P' else 'decolagens'}"
                        if parametros.get('natureza'):
                            resposta_semantica += f" em voos {'domésticos' if parametros['natureza'] == 'D' else 'internacionais'}"
                        
                        resposta_chatbot_texto = resposta_semantica.replace(" ,", ",").replace("  ", " ").strip() + "."
                    else:
                        criterios = []
                        if parametros.get('aeroporto'):
                            nome_aeroporto = next((nome.title() for nome, icao in aeroporto_nome_para_icao.items() if icao == parametros['aeroporto'].upper()), parametros['aeroporto'])
                            criterios.append(f"aeroporto: {nome_aeroporto}")
                        if parametros.get('ano'): criterios.append(f"ano: {parametros['ano']}")
                        if parametros.get('mes'): criterios.append(f"mês: {mes_numero_para_nome.get(parametros['mes'], '')}")
                        
                        resposta_chatbot_texto = f"Não foram encontrados dados com os critérios especificados: {', '.join(criterios)}." if criterios else "Não encontrei dados para sua solicitação."
        
        # Reescreve a resposta se não for um erro
        is_error_or_feedback = (
            "Não consegui extrair" in resposta_chatbot_texto or
            "Não encontrei dados" in resposta_chatbot_texto or
            "Não foi possível determinar" in resposta_chatbot_texto
        )
        if not is_error_or_feedback and resposta_chatbot_texto:
            with st.spinner("Elaborando resposta..."):
                resposta_final = reescrever_resposta_com_llm(prompt, resposta_chatbot_texto)
                st.markdown(resposta_final)
                if resposta_chatbot_imagem:
                    st.image(resposta_chatbot_imagem, use_container_width=True)
                
                content_to_save = (resposta_final, resposta_chatbot_imagem) if resposta_chatbot_imagem else resposta_final
                st.session_state.messages.append({"role": "assistant", "content": content_to_save})
                save_conversation(prompt, resposta_final)
        else:
            st.markdown(resposta_chatbot_texto)
            content_to_save = (resposta_chatbot_texto, None)
            st.session_state.messages.append({"role": "assistant", "content": content_to_save})
            save_conversation(prompt, resposta_chatbot_texto)

        st.markdown(
            """
            <script>
                setTimeout(function() {
                    window.top.document.querySelector('div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] > div:last-child').scrollIntoView({ behavior: 'smooth' });
                }, 200);
            </script>
            """,
            unsafe_allow_html=True
        )

# --- Processamento do Input ---
prompt = st.chat_input("Pergunte-me sobre movimentações aeroportuárias...")

final_prompt = None

if prompt:
    final_prompt = prompt
elif st.session_state.get('process_preset_prompt'):
    final_prompt = st.session_state.get('preset_prompt')
    st.session_state.process_preset_prompt = False

if final_prompt:
    process_input(final_prompt)
    st.rerun()
