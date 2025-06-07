import streamlit as st
import os
import sys
import pandas as pd

# Adiciona o diretório atual ao sys.path para que os módulos possam ser importados
sys.path.append(os.path.dirname(__file__))

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
    obter_operador_maiores_atrasos
)

# --- NOVO: Importa as funções de banco de dados ---
from database_logic import init_db, save_conversation, get_all_conversations_as_df

# --- Configuração da página Streamlit ---
st.set_page_config(
    page_title="Chatbot de Movimentações Aeroportuárias - IBI",
    page_icon="images/favicon.png",
    layout="centered",
    initial_sidebar_state="auto"
)

# --- NOVO: Inicializa o banco de dados ao iniciar o app ---
init_db()

# Caminho para a pasta de arquivos Parquet
PASTA_ARQUIVOS_PARQUET = 'dados_aeroportuarios_parquet'

# --- Obter o Último Ano Disponível ---
ultimo_ano = obter_ultimo_ano_disponivel(PASTA_ARQUIVOS_PARQUET)
if ultimo_ano is None:
    st.error("Não foi possível determinar o último ano disponível nos dados. Verifique a pasta de arquivos Parquet.")
    st.stop()

# --- Logo e CSS Personalizado (sem alterações) ---
LOGO_PATH = "images/logo.svg"
st.markdown(
    """
    <style>
    div[data-testid="stFullScreenFrame"] > div:first-child {
        margin: 0 auto;
        display: table;
        width: 100%;
        max-width: 700px;
    }
    .stImage {
        width: 100%;
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .stImage img {
        width: 100%;
        max-width: 500px;
        height: auto;
        display: block;
    }
    .stButton > button {
        text-align: left;
        justify-content: flex-start;
        width: 100%;
    }
    div[data-testid="stForm"] {
        margin-left: auto;
        margin-right: auto;
        max-width: 700px;
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
preset_questions = [
    f"Qual o volume de passageiros que chegaram em Recife em janeiro de {ultimo_ano}?",
    f"Total de carga transportada em Guarulhos em {ultimo_ano}?",
    "Qual o aeroporto mais movimentado do Brasil?",
    "Qual aeroporto com mais voos internacionais?",
    f"Qual a empresa que mais transportou passageiros em {ultimo_ano}?",
    f"Qual o operador que mais transportou cargas em Brasília em {ultimo_ano}?",
    f"Qual foi o principal destino para o aeroporto de Brasília em {ultimo_ano}?",
    "Qual o destino mais acessado no Brasil?",
    f"Qual a empresa com maiores atrasos em Brasília em {ultimo_ano}?",
    f"Qual o operador com maiores atrasos no Brasil em {ultimo_ano}?"
]

if 'preset_prompt' not in st.session_state:
    st.session_state.preset_prompt = ""
if 'process_preset_prompt' not in st.session_state:
    st.session_state.process_preset_prompt = False

for i, q in enumerate(preset_questions):
    if st.button(q, key=f"q_button_{i}", use_container_width=True):
        st.session_state.preset_prompt = q
        st.session_state.process_preset_prompt = True
        st.rerun()

# --- Verificação dos arquivos Parquet ---
if not os.path.exists(PASTA_ARQUIVOS_PARQUET) or not os.listdir(PASTA_ARQUIVOS_PARQUET):
    st.error("Atenção: A pasta 'dados_aeroportuarios_parquet' não foi encontrada ou está vazia.")
    st.info("Por favor, execute o script `conversor_json_parquet.py` para gerar os arquivos Parquet e coloque-os na pasta.")
    st.stop()

# --- Interface do Chat (Histórico da Sessão Atual) ---
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Lógica principal do Chat ---
current_prompt = None

if prompt_from_input := st.chat_input("Pergunte-me sobre movimentações aeroportuárias..."):
    current_prompt = prompt_from_input
elif st.session_state.process_preset_prompt:
    current_prompt = st.session_state.preset_prompt
    st.session_state.process_preset_prompt = False
    st.session_state.preset_prompt = ""

if current_prompt:
    st.session_state.messages.append({"role": "user", "content": current_prompt})
    with st.chat_message("user"):
        st.markdown(current_prompt)

    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            # A lógica para gerar a resposta permanece a mesma
            resposta_chatbot = ""
            parametros = parse_pergunta_com_llm(current_prompt)
            
            # [TODA A LÓGICA DE PROCESSAMENTO DA PERGUNTA E GERAÇÃO DA RESPOSTA VEM AQUI]
            # [COLE AQUI A LÓGICA ORIGINAL DO SEU 'app.py' QUE FICA DENTRO DO 'with st.spinner("Pensando..."):' ]
            # ...
            # Para manter o exemplo conciso, vamos simular que a resposta foi gerada.
            # No seu código final, você deve manter toda a sua lógica de `if/elif/else` para definir a `resposta_chatbot`.
            
            # --- Início da Lógica de Geração de Resposta (COPIADA DO SEU ARQUIVO ORIGINAL) ---
            feedback_usuario = []
            if not parametros or not (
                any(parametros.get(k) for k in ["aeroporto", "ano", "mes", "tipo_movimento", "natureza", "intencao_carga"]) or
                parametros.get('intencao_mais_movimentado') or
                parametros.get('intencao_mais_voos_internacionais') or
                parametros.get('intencao_maior_operador_pax') or
                parametros.get('intencao_maior_operador_carga') or
                parametros.get('intencao_principal_destino') or
                parametros.get('intencao_maiores_atrasos')
            ):
                feedback_usuario.append("Não consegui extrair informações relevantes da sua pergunta.")
            else:
                for key in ["intencao_carga", "intencao_mais_movimentado", "intencao_mais_voos_internacionais",
                            "intencao_maior_operador_pax", "intencao_maior_operador_carga", "intencao_principal_destino", "intencao_maiores_atrasos"]:
                    if key not in parametros or not isinstance(parametros[key], bool):
                        parametros[key] = False

                if not (parametros['intencao_mais_movimentado'] or parametros['intencao_mais_voos_internacionais'] or
                        parametros['intencao_maior_operador_pax'] or parametros['intencao_maior_operador_carga'] or
                        parametros['intencao_principal_destino'] or parametros['intencao_maiores_atrasos']):
                    if not parametros.get('aeroporto'):
                        feedback_usuario.append("Não identifiquei o aeroporto. Poderia especificar o nome ou código ICAO?")
                    if not parametros.get('ano'):
                        if not (parametros.get('aeroporto') and parametros.get('mes')):
                            feedback_usuario.append("Não identifiquei o ano. Poderia especificar o ano da movimentação?")
                
                if (parametros['intencao_maior_operador_pax'] or parametros['intencao_maior_operador_carga'] or parametros['intencao_principal_destino'] or parametros['intencao_maiores_atrasos']):
                    if parametros.get('aeroporto') and (parametros.get('aeroporto').upper() not in aeroporto_nome_para_icao.values()):
                        if not (parametros['aeroporto'].lower() == "brasil" or parametros['aeroporto'] == "null"):
                            feedback_usuario.append(f"O aeroporto '{parametros['aeroporto']}' não é reconhecido. Poderia usar um código ICAO ou nome mais comum?")

            if feedback_usuario:
                resposta_chatbot = f"Desculpe. {' '.join(feedback_usuario)} Por favor, tente novamente de forma mais clara."
            else:
                if 'mes' in parametros and parametros['mes'] is not None:
                    try:
                        parametros['mes'] = int(parametros['mes'])
                    except (ValueError, TypeError):
                        parametros['mes'] = None
                
                if parametros['intencao_mais_movimentado']:
                    ano_referencia = parametros.get('ano')
                    resultado_ranking = obter_aeroporto_mais_movimentado(PASTA_ARQUIVOS_PARQUET, ano=ano_referencia)
                    if resultado_ranking:
                        aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                        total_passageiros_formatado = formatar_numero_br(resultado_ranking['total_passageiros'])
                        resposta_chatbot = f"No ano de {resultado_ranking['ano']}, o aeroporto mais movimentado do Brasil foi **{aeroporto_nome.title()}**, com um total de **{total_passageiros_formatado}** passageiros."
                    else:
                        resposta_chatbot = f"Não foi possível determinar o aeroporto mais movimentado. Verifique os dados para o ano {ano_referencia if ano_referencia else 'disponível'}."
                
                elif parametros['intencao_mais_voos_internacionais']:
                    ano_referencia = parametros.get('ano')
                    resultado_ranking = obter_aeroporto_mais_voos_internacionais(PASTA_ARQUIVOS_PARQUET, ano=ano_referencia)
                    if resultado_ranking:
                        aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                        total_voos_formatado = formatar_numero_br(resultado_ranking['total_voos'])
                        resposta_chatbot = f"No ano de {resultado_ranking['ano']}, o aeroporto com mais voos internacionais foi **{aeroporto_nome.title()}**, com **{total_voos_formatado}** voos internacionais registrados."
                    else:
                        resposta_chatbot = f"Não foi possível determinar o aeroporto com mais voos internacionais. Verifique os dados para o ano {ano_referencia if ano_referencia else 'disponível'}."

                elif parametros['intencao_maior_operador_pax']:
                    ano_referencia = parametros.get('ano')
                    aeroporto_filtro = parametros.get('aeroporto')
                    resultado_operador = obter_operador_mais_passageiros(PASTA_ARQUIVOS_PARQUET, ano=ano_referencia, aeroporto=aeroporto_filtro)
                    if resultado_operador:
                        ano_str = f" em {resultado_operador['ano']}" if resultado_operador['ano'] else ""
                        aeroporto_str = ""
                        if aeroporto_filtro:
                            nome_aeroporto_exibicao = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == aeroporto_filtro.upper()), aeroporto_filtro.upper())
                            aeroporto_str = f" no aeroporto de {nome_aeroporto_exibicao.title()}"
                        
                        total_pax_formatado = formatar_numero_br(resultado_operador['total_passageiros'])
                        nome_operador_completo = operador_icao_para_nome.get(resultado_operador['operador'].upper(), resultado_operador['operador'].upper())
                        
                        resposta_chatbot = f"A empresa que mais transportou passageiros{aeroporto_str}{ano_str} foi a **{nome_operador_completo}**, com um total de **{total_pax_formatado}** passageiros."
                    else:
                        resposta_chatbot = f"Não foi possível determinar a empresa que mais transportou passageiros para os critérios especificados (Ano: {ano_referencia if ano_referencia else 'último disponível'}, Aeroporto: {aeroporto_filtro if aeroporto_filtro else 'todos'})."

                elif parametros['intencao_maior_operador_carga']:
                    ano_referencia = parametros.get('ano')
                    aeroporto_filtro = parametros.get('aeroporto')
                    resultado_operador = obter_operador_mais_cargas(PASTA_ARQUIVOS_PARQUET, ano=ano_referencia, aeroporto=aeroporto_filtro)
                    if resultado_operador:
                        ano_str = f" em {resultado_operador['ano']}" if resultado_operador['ano'] else ""
                        aeroporto_str = ""
                        if aeroporto_filtro:
                            nome_aeroporto_exibicao = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == aeroporto_filtro.upper()), aeroporto_filtro.upper())
                            aeroporto_str = f" no aeroporto de {nome_aeroporto_exibicao.title()}"

                        total_cargas_formatado = formatar_numero_br(resultado_operador['total_cargas'])
                        nome_operador_completo = operador_icao_para_nome.get(resultado_operador['operador'].upper(), resultado_operador['operador'].upper())

                        resposta_chatbot = f"A empresa que mais transportou cargas{aeroporto_str}{ano_str} foi a **{nome_operador_completo}**, com um total de **{total_cargas_formatado}** kg de cargas."
                    else:
                        resposta_chatbot = f"Não foi possível determinar a empresa que mais transportou cargas para os critérios especificados (Ano: {ano_referencia if ano_referencia else 'último disponível'}, Aeroporto: {aeroporto_filtro if aeroporto_filtro else 'todos'})."

                elif parametros['intencao_principal_destino']:
                    ano_referencia = parametros.get('ano')
                    aeroporto_origem_filtro = parametros.get('aeroporto')
                    
                    resultado_destino = obter_principal_destino(PASTA_ARQUIVOS_PARQUET, aeroporto_origem=aeroporto_origem_filtro, ano=ano_referencia)
                    
                    if resultado_destino and resultado_destino['destino_icao'] is not None:
                        destino_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_destino['destino_icao'].upper()), resultado_destino['destino_icao'].upper())
                        total_voos_formatado = formatar_numero_br(resultado_destino['total_voos'])
                        frase_aeroporto_origem = ""
                        if aeroporto_origem_filtro:
                            nome_aeroporto_origem_exibicao = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == aeroporto_origem_filtro.upper()), aeroporto_origem_filtro.upper())
                            frase_aeroporto_origem = f" para o aeroporto de **{nome_aeroporto_origem_exibicao.title()}**"

                        resposta_chatbot = f"No ano de {resultado_destino['ano']}{frase_aeroporto_origem}, o principal destino foi **{destino_nome.title()}**, com um total de **{total_voos_formatado}** voos."
                    else:
                        resposta_chatbot = f"Não foi possível determinar o principal destino para os critérios especificados (Ano: {ano_referencia if ano_referencia else 'último disponível'}, Aeroporto: {aeroporto_origem_filtro if aeroporto_origem_filtro else 'todos'})."

                elif parametros['intencao_maiores_atrasos']:
                    ano_referencia = parametros.get('ano')
                    aeroporto_filtro = parametros.get('aeroporto')
                    resultado_atrasos = obter_operador_maiores_atrasos(PASTA_ARQUIVOS_PARQUET, ano=ano_referencia, aeroporto=aeroporto_filtro)
                    
                    if resultado_atrasos:
                        nome_operador_completo = operador_icao_para_nome.get(resultado_atrasos['operador'].upper(), resultado_atrasos['operador'].upper())
                        total_minutos_atraso = resultado_atrasos['total_minutos_atraso']
                        horas = int(total_minutos_atraso // 60)
                        minutos = int(total_minutos_atraso % 60)
                        ano_str = f" em {resultado_atrasos['ano']}" if resultado_atrasos['ano'] else ""
                        aeroporto_str_exibicao = ""
                        if aeroporto_filtro:
                            nome_aeroporto_exibicao = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == aeroporto_filtro.upper()), aeroporto_filtro.upper())
                            aeroporto_str_exibicao = f" no aeroporto de **{nome_aeroporto_exibicao.title()}**"

                        resposta_chatbot = f"A empresa com maiores atrasos{aeroporto_str_exibicao}{ano_str} foi a **{nome_operador_completo}**, com um total de **{horas} horas e {minutos} minutos** de atraso em pousos."
                    else:
                        resposta_chatbot = f"Não foi possível determinar a empresa com maiores atrasos para os critérios especificados (Ano: {ano_referencia if ano_referencia else 'último disponível'}, Aeroporto: {aeroporto_filtro if aeroporto_filtro else 'todos'})."
                
                else: 
                    tipo_consulta_db = "passageiros"
                    if parametros.get('intencao_carga'):
                        tipo_consulta_db = "carga"

                    resultados_df = consultar_movimentacoes_aeroportuarias(
                        PASTA_ARQUIVOS_PARQUET,
                        aeroporto=parametros.get('aeroporto'),
                        ano=parametros.get('ano'),
                        mes=parametros.get('mes'),
                        tipo_movimento=parametros.get('tipo_movimento'),
                        natureza=parametros.get('natureza'),
                        tipo_consulta=tipo_consulta_db
                    )

                    if resultados_df is not None and not resultados_df.empty:
                        total_valor = resultados_df['TotalValor'].iloc[0]
                        resposta_semantica = f"No "
                        if parametros.get('mes'):
                            nome_do_mes = mes_numero_para_nome.get(parametros['mes'], str(parametros['mes']))
                            resposta_semantica += f"mês de {nome_do_mes.capitalize()} de "

                        if parametros.get('ano'):
                            resposta_semantica += f"{parametros['ano']}"
                            if parametros.get('aeroporto') or parametros.get('mes'):
                                resposta_semantica += ", "
                            else:
                                resposta_semantica += " "

                        if parametros.get('aeroporto'):
                            aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == parametros['aeroporto'].upper()), parametros['aeroporto'].upper())
                            if not parametros.get('mes') and not parametros.get('ano'):
                                resposta_semantica = f"O aeroporto de {aeroporto_nome.title()} "
                            else:
                                resposta_semantica += f"o aeroporto de {aeroporto_nome.title()} "

                        valor_formatado = formatar_numero_br(total_valor)

                        if tipo_consulta_db == "passageiros":
                            verbo = "recebeu" if parametros.get('tipo_movimento') == 'P' else ("registrou" if parametros.get('tipo_movimento') == 'D' else "movimentou")
                            resposta_semantica += f"{verbo} um total de **{valor_formatado}** passageiros"
                        else: # Carga
                            resposta_semantica += f"movimentou um total de **{valor_formatado}** kg de cargas"

                        if parametros.get('tipo_movimento'):
                            mov_tipo = "pousos" if parametros['tipo_movimento'] == 'P' else "decolagens"
                            resposta_semantica += f" em {mov_tipo}"

                        if parametros.get('natureza'):
                            natureza_tipo = "domésticos" if parametros['natureza'] == 'D' else "internacionais"
                            resposta_semantica += f" em voos {natureza_tipo}"

                        resposta_semantica += "."
                        resposta_chatbot = resposta_semantica.replace(", ,", ", ").replace("  ", " ").strip().replace(" .", ".")

                    else:
                        criterios = []
                        if parametros.get('aeroporto'):
                            aeroporto_nome_feedback = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == parametros['aeroporto'].upper()), parametros['aeroporto'].upper())
                            criterios.append(f"aeroporto: {aeroporto_nome_feedback.title()}")
                        if parametros.get('ano'):
                                criterios.append(f"ano: {parametros['ano']}")
                        if parametros.get('mes'):
                            nome_do_mes_feedback = mes_numero_para_nome.get(parametros['mes'], str(parametros['mes']))
                            criterios.append(f"mês: {nome_do_mes_feedback.capitalize()}")
                        if parametros.get('tipo_movimento'):
                            mov_tipo_feedback = "Pouso" if parametros['tipo_movimento'] == 'P' else "Decolagem"
                            criterios.append(f"tipo de movimento: {mov_tipo_feedback}")
                        if parametros.get('natureza'):
                            natureza_tipo_feedback = "Doméstico" if parametros['natureza'] == 'D' else "Internacional"
                            criterios.append(f"natureza: {natureza_tipo_feedback}")
                        
                        tipo_valor_feedback = "passageiros" if tipo_consulta_db == "passageiros" else "cargas"
                        criterios.append(f"para {tipo_valor_feedback}")

                        if criterios:
                            resposta_chatbot = f"Não foram encontrados dados com os critérios especificados. Para os critérios {', '.join(criterios)}: Verifique se os dados existem para esta combinação ou tente critérios de pesquisa mais amplos."
                        else:
                            resposta_chatbot = "Não foram encontrados dados com os critérios especificados. Por favor, tente uma pergunta diferente ou especifique mais detalhes."
            # --- Fim da Lógica de Geração de Resposta ---

            # Exibe a resposta do chatbot na interface
            st.markdown(resposta_chatbot)
            
            # --- NOVO: Salva a conversa no banco de dados ---
            if current_prompt and resposta_chatbot:
                save_conversation(current_prompt, resposta_chatbot)
            
            # Adiciona a resposta ao histórico da sessão atual
            st.session_state.messages.append({"role": "assistant", "content": resposta_chatbot})

# --- NOVO: Seção para exibir o histórico de conversas do banco de dados ---
st.markdown("---")
with st.expander("Ver Histórico de Conversas Salvas"):
    history_df = get_all_conversations_as_df()
    if not history_df.empty:
        # Usamos st.dataframe para uma visualização em tabela interativa
        st.dataframe(history_df, use_container_width=True, hide_index=True)
    else:
        st.info("O histórico de conversas está vazio.")
