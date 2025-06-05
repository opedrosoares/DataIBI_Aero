import streamlit as st
import os
import sys

# Adiciona o diretório atual ao sys.path para que chatbot_logic.py possa ser importado
sys.path.append(os.path.dirname(__file__))

# Importa todas as funções e variáveis necessárias do chatbot_logic.py
from chatbot_logic import (
    parse_pergunta_com_llm,
    consultar_movimentacoes_aeroportuarias,
    obter_aeroporto_mais_movimentado,
    obter_aeroporto_mais_voos_internacionais,
    formatar_numero_br,
    aeroporto_nome_para_icao,
    mes_numero_para_nome
)

# Configuração da página Streamlit
st.set_page_config(
    page_title="Chatbot de Movimentações Aeroportuárias",
    page_icon="✈️",
    layout="centered",
    initial_sidebar_state="auto"
)

# Caminho para a pasta de arquivos Parquet
PASTA_ARQUIVOS_PARQUET = 'dados_aeroportuarios_parquet'

# --- Título e Descrição do Chatbot ---
st.title("✈️ Chatbot de Movimentações Aeroportuárias")
st.markdown(
    """
    #### Olá! Sou seu assitente virtual do **Observatório de Dados** do Instituto Brasileiro de Infraestrutura.
    ---
    Fui treinado com dados das movimentações aeroportuárias de 2019 à 2024.

    Posso responder a perguntas como:
    > Qual o volume de passageiros que chegaram em Recife em janeiro de 2024?
    >
    > Total de carga em Guarulhos em 2024?
    >
    > Qual o aeroporto mais movimentado do Brasil?
    >
    > Qual aeroporto com mais voos internacionais?
    """
)

# Verifica se os arquivos Parquet existem antes de continuar
if not os.path.exists(PASTA_ARQUIVOS_PARQUET) or not os.listdir(PASTA_ARQUIVOS_PARQUET):
    st.error("Atenção: A pasta 'dados_aeroportuarios_parquet' não foi encontrada ou está vazia.")
    st.info("Por favor, execute o script `conversor_json_parquet.py` para gerar os arquivos Parquet e coloque-os na pasta.")
    st.stop() # Para a execução do Streamlit se os dados não estiverem prontos

# --- Interface do Chat (Histórico de Conversa) ---
# Inicializa o histórico de chat na sessão do Streamlit
if "messages" not in st.session_state:
    st.session_state.messages = []

# Exibe mensagens do histórico de chat
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Entrada do Usuário ---
if prompt := st.chat_input("Pergunte-me sobre movimentações aeroportuárias..."):
    # Adiciona a mensagem do usuário ao histórico
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            resposta_chatbot = ""
            parametros = parse_pergunta_com_llm(prompt) # Chamada ao LLM

            # --- Lógica de Tratamento de Entidades, Intenções e Geração de Resposta ---
            feedback_usuario = []
            if not parametros or not any(parametros.get(k) for k in ["aeroporto", "ano", "mes", "tipo_movimento", "natureza", "intencao_carga", "intencao_mais_movimentado", "intencao_mais_voos_internacionais"]):
                feedback_usuario.append("Não consegui extrair informações relevantes da sua pergunta.")
            else:
                for key in ["intencao_carga", "intencao_mais_movimentado", "intencao_mais_voos_internacionais"]:
                    if key not in parametros or not isinstance(parametros[key], bool):
                        parametros[key] = False

                if not (parametros['intencao_mais_movimentado'] or parametros['intencao_mais_voos_internacionais']):
                    if not parametros.get('aeroporto'):
                        feedback_usuario.append("Não identifiquei o aeroporto. Poderia especificar o nome ou código ICAO?")
                    if not parametros.get('ano'):
                        if not (parametros.get('aeroporto') and parametros.get('mes')):
                            feedback_usuario.append("Não identifiquei o ano. Poderia especificar o ano da movimentação?")
                    
            if feedback_usuario:
                resposta_chatbot = f"Desculpe. {' '.join(feedback_usuario)} Por favor, tente novamente de forma mais clara."
            else:
                # Convertendo mês para int se não for null
                if 'mes' in parametros and parametros['mes'] is not None:
                    try:
                        parametros['mes'] = int(parametros['mes'])
                    except (ValueError, TypeError):
                        parametros['mes'] = None
                
                # Lógica para Responder às Novas Perguntas de Ranking
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
                
                else: # Lógica para perguntas gerais de volume/carga
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
                        resposta_semantica = f"Em "
                        
                        if parametros.get('mes'):
                            nome_do_mes = mes_numero_para_nome.get(parametros['mes'], str(parametros['mes']))
                            resposta_semantica += f"o mês de {nome_do_mes.capitalize()} de "

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

                    else: # Se a consulta ao DB retornar vazio
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

            # Exibe a resposta do chatbot na interface
            st.markdown(resposta_chatbot)
            st.session_state.messages.append({"role": "assistant", "content": resposta_chatbot})