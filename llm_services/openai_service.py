import io
import json
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from utils.constants import client, OPENAI_MODEL, aeroporto_nome_para_icao

def transcrever_audio(audio_data):
    """Transcreve um arquivo de áudio usando a API da OpenAI"""
    if not client:
        print("Cliente OpenAI não configurado. Transcrição de áudio abortada.")
        return None
    try:
        audio_file = io.BytesIO(audio_data)
        audio_file.name = "audio.wav"
        transcript = client.audio.transcriptions.create(
            model="whisper-1",
            file=audio_file
        )
        return transcript.text
    except Exception as e:
        print(f"Erro ao transcrever áudio: {e}")
        return None

def reescrever_resposta_com_llm(pergunta, resposta_factual):
    """Usa o LLM para reescrever a resposta factual de forma mais fluida e elaborada"""
    if not client:
        print("Cliente OpenAI não configurado. Reescrita da resposta abortada.")
        return resposta_factual

    prompt_messages = [
        {
            "role": "system",
            "content": "Você é um assistente de comunicação especializado em aviação e dados. Sua tarefa é reescrever respostas técnicas e factuais, tornando-as mais naturais, fluídas e informativas para um usuário geral, sem perder a precisão dos dados. Adicione um breve contexto ou um fato interessante sobre o tema quando apropriado."
        },
        {
            "role": "user",
            "content": f"""Reescreva a resposta abaixo, ampliando-a suscintamente.

**Pergunta Original:**
"{pergunta}"

**Resposta Factual a ser Reescrevida:**
"{resposta_factual}"
"""
        }
    ]

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=prompt_messages,
            max_tokens=500,
            temperature=0.7
        )
        
        rewritten_text = completion.choices[0].message.content
        return rewritten_text.strip()
    except Exception as e:
        print(f"DEBUG: Erro ao reescrever resposta com o LLM: {e}")
        return resposta_factual

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_result(lambda result: not result))
def parse_pergunta_com_llm(pergunta_usuario):
    """Usa o LLM para extrair parâmetros e intenções da pergunta do usuário"""
    prompt_messages = [
        {"role": "system", "content": f"""Você é um assistente especializado em extrair informações de perguntas sobre movimentações aeroportuárias.
        Sua única saída deve ser um objeto JSON.
        Extraia os seguintes parâmetros da pergunta do usuário. Se um parâmetro não for mencionado ou não se aplica, seu valor deve ser `null`.
        Identifique intenções específicas para perguntas sobre rankings de aeroportos, operadores e destinos.

        Parâmetros a extrair:
        - "aeroporto": (string, nome da cidade ou código ICAO, ex: "Recife" ou "SBRF")
        - "ano": (número inteiro)
        - "mes": (número inteiro de 1 a 12)
        - "tipo_movimento": ("P" para pouso/chegadas, "D" para decolagem/saídas)
        - "natureza": ("D" para doméstico, "I" para internacional)
        - "intencao_carga": (booleano: `true` se a pergunta mencionar "carga" ou "cargas", `false` caso contrário)
        - "intencao_mais_movimentado": (booleano: `true` para "aeroporto mais movimentado", "maior tráfego", "mais passageiros" DE AEROPORTO, `false` caso contrário)
        - "intencao_mais_voos_internacionais": (booleano: `true` para "aeroporto com mais voos internacionais", `false` caso contrário)
        - "intencao_maior_operador_pax": (booleano: `true` para "empresa que mais transportou passageiros", "operador com mais passageiros", `false` caso contrário)
        - "intencao_maior_operador_carga": (booleano: `true` para "empresa que mais transportou cargas", "operador com mais cargas", `false` caso contrário)
        - "intencao_principal_destino": (booleano: `true` para "principal destino", "destino mais acessado", "local de destino", `false` caso contrário)
        - "intencao_maiores_atrasos": (booleano: `true` se a pergunta for sobre "maiores atrasos", "piores atrasos", "empresa mais atrasada", `false` caso contrário)
        - "intencao_market_share": (booleano: `true` se a pergunta for sobre "market share", "participação de mercado", "quais empresas operam", `false` caso contrário)
        - "intencao_historico_movimentacao": (booleano: `true` se a pergunta for sobre "histórico", "evolução", "gráfico da movimentação ao longo do tempo", `false` caso contrário)

        Prioridade de intenções: Se uma intenção de ranking for `true`, outros parâmetros podem ser `null`, a menos que o ano ou um aeroporto específico seja mencionado.

        Exemplos de saída JSON:
        - Pergunta: "Qual a evolução da quantidade de passageiros no Brasil?"
          Saída: {{"aeroporto": null, "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": true}}
        - Pergunta: "gráfico do histórico de cargas em Guarulhos"
          Saída: {{"aeroporto": "Guarulhos", "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": true, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": true}}
        - Pergunta: "Qual o destino mais acessado no Brasil?"
          Saída: {{"aeroporto": null, "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": true, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual o destino mais acessado no Brasil em 2022?"
          Saída: {{"aeroporto": null, "ano": 2022, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": true, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual foi o principal destino para o aeroporto de Brasília em 2024?"
          Saída: {{"aeroporto": "Brasília", "ano": 2024, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": true, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual o número de passageiros no aeroporto de Recife em 2023?"
          Saída: {{"aeroporto": "Recife", "ano": 2023, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual aeroporto mais movimentado do Brasil?"
          Saída: {{"aeroporto": null, "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": true, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual a empresa que mais transportou passageiros em 2024?"
          Saída: {{"aeroporto": null, "ano": 2024, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": true, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual o operador que mais transportou cargas em Brasília no último ano?"
          Saída: {{"aeroporto": "Brasília", "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": true, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": true, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Qual a empresa com maiores atrasos em Brasília?"
          Saída: {{"aeroporto": "Brasília", "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": true, "intencao_market_share": false, "intencao_historico_movimentacao": false}}
        - Pergunta: "Quais empresas operam no Brasil?"
          Saída: {{"aeroporto": null, "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false, "intencao_maior_operador_pax": false, "intencao_maior_operador_carga": false, "intencao_principal_destino": false, "intencao_maiores_atrasos": false, "intencao_market_share": true, "intencao_historico_movimentacao": false}}
        """},
        {"role": "user", "content": pergunta_usuario}
    ]

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=prompt_messages,
            response_format={"type": "json_object"},
            max_tokens=350,
            temperature=0.0
        )
        
        raw_response_text = completion.choices[0].message.content
        if not raw_response_text: return {}

        params = json.loads(raw_response_text.strip())

        if 'aeroporto' in params and params['aeroporto'] is not None:
            nome_ou_icao_extraido = params['aeroporto'].strip().lower()
            icao_code_mapeado = aeroporto_nome_para_icao.get(nome_ou_icao_extraido)
            params['aeroporto'] = icao_code_mapeado if icao_code_mapeado else nome_ou_icao_extraido.upper()
        
        for key, value in params.items():
            if isinstance(value, str) and value.lower() == 'null':
                params[key] = None

        valid_keys = {"aeroporto", "ano", "mes", "tipo_movimento", "natureza", "intencao_carga", 
                      "intencao_mais_movimentado", "intencao_mais_voos_internacionais",
                      "intencao_maior_operador_pax", "intencao_maior_operador_carga",
                      "intencao_principal_destino", "intencao_maiores_atrasos",
                      "intencao_market_share", "intencao_historico_movimentacao"}
                      
        for key in valid_keys:
             if key.startswith("intencao_") and (key not in params or not isinstance(params[key], bool)):
                params[key] = False

        return {k: v for k, v in params.items() if k in valid_keys}
    except Exception as e:
        print(f"DEBUG: Erro ao processar a pergunta com o LLM da OpenAI: {e}")
        return {} 