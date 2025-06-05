import os
import pandas as pd
import json
import duckdb
import openai
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result

# --- Configuração da API da OpenAI ---
try:
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    if not client.api_key:
        raise ValueError("OPENAI_API_KEY não configurada. Por favor, defina a variável de ambiente OPENAI_API_KEY.")
except Exception as e:
    # No contexto de um módulo, é melhor não dar exit() para permitir importação.
    # A mensagem de erro será tratada no app.py.
    client = None # Define client como None para indicar falha
    print(f"Erro ao configurar a API da OpenAI no chatbot_functions: {e}")

OPENAI_MODEL = "gpt-3.5-turbo" # Mantemos este modelo

# --- Dicionário de Mapeamento de Nomes de Aeroportos para Códigos ICAO ---
aeroporto_nome_para_icao = {
    "recife": "SBRF",
    "guarulhos": "SBGR",
    "congonhas": "SBSP",
    "santos dumont": "SBRJ",
    "galeão": "SBGL",
    "brasília": "SBBR",
    "salvador": "SBSV",
    "confins": "SBCF",
    "viracopos": "SBKP",
    "porto alegre": "SBPA",
    "manaus": "SBEG",
    "fortaleza": "SBFZ",
    "belém": "SBBE",
    "curitiba": "SBCT",
    "florianópolis": "SBFL",
    "natal": "SBSG",
    "maceió": "SBMO",
    "aracaju": "SBAR",
    "são luís": "SBSL",
    "teresina": "SBTE",
    "joão pessoa": "SBJP",
    "campina grande": "SBKG",
    "petrolina": "SBPL",
    "palmas": "SBPJ",
    "goiânia": "SBGO",
    "cuiabá": "SBCY",
    "campo grande": "SBCG",
    "porto velho": "SBPV",
    "rio branco": "SBRB",
    "macapá": "SBMQ",
    "boa vista": "SBBV",
    "foz do iguaçu": "SBFI",
    "são josé dos campos": "SBSJ",
    "marabá": "SBMA",
    "santarém": "SBSN",
    "imperatriz": "SBIZ",
    "juazeiro do norte": "SBJU",
    "ilhéus": "SBIL",
    "vitória da conquista": "SBVC",
    "teixeira de freitas": "SNTF",
    "barreiras": "SNBR",
    "lençóis": "SBLE",
    "jacobina": "SNJB",
    "valença": "SNVB",
    "una": "SBTC",
    "feira de santana": "SNJD",
    "itabuna": "SNIB",
    "jequié": "SNJK",
    "guanambi": "SNGI",
    "paulo afonso": "SBUF",
    "caravelas": "SBCV",
    "vitória": "SBVT",
    "são paulo": "SBSP",
    "rio de janeiro": "SBGL",
    "são carlos": "SDSC",
    "são josé do rio preto": "SBSR",
    "presidente prudente": "SBDN",
    "marília": "SBML",
    "ribeirão preto": "SBRP",
    "são josé dos campos": "SBSJ",
    "araraquara": "SBAQ",
    "bauru": "SBAE",
    "campinas": "SBKP",
    "sorocaba": "SDCO",
    "jundiaí": "SBJD",
    "são vicente": "SDNY",
    "ubatuba": "SDUB",
    "são sebastião": "SDSS",
    "caraguatatuba": "SDCG",
    "paraty": "SDTK",
    "angra dos reis": "SDAG",
    "resende": "SDRE",
    "volta redonda": "SDVR",
    "petropolis": "SDPE",
    "nova friburgo": "SDNF",
    "macuco": "SDMC",
    "cabo frio": "SBCB",
    "maricá": "SBMI",
    "niterói": "SDNT",
    "são gonçalo": "SDGO",
    "duque de caxias": "SDDC",
    "nilópolis": "SDNL",
    "nova iguaçu": "SDNI",
    "belford roxo": "SDBR",
    "mesquita": "SDME",
    "queimados": "SDQU",
    "japeri": "SDJP",
    "seropédica": "SDSE",
    "itaguaí": "SDIT",
    "mangaratiba": "SDMG",
    "paracambi": "SDPA",
    "valença": "SDVA",
    "vassouras": "SDVS",
    "barra do piraí": "SDBP",
    "piraí": "SDPI",
    "volta redonda": "SDVR",
    "resende": "SDRE",
    "angra dos reis": "SDAG",
    "paraty": "SDTK",
    "ubatuba": "SDUB",
    "caraguatatuba": "SDCG",
    "são sebastião": "SDSS",
    "são vicente": "SDNY",
    "santos": "SBST",
    "guarujá": "SDGU",
    "praia grande": "SDPG",
    "cubatão": "SDCB",
    "são paulo": "SBSP",
    "osasco": "SDOS",
    "barueri": "SDBA",
    "carapicuíba": "SDCA",
    "santana de parnaíba": "SDSP",
    "cotia": "SDCT",
    "taboão da serra": "SDTS",
    "embu das artes": "SDEA",
    "itapecerica da serra": "SDIS",
    "juquitiba": "SDJU",
    "são roque": "SDSR",
    "mairiporã": "SDMP",
    "franco da rocha": "SDFR",
    "caieiras": "SDCA",
    "guarulhos": "SBGR",
    "arujá": "SDAR",
    "itaquaquecetuba": "SDIT",
    "poá": "SDPO",
    "suzano": "SDSU",
    "mogi das cruzes": "SDMC",
    "biritiba mirim": "SDBM",
    "salesópolis": "SDSL",
    "santa isabel": "SDSI",
    "guararema": "SDGU",
    "jacareí": "SDJA",
    "são josé dos campos": "SBSJ",
    "taubaté": "SDTB",
    "pindamonhangaba": "SDPI",
    "guaratinguetá": "SDGU",
    "lorena": "SDLO",
    "aparecida": "SDAP",
    "cruzeiro": "SDCR",
    "cachoeira paulista": "SDCP",
    "são luís do paraitinga": "SDSL",
    "ubatuba": "SDUB",
    "caraguatatuba": "SDCG",
    "são sebastião": "SDSS",
    "ilhabela": "SDIL",
    "são paulo": "SBSP",
    "campinas": "SBKP",
    "são josé dos campos": "SBSJ",
    "sorocaba": "SDCO",
    "jundiaí": "SBJD",
    "piracicaba": "SDPC",
    "americana": "SDAM",
    "limeira": "SDLI",
    "rio claro": "SDRC",
    "araraquara": "SBAQ",
    "são carlos": "SDSC",
    "jaú": "SDJA",
    "bauru": "SBAE",
    "marília": "SBML",
    "presidente prudente": "SBDN",
    "assis": "SDAS",
    "ourinhos": "SDOU",
    "botucatu": "SDBO",
    "avare": "SDAV",
    "itapetininga": "SDIT",
    "tatui": "SDTA",
    "são roque": "SDSR",
    "são paulo": "SBSP",
    "guarulhos": "SBGR",
    "viracopos": "SBKP",
    "congonhas": "SBSP",
    "campo de marte": "SBMT",
    "santos": "SBST",
    "são josé dos campos": "SBSJ",
    "taubaté": "SDTB",
    "são josé do rio preto": "SBSR",
    "ribeirão preto": "SBRP",
    "araraquara": "SBAQ",
    "são carlos": "SDSC",
    "bauru": "SBAE",
    "marília": "SBML",
    "presidente prudente": "SBDN",
    "campinas": "SBKP",
    "sorocaba": "SDCO",
    "jundiaí": "SBJD",
    "piracicaba": "SDPC",
    "americana": "SDAM",
    "limeira": "SDLI",
    "rio claro": "SDRC",
    "jaú": "SDJA",
    "assis": "SDAS",
    "ourinhos": "SDOU",
    "botucatu": "SDBO",
    "avare": "SDAV",
    "itapetininga": "SDIT",
    "tatui": "SDTA",
    "são roque": "SDSR",
    "são paulo": "SBSP",
    "guarulhos": "SBGR",
    "viracopos": "SBKP",
    "congonhas": "SBSP",
    "campo de marte": "SBMT",
    "santos": "SBST",
    "são josé dos campos": "SBSJ",
    "taubaté": "SDTB",
    "são josé do rio preto": "SBSR",
}

# --- Dicionário de Mapeamento de Números de Mês para Nomes ---
mes_numero_para_nome = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril",
    5: "maio", 6: "junho", 7: "julho", 8: "agosto",
    9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
}

# --- Função para Formatação Manual de Números para o Padrão Brasileiro ---
def formatar_numero_br(valor):
    return f"{int(valor):,}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- Função para obter o último ano disponível nos dados ---
def obter_ultimo_ano_disponivel(pasta_parquet):
    # ... (código existente da função) ...
    if not os.path.exists(pasta_parquet):
        return None
    
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        return None
    
    con = duckdb.connect(database=':memory:', read_only=False)
    try:
        query = f"SELECT MAX(ANO) FROM read_parquet({arquivos_parquet})"
        max_ano = con.execute(query).fetchone()[0]
        return max_ano
    except Exception as e:
        print(f"DEBUG: Erro ao obter o último ano disponível: {e}")
        return None
    finally:
        con.close()

# --- Função de Consulta DuckDB (inalterada) ---
def consultar_movimentacoes_aeroportuarias(pasta_parquet, aeroporto=None, ano=None, mes=None, tipo_movimento=None, natureza=None, tipo_consulta="passageiros"):
    # ... (código existente da função) ...
    if not os.path.exists(pasta_parquet):
        return pd.DataFrame()

    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]

    if not arquivos_parquet:
        con.close()
        return pd.DataFrame()

    condicoes = []
    if aeroporto:
        condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    if ano:
        condicoes.append(f"ANO = {ano}")
    if mes:
        condicoes.append(f"MES = {mes}")
    if tipo_movimento:
        condicoes.append(f"NR_MOVIMENTO_TIPO = '{tipo_movimento.upper()}'")
    if natureza:
        condicoes.append(f"NR_NATUREZA = '{natureza.upper()}'")

    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""

    select_clause = ""
    if tipo_consulta == "passageiros":
        select_clause = "SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalValor"
    elif tipo_consulta == "carga":
        select_clause = "SUM(QT_CARGA) AS TotalValor"
    else:
        select_clause = "SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalValor"

    query = f"""
    SELECT
        {select_clause}
    FROM read_parquet({arquivos_parquet})
    {where_clause}
    """
    
    try:
        resultado = con.execute(query).fetchdf()
        return resultado
    except Exception as e:
        print(f"DEBUG: Erro ao executar a consulta DuckDB: {e}")
        return pd.DataFrame()
    finally:
        con.close()

# --- NOVA FUNÇÃO: Obter Aeroporto Mais Movimentado ---
def obter_aeroporto_mais_movimentado(pasta_parquet, ano=None):
    # ... (código existente da função) ...
    if not os.path.exists(pasta_parquet):
        return None

    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None:
            return None

    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]

    if not arquivos_parquet:
        con.close()
        return None

    query = f"""
    SELECT
        NR_AEROPORTO_REFERENCIA,
        SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalPassageiros
    FROM read_parquet({arquivos_parquet})
    WHERE ANO = {ano}
    GROUP BY NR_AEROPORTO_REFERENCIA
    ORDER BY TotalPassageiros DESC
    LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {
                "aeroporto": resultado['NR_AEROPORTO_REFERENCIA'].iloc[0],
                "total_passageiros": int(resultado['TotalPassageiros'].iloc[0]),
                "ano": ano
            }
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter aeroporto mais movimentado: {e}")
        return None
    finally:
        con.close()

# --- NOVA FUNÇÃO: Obter Aeroporto com Mais Voos Internacionais ---
def obter_aeroporto_mais_voos_internacionais(pasta_parquet, ano=None):
    # ... (código existente da função) ...
    if not os.path.exists(pasta_parquet):
        return None

    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None:
            return None

    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]

    if not arquivos_parquet:
        con.close()
        return None

    query = f"""
    SELECT
        NR_AEROPORTO_REFERENCIA,
        COUNT(*) AS TotalVoosInternacionais
    FROM read_parquet({arquivos_parquet})
    WHERE ANO = {ano} AND NR_NATUREZA = 'I'
    GROUP BY NR_AEROPORTO_REFERENCIA
    ORDER BY TotalVoosInternacionais DESC
    LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {
                "aeroporto": resultado['NR_AEROPORTO_REFERENCIA'].iloc[0],
                "total_voos": int(resultado['TotalVoosInternacionais'].iloc[0]),
                "ano": ano
            }
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter aeroporto com mais voos internacionais: {e}")
        return None
    finally:
        con.close()


# --- Função de Parsing da Pergunta do Usuário com LLM (OpenAI - Aprimorada para Novas Intenções) ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_result(lambda result: not result))
def parse_pergunta_com_llm(pergunta_usuario):
    # ... (código existente da função) ...
    prompt_messages = [
        {"role": "system", "content": f"""Você é um assistente especializado em extrair informações de perguntas sobre movimentações aeroportuárias.
        Sua única saída deve ser um objeto JSON.
        Extraia os seguintes parâmetros da pergunta do usuário. Se um parâmetro não for mencionado, seu valor deve ser `null`.
        Além disso, identifique intenções específicas para perguntas sobre rankings.

        Parâmetros a extrair:
        - "aeroporto": (nome comum do aeroporto, ou código ICAO se já estiver presente, ex: "Recife", "Congonhas", "SBRF")
        - "ano": (número inteiro)
        - "mes": (número inteiro de 1 a 12, para meses como "janeiro" -> 1)
        - "tipo_movimento": ("P" para pouso/chegadas, "D" para decolagem/saídas)
        - "natureza": ("D" para doméstico, "I" para internacional)
        - "intencao_carga": (booleano: `true` se a pergunta mencionar "carga" ou "cargas", `false` caso contrário)
        - "intencao_mais_movimentado": (booleano: `true` se a pergunta for sobre o "aeroporto mais movimentado do Brasil", "maior tráfego", "mais passageiros", `false` caso contrário)
        - "intencao_mais_voos_internacionais": (booleano: `true` se a pergunta for sobre o "aeroporto com mais voos internacionais", `false` caso contrário)

        Prioridade de intenções: Se houver "mais movimentado" ou "mais voos internacionais", os outros parâmetros (aeroporto, mes, tipo_movimento, natureza, intencao_carga) podem ser null, a menos que o ano seja especificado.

        Exemplos de saída JSON:
        - Pergunta: "Qual o número de passageiros no aeroporto de Recife em 2023?"
          Saída: {{"aeroporto": "Recife", "ano": 2023, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": false}}
        - Pergunta: "Qual aeroporto mais movimentado do Brasil?"
          Saída: {{"aeroporto": null, "ano": null, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": true, "intencao_mais_voos_internacionais": false}}
        - Pergunta: "Aeroporto com maior tráfego em 2024?"
          Saída: {{"aeroporto": null, "ano": 2024, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": true, "intencao_mais_voos_internacionais": false}}
        - Pergunta: "Qual aeroporto com mais voos internacionais em 2023?"
          Saída: {{"aeroporto": null, "ano": 2023, "mes": null, "tipo_movimento": null, "natureza": null, "intencao_carga": false, "intencao_mais_movimentado": false, "intencao_mais_voos_internacionais": true}}
        """},
        {"role": "user", "content": pergunta_usuario}
    ]

    raw_response_text = ""
    json_text = ""
    params = {}

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=prompt_messages,
            response_format={"type": "json_object"},
            max_tokens=200,
            temperature=0.0
        )
        
        raw_response_text = completion.choices[0].message.content

        if not raw_response_text or raw_response_text.strip() == "":
            print(f"DEBUG: Tentativa {parse_pergunta_com_llm.retry.statistics['attempt_number'] if 'retry' in parse_pergunta_com_llm.__dict__ else 1}: O LLM da OpenAI retornou uma resposta vazia. Reintentando...")
            return {}

        json_text = raw_response_text.strip()
        json_text = json_text.replace('```json', '').replace('```', '').strip()
        
        params = json.loads(json_text)

        # Lógica de Mapeamento de Aeroportos
        if 'aeroporto' in params and params['aeroporto'] is not None:
            nome_ou_icao_extraido = params['aeroporto'].strip()
            nome_normalizado_para_lookup = nome_ou_icao_extraido.lower()
            
            icao_code_mapeado = aeroporto_nome_para_icao.get(nome_normalizado_para_lookup)
            
            if icao_code_mapeado:
                params['aeroporto'] = icao_code_mapeado
            else:
                params['aeroporto'] = nome_ou_icao_extraido.upper()
        
        for key, value in params.items():
            if isinstance(value, str) and value.lower() == 'null':
                params[key] = None

        # Garante que as novas intenções são booleanos
        if 'intencao_carga' not in params or not isinstance(params['intencao_carga'], bool):
            params['intencao_carga'] = False
        if 'intencao_mais_movimentado' not in params or not isinstance(params['intencao_mais_movimentado'], bool):
            params['intencao_mais_movimentado'] = False
        if 'intencao_mais_voos_internacionais' not in params or not isinstance(params['intencao_mais_voos_internacionais'], bool):
            params['intencao_mais_voos_internacionais'] = False
        
        valid_keys = {"aeroporto", "ano", "mes", "tipo_movimento", "natureza", "intencao_carga", "intencao_mais_movimentado", "intencao_mais_voos_internacionais"}
        parsed_params = {k: v for k, v in params.items() if k in valid_keys}

        return parsed_params
    except json.JSONDecodeError as e:
        print(f"DEBUG: Erro ao decodificar JSON do LLM da OpenAI (json.JSONDecodeError).")
        print(f"DEBUG: Resposta do LLM (RAW): '{raw_response_text}'")
        print(f"DEBUG: Resposta do LLM (Processada para JSON): '{json_text}'")
        print(f"DEBUG: Erro detalhado: {e}")
        return {}
    except Exception as e:
        print(f"DEBUG: Erro INESPERADO ao processar a pergunta com o LLM da OpenAI (Exception).")
        print(f"DEBUG: Resposta do LLM (RAW): '{raw_response_text}'")
        print(f"DEBUG: Resposta do LLM (Processada para JSON): '{json_text}'")
        print(f"DEBUG: Erro detalhado: {e}")
        return {}

# --- Função Principal do Chatbot (Aprimorada para novas perguntas) ---
def chatbot():
    """
    Inicia o loop do chatbot para interagir com o usuário.
    Agora responde a perguntas sobre o aeroporto mais movimentado e com mais voos internacionais.
    """
    pasta_arquivos_parquet = 'dados_aeroportuarios_parquet'
    if not os.path.exists(pasta_arquivos_parquet) or not os.listdir(pasta_arquivos_parquet):
        print("Atenção: A pasta 'dados_aeroportuarios_parquet' não foi encontrada ou está vazia.")
        print("Por favor, execute o script 'conversor_json_parquet.py' primeiro para gerar os arquivos Parquet.")
        return

    print("Olá! Sou seu parceiro de programação para dados de movimentações aeroportuárias.")
    print("Posso responder a perguntas como 'Qual o volume de passageiros que chegaram em Recife em janeiro de 2024?', 'Total de carga em Guarulhos em 2024?', 'Qual o aeroporto mais movimentado do Brasil?' ou 'Qual aeroporto com mais voos internacionais?'.")
    print("Digite 'sair' para encerrar.")

    while True:
        pergunta = input("\nVocê: ")
        if pergunta.lower() == 'sair':
            print("Chatbot encerrado. Até mais!")
            break

        print("Pensando...")
        parametros = parse_pergunta_com_llm(pergunta)

        # --- Tratamento de Entidades e Novas Intenções ---
        feedback_usuario = []
        # Validar que pelo menos uma intenção ou conjunto de filtros básicos foi extraído
        if not parametros or not (
            any(parametros.get(k) for k in ["aeroporto", "ano", "mes", "tipo_movimento", "natureza", "intencao_carga"]) or
            parametros.get('intencao_mais_movimentado') or
            parametros.get('intencao_mais_voos_internacionais')
        ):
            feedback_usuario.append("Não consegui extrair informações relevantes da sua pergunta.")
        else:
            # Garante que flags de intenção são booleanos válidos
            for key in ["intencao_carga", "intencao_mais_movimentado", "intencao_mais_voos_internacionais"]:
                if key not in parametros or not isinstance(parametros[key], bool):
                    parametros[key] = False

            # Se não for uma das novas perguntas de ranking, exige aeroporto e ano para perguntas detalhadas
            if not (parametros['intencao_mais_movimentado'] or parametros['intencao_mais_voos_internacionais']):
                if not parametros.get('aeroporto'):
                    feedback_usuario.append("Não identifiquei o aeroporto. Poderia especificar o nome ou código ICAO?")
                if not parametros.get('ano'):
                    # Se o ano não for especificado, usamos o ano atual/último disponível para perguntas gerais.
                    # Mas para perguntas detalhadas de aeroporto/mês, ainda é bom pedir o ano.
                    if not (parametros.get('aeroporto') and parametros.get('mes')):
                        feedback_usuario.append("Não identifiquei o ano. Poderia especificar o ano da movimentação?")
                
        if feedback_usuario:
            print(f"Desculpe. {' '.join(feedback_usuario)} Por favor, tente novamente de forma mais clara.")
            continue
        # Fim do Tratamento de Entidades

        # Convertendo mês para int se não for null
        if 'mes' in parametros and parametros['mes'] is not None:
            try:
                parametros['mes'] = int(parametros['mes'])
            except (ValueError, TypeError):
                parametros['mes'] = None
        
        # --- Lógica para Responder às Novas Perguntas de Ranking ---
        if parametros['intencao_mais_movimentado']:
            ano_referencia = parametros.get('ano') # Pode ser nulo, a função lidará com isso
            resultado_ranking = obter_aeroporto_mais_movimentado(pasta_arquivos_parquet, ano=ano_referencia)
            if resultado_ranking:
                aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                total_passageiros_formatado = formatar_numero_br(resultado_ranking['total_passageiros'])
                print(f"No ano de {resultado_ranking['ano']}, o aeroporto mais movimentado do Brasil foi **{aeroporto_nome.title()}**, com um total de **{total_passageiros_formatado}** passageiros.")
            else:
                print(f"Não foi possível determinar o aeroporto mais movimentado. Verifique os dados para o ano {ano_referencia if ano_referencia else 'disponível'}.")
            continue

        if parametros['intencao_mais_voos_internacionais']:
            ano_referencia = parametros.get('ano') # Pode ser nulo
            resultado_ranking = obter_aeroporto_mais_voos_internacionais(pasta_arquivos_parquet, ano=ano_referencia)
            if resultado_ranking:
                aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == resultado_ranking['aeroporto'].upper()), resultado_ranking['aeroporto'].upper())
                total_voos_formatado = formatar_numero_br(resultado_ranking['total_voos'])
                print(f"No ano de {resultado_ranking['ano']}, o aeroporto com mais voos internacionais foi **{aeroporto_nome.title()}**, com **{total_voos_formatado}** voos internacionais registrados.")
            else:
                print(f"Não foi possível determinar o aeroporto com mais voos internacionais. Verifique os dados para o ano {ano_referencia if ano_referencia else 'disponível'}.")
            continue
        # Fim da Lógica para Novas Perguntas de Ranking

        # --- Lógica de Presunção de Passageiros/Carga (existente) ---
        tipo_consulta_db = "passageiros"
        if parametros.get('intencao_carga'):
            tipo_consulta_db = "carga"
        # Fim da Lógica de Presunção

        resultados_df = consultar_movimentacoes_aeroportuarias(
            pasta_arquivos_parquet,
            aeroporto=parametros.get('aeroporto'),
            ano=parametros.get('ano'),
            mes=parametros.get('mes'),
            tipo_movimento=parametros.get('tipo_movimento'),
            natureza=parametros.get('natureza'),
            tipo_consulta=tipo_consulta_db
        )

        if resultados_df is not None and not resultados_df.empty:
            total_valor = resultados_df['TotalValor'].iloc[0]

            # --- Resposta Semântica Aprimorada (existente) ---
            resposta_semantica = f"No " 
            
            if parametros.get('mes'):
                nome_do_mes = mes_numero_para_nome.get(parametros['mes'], str(parametros['mes']))
                resposta_semantica += f"mês de {nome_do_mes.capitalize()} de "

            # Adiciona o ano se ele existir
            if parametros.get('ano'):
                resposta_semantica += f"{parametros['ano']}"
                # Adiciona vírgula se houver mais detalhes depois do ano
                if parametros.get('aeroporto') or parametros.get('mes'):
                    resposta_semantica += ", "
                else:
                    resposta_semantica += " " # Se não tem mais detalhes, só um espaço

            # Adiciona o aeroporto se ele existir
            if parametros.get('aeroporto'):
                aeroporto_nome = next((nome for nome, icao in aeroporto_nome_para_icao.items() if icao == parametros['aeroporto'].upper()), parametros['aeroporto'].upper())
                # Se não tem mês nem ano, mas tem aeroporto, ajusta a frase
                if not parametros.get('mes') and not parametros.get('ano'):
                     resposta_semantica = f"O aeroporto de {aeroporto_nome.title()} "
                else:
                    resposta_semantica += f"o aeroporto de {aeroporto_nome.title()} "
            
            # Formatar o número para padrão brasileiro manualmente
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
            # Pequena limpeza final de espaços e vírgulas duplicadas
            print(resposta_semantica.replace(", ,", ", ").replace("  ", " ").strip().replace(" .", "."))

        else:
            # --- Feedback aprimorado se a consulta ao DB retornar vazio ---
            print("Não foram encontrados dados com os critérios especificados.")
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
                print(f"Para os critérios {', '.join(criterios)}:")
                print("  Verifique se os dados existem para esta combinação ou tente critérios de pesquisa mais amplos.")
            else:
                print("Por favor, tente uma pergunta diferente ou especifique mais detalhes.")

# --- Executa o Chatbot ---
if __name__ == "__main__":
    chatbot()