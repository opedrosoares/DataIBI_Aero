import os
from openai import OpenAI

# --- Configuração da API da OpenAI ---
try:
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    if not client.api_key:
        raise ValueError("OPENAI_API_KEY não configurada. Por favor, defina a variável de ambiente OPENAI_API_KEY.")
except Exception as e:
    client = None 
    print(f"Erro ao configurar a API da OpenAI no chatbot_functions: {e}")

OPENAI_MODEL = "gpt-3.5-turbo"

# --- Dicionários de Mapeamento ---
aeroporto_nome_para_icao = {
    "recife": "SBRF", "guarulhos": "SBGR", "congonhas": "SBSP", "santos dumont": "SBRJ", "galeão": "SBGL", "brasília": "SBBR", "salvador": "SBSV", "confins": "SBCF", "viracopos": "SBKP", "porto alegre": "SBPA", "manaus": "SBEG", "fortaleza": "SBFZ", "belém": "SBBE", "curitiba": "SBCT", "florianópolis": "SBFL", "natal": "SBSG", "maceió": "SBMO", "aracaju": "SBAR", "são luís": "SBSL", "teresina": "SBTE", "joão pessoa": "SBJP", "campina grande": "SBKG", "petrolina": "SBPL", "palmas": "SBPJ", "goiânia": "SBGO", "cuiabá": "SBCY", "campo grande": "SBCG", "porto velho": "SBPV", "rio branco": "SBRB", "macapá": "SBMQ", "boa vista": "SBBV", "foz do iguaçu": "SBFI", "são josé dos campos": "SBSJ", "marabá": "SBMA", "santarém": "SBSN", "imperatriz": "SBIZ", "juazeiro do norte": "SBJU", "ilhéus": "SBIL", "vitória da conquista": "SBVC", "teixeira de freitas": "SNTF", "barreiras": "SNBR", "lençóis": "SBLE", "jacobina": "SNJB", "valença": "SNVB", "una": "SBTC", "feira de santana": "SNJD", "itabuna": "SNIB", "jequié": "SNJK", "guanambi": "SNGI", "paulo afonso": "SBUF", "caravelas": "SBCV", "vitória": "SBVT", "são paulo": "SBSP", "rio de janeiro": "SBGL", "são carlos": "SDSC", "são josé do rio preto": "SBSR", "presidente prudente": "SBDN", "marília": "SBML", "ribeirão preto": "SBRP", "araraquara": "SBAQ", "bauru": "SBAE", "campinas": "SBKP", "sorocaba": "SDCO", "jundiaí": "SBJD", "são vicente": "SDNY", "ubatuba": "SDUB", "são sebastião": "SDSS", "caraguatatuba": "SDCG", "paraty": "SDTK", "angra dos reis": "SDAG", "resende": "SDRE", "volta redonda": "SDVR", "petropolis": "SDPE", "nova friburgo": "SDNF", "macuco": "SDMC", "cabo frio": "SBCB", "maricá": "SBMI", "niterói": "SDNT", "são gonçalo": "SDGO", "duque de caxias": "SDDC", "nilópolis": "SDNL", "nova iguaçu": "SDNI", "belford roxo": "SDBR", "mesquita": "SDME", "queimados": "SDQU", "japeri": "SDJP", "seropédica": "SDSE", "itaguaí": "SDIT", "mangaratiba": "SDMG", "paracambi": "SDPA", "vassouras": "SDVS", "barra do piraí": "SDBP", "piraí": "SDPI", "santos": "SBST", "guarujá": "SDGU", "praia grande": "SDPG", "cubatão": "SDCB", "osasco": "SDOS", "barueri": "SDBA", "carapicuíba": "SDCA", "santana de parnaíba": "SDSP", "cotia": "SDCT", "taboão da serra": "SDTS", "embu das artes": "SDEA", "itapecerica da serra": "SDIS", "juquitiba": "SDJU", "são roque": "SDSR", "mairiporã": "SDMP", "franco da rocha": "SDFR", "caieiras": "SDCA", "arujá": "SDAR", "itaquaquecetuba": "SDIT", "poá": "SDPO", "suzano": "SDSU", "mogi das cruzes": "SDMC", "biritiba mirim": "SDBM", "salesópolis": "SDSL", "santa isabel": "SDSI", "guararema": "SDGU", "jacareí": "SDJA", "taubaté": "SDTB", "pindamonhangaba": "SDPI", "guaratinguetá": "SDGU", "lorena": "SDLO", "aparecida": "SDAP", "cruzeiro": "SDCR", "cachoeira paulista": "SDCP", "são luís do paraitinga": "SDSL", "ilhabela": "SDIL", "piracicaba": "SDPC", "americana": "SDAM", "limeira": "SDLI", "rio claro": "SDRC", "jaú": "SDJA", "assis": "SDAS", "ourinhos": "SDOU", "botucatu": "SDBO", "avare": "SDAV", "itapetininga": "SDIT", "tatui": "SDTA", "campo de marte": "SBMT"
}

operador_icao_para_nome = {
    "ABJ": "Abaeté", "ACN": "Azul Conecta", "AEB": "Avion Express Brasil", "AFR": "Air France", "ASO": "Aerosul", "AZU": "Azul", "BPC": "Braspress", "BRS": "Força Aérea Brasileira", "DLH": "Lufthansa", "DUX": "Dux Express", "GLO": "Gol", "KLM": "KLM", "LTG": "LATAM Cargo", "MWM": "Modern Logistics", "OMI": "Omni Táxi Aéreo", "PAM": "MAP Linhas Aéreas", "PTB": "Voepass", "RIM": "Rima Táxi Aéreo", "SID": "Sideral", "SUL": "ASTA", "TAM": "LATAM Brasil", "TAP": "TAP Air Portugal", "TOT": "Total Express", "TTL": "Total"
}

mes_numero_para_nome = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril", 5: "maio", 6: "junho", 7: "julho", 8: "agosto", 9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
} 