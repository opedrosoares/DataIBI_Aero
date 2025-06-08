import os
import pandas as pd
import json
import duckdb
import io
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from datetime import datetime, timedelta


# --- Configuração da API da OpenAI ---
try:
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    if not client.api_key:
        raise ValueError("OPENAI_API_KEY não configurada. Por favor, defina a variável de ambiente OPENAI_API_KEY.")
except Exception as e:
    client = None 
    print(f"Erro ao configurar a API da OpenAI no chatbot_functions: {e}")

OPENAI_MODEL = "gpt-3.5-turbo"

# --- Dicionários de Mapeamento (sem alterações) ---
aeroporto_nome_para_icao = {
    "recife": "SBRF", "guarulhos": "SBGR", "congonhas": "SBSP", "santos dumont": "SBRJ", "galeão": "SBGL", "brasília": "SBBR", "salvador": "SBSV", "confins": "SBCF", "viracopos": "SBKP", "porto alegre": "SBPA", "manaus": "SBEG", "fortaleza": "SBFZ", "belém": "SBBE", "curitiba": "SBCT", "florianópolis": "SBFL", "natal": "SBSG", "maceió": "SBMO", "aracaju": "SBAR", "são luís": "SBSL", "teresina": "SBTE", "joão pessoa": "SBJP", "campina grande": "SBKG", "petrolina": "SBPL", "palmas": "SBPJ", "goiânia": "SBGO", "cuiabá": "SBCY", "campo grande": "SBCG", "porto velho": "SBPV", "rio branco": "SBRB", "macapá": "SBMQ", "boa vista": "SBBV", "foz do iguaçu": "SBFI", "são josé dos campos": "SBSJ", "marabá": "SBMA", "santarém": "SBSN", "imperatriz": "SBIZ", "juazeiro do norte": "SBJU", "ilhéus": "SBIL", "vitória da conquista": "SBVC", "teixeira de freitas": "SNTF", "barreiras": "SNBR", "lençóis": "SBLE", "jacobina": "SNJB", "valença": "SNVB", "una": "SBTC", "feira de santana": "SNJD", "itabuna": "SNIB", "jequié": "SNJK", "guanambi": "SNGI", "paulo afonso": "SBUF", "caravelas": "SBCV", "vitória": "SBVT", "são paulo": "SBSP", "rio de janeiro": "SBGL", "são carlos": "SDSC", "são josé do rio preto": "SBSR", "presidente prudente": "SBDN", "marília": "SBML", "ribeirão preto": "SBRP", "araraquara": "SBAQ", "bauru": "SBAE", "campinas": "SBKP", "sorocaba": "SDCO", "jundiaí": "SBJD", "são vicente": "SDNY", "ubatuba": "SDUB", "são sebastião": "SDSS", "caraguatatuba": "SDCG", "paraty": "SDTK", "angra dos reis": "SDAG", "resende": "SDRE", "volta redonda": "SDVR", "petropolis": "SDPE", "nova friburgo": "SDNF", "macuco": "SDMC", "cabo frio": "SBCB", "maricá": "SBMI", "niterói": "SDNT", "são gonçalo": "SDGO", "duque de caxias": "SDDC", "nilópolis": "SDNL", "nova iguaçu": "SDNI", "belford roxo": "SDBR", "mesquita": "SDME", "queimados": "SDQU", "japeri": "SDJP", "seropédica": "SDSE", "itaguaí": "SDIT", "mangaratiba": "SDMG", "paracambi": "SDPA", "vassouras": "SDVS", "barra do piraí": "SDBP", "piraí": "SDPI", "santos": "SBST", "guarujá": "SDGU", "praia grande": "SDPG", "cubatão": "SDCB", "osasco": "SDOS", "barueri": "SDBA", "carapicuíba": "SDCA", "santana de parnaíba": "SDSP", "cotia": "SDCT", "taboão da serra": "SDTS", "embu das artes": "SDEA", "itapecerica da serra": "SDIS", "juquitiba": "SDJU", "são roque": "SDSR", "mairiporã": "SDMP", "franco da rocha": "SDFR", "caieiras": "SDCA", "arujá": "SDAR", "itaquaquecetuba": "SDIT", "poá": "SDPO", "suzano": "SDSU", "mogi das cruzes": "SDMC", "biritiba mirim": "SDBM", "salesópolis": "SDSL", "santa isabel": "SDSI", "guararema": "SDGU", "jacareí": "SDJA", "taubaté": "SDTB", "pindamonhangaba": "SDPI", "guaratinguetá": "SDGU", "lorena": "SDLO", "aparecida": "SDAP", "cruzeiro": "SDCR", "cachoeira paulista": "SDCP", "são luís do paraitinga": "SDSL", "ilhabela": "SDIL", "piracicaba": "SDPC", "americana": "SDAM", "limeira": "SDLI", "rio claro": "SDRC", "jaú": "SDJA", "assis": "SDAS", "ourinhos": "SDOU", "botucatu": "SDBO", "avare": "SDAV", "itapetininga": "SDIT", "tatui": "SDTA", "campo de marte": "SBMT"
}
operador_icao_para_nome = {
    "ABJ": "Abaeté", "ACN": "Azul Conecta", "AEB": "Avion Express Brasil", "AFR": "Air France", "ASO": "Aerosul", "AZU": "Azul", "BPC": "Braspress", "BRS": "Força Aérea Brasileira", "DLH": "Lufthansa", "DUX": "Dux Express", "GLO": "Gol", "KLM": "KLM", "LTG": "LATAM Cargo", "MWM": "Modern Logistics", "OMI": "Omni Táxi Aéreo", "PAM": "MAP Linhas Aéreas", "PTB": "Voepass", "RIM": "Rima Táxi Aéreo", "SID": "Sideral", "SUL": "ASTA", "TAM": "LATAM Brasil", "TAP": "TAP Air Portugal", "TOT": "Total Express", "TTL": "Total"
}
mes_numero_para_nome = {
    1: "janeiro", 2: "fevereiro", 3: "março", 4: "abril", 5: "maio", 6: "junho", 7: "julho", 8: "agosto", 9: "setembro", 10: "outubro", 11: "novembro", 12: "dezembro"
}

# --- Funções de Utilitários e Consulta ---
def formatar_numero_br(valor):
    return f"{int(valor):,}".replace(",", "X").replace(".", ",").replace("X", ".")

def obter_ultimo_ano_disponivel(pasta_parquet):
    if not os.path.exists(pasta_parquet): return None
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    try:
        return con.execute(f"SELECT MAX(ANO) FROM read_parquet({arquivos_parquet})").fetchone()[0]
    except Exception as e:
        print(f"DEBUG: Erro ao obter o último ano disponível: {e}")
        return None
    finally:
        con.close()

def consultar_movimentacoes_aeroportuarias(pasta_parquet, aeroporto=None, ano=None, mes=None, tipo_movimento=None, natureza=None, tipo_consulta="passageiros"):
    if not os.path.exists(pasta_parquet): return pd.DataFrame()
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return pd.DataFrame()
    condicoes = []
    if aeroporto: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    if ano: condicoes.append(f"ANO = {ano}")
    if mes: condicoes.append(f"MES = {mes}")
    if tipo_movimento: condicoes.append(f"NR_MOVIMENTO_TIPO = '{tipo_movimento.upper()}'")
    if natureza: condicoes.append(f"NR_NATUREZA = '{natureza.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    if tipo_consulta == "passageiros":
        select_clause = "SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalValor"
    elif tipo_consulta == "carga":
        select_clause = "SUM(QT_CARGA) AS TotalValor"
    else:
        select_clause = "SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalValor"
    query = f"SELECT {select_clause} FROM read_parquet({arquivos_parquet}) {where_clause}"
    try:
        return con.execute(query).fetchdf()
    except Exception as e:
        print(f"DEBUG: Erro ao executar a consulta DuckDB: {e}")
        return pd.DataFrame()
    finally:
        con.close()

# --- Funções de Ranking ---
def obter_aeroporto_mais_movimentado(pasta_parquet, ano=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    query = f"""
    SELECT NR_AEROPORTO_REFERENCIA, SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalPassageiros
    FROM read_parquet({arquivos_parquet}) WHERE ANO = {ano}
    GROUP BY NR_AEROPORTO_REFERENCIA ORDER BY TotalPassageiros DESC LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"aeroporto": resultado['NR_AEROPORTO_REFERENCIA'].iloc[0], "total_passageiros": int(resultado['TotalPassageiros'].iloc[0]), "ano": ano}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter aeroporto mais movimentado: {e}")
        return None
    finally:
        con.close()

def obter_aeroporto_mais_voos_internacionais(pasta_parquet, ano=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    query = f"""
    SELECT NR_AEROPORTO_REFERENCIA, COUNT(*) AS TotalVoosInternacionais
    FROM read_parquet({arquivos_parquet}) WHERE ANO = {ano} AND NR_NATUREZA = 'I'
    GROUP BY NR_AEROPORTO_REFERENCIA ORDER BY TotalVoosInternacionais DESC LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"aeroporto": resultado['NR_AEROPORTO_REFERENCIA'].iloc[0], "total_voos": int(resultado['TotalVoosInternacionais'].iloc[0]), "ano": ano}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter aeroporto com mais voos internacionais: {e}")
        return None
    finally:
        con.close()

def obter_operador_mais_passageiros(pasta_parquet, ano=None, aeroporto=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = [f"ANO = {ano}"]
    if aeroporto: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    query = f"""
    SELECT NR_AERONAVE_OPERADOR, SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalPassageirosOperador
    FROM read_parquet({arquivos_parquet}) {where_clause}
    GROUP BY NR_AERONAVE_OPERADOR ORDER BY TotalPassageirosOperador DESC LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"operador": resultado['NR_AERONAVE_OPERADOR'].iloc[0], "total_passageiros": int(resultado['TotalPassageirosOperador'].iloc[0]), "ano": ano, "aeroporto": aeroporto}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter operador com mais passageiros: {e}")
        return None
    finally:
        con.close()

def obter_operador_mais_cargas(pasta_parquet, ano=None, aeroporto=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = [f"ANO = {ano}"]
    if aeroporto: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    query = f"""
    SELECT NR_AERONAVE_OPERADOR, SUM(QT_CARGA) AS TotalCargasOperador
    FROM read_parquet({arquivos_parquet}) {where_clause}
    GROUP BY NR_AERONAVE_OPERADOR ORDER BY TotalCargasOperador DESC LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"operador": resultado['NR_AERONAVE_OPERADOR'].iloc[0], "total_cargas": int(resultado['TotalCargasOperador'].iloc[0]), "ano": ano, "aeroporto": aeroporto}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter operador com mais cargas: {e}")
        return None
    finally:
        con.close()

def obter_principal_destino(pasta_parquet, aeroporto_origem=None, ano=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = [f"ANO = {ano}"]
    if aeroporto_origem: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto_origem.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    query = f"""
    SELECT NR_VOO_OUTRO_AEROPORTO, COUNT(*) AS TotalVoos
    FROM read_parquet({arquivos_parquet}) {where_clause}
    GROUP BY NR_VOO_OUTRO_AEROPORTO ORDER BY TotalVoos DESC LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"destino_icao": resultado['NR_VOO_OUTRO_AEROPORTO'].iloc[0], "total_voos": int(resultado['TotalVoos'].iloc[0]), "ano": ano, "aeroporto_origem": aeroporto_origem}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter principal destino: {e}")
        return None
    finally:
        con.close()

def obter_operador_maiores_atrasos(pasta_parquet, ano=None, aeroporto=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = [f"ANO = {ano}", "NR_MOVIMENTO_TIPO = 'P'", "NR_AERONAVE_OPERADOR != 'GERAL'"]
    if aeroporto: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    query = f"""
    WITH AtrasosCalculados AS (
        SELECT
            NR_AERONAVE_OPERADOR,
            (HH_CALCO.TotalMinutes - HH_PREVISTO.TotalMinutes) AS MinutosAtrasoBrutos
        FROM read_parquet({arquivos_parquet})
        {where_clause}
    )
    SELECT
        NR_AERONAVE_OPERADOR,
        SUM(CASE WHEN MinutosAtrasoBrutos > 0 THEN MinutosAtrasoBrutos ELSE 0 END) AS TotalMinutosAtraso
    FROM AtrasosCalculados
    GROUP BY NR_AERONAVE_OPERADOR
    ORDER BY TotalMinutosAtraso DESC
    LIMIT 1
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return {"operador": resultado['NR_AERONAVE_OPERADOR'].iloc[0], "total_minutos_atraso": int(resultado['TotalMinutosAtraso'].iloc[0]), "ano": ano, "aeroporto": aeroporto}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao obter operador com maiores atrasos: {e}")
        return None
    finally:
        con.close()

def obter_top_10_aeroportos(pasta_parquet, ano):
    if not os.path.exists(pasta_parquet): return []
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return []
    query = f"""
    SELECT
        NR_AEROPORTO_REFERENCIA
    FROM read_parquet({arquivos_parquet})
    WHERE ANO = {ano}
    GROUP BY NR_AEROPORTO_REFERENCIA
    ORDER BY SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) DESC
    LIMIT 10
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            return resultado['NR_AEROPORTO_REFERENCIA'].tolist()
        return []
    except Exception as e:
        print(f"DEBUG: Erro ao obter top 10 aeroportos: {e}")
        return []
    finally:
        con.close()

def calcular_market_share(pasta_parquet, ano=None, mes=None, aeroporto=None):
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        ano = obter_ultimo_ano_disponivel(pasta_parquet)
        if ano is None: return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = [f"ANO = {ano}"]
    if mes: condicoes.append(f"MES = {mes}")
    if aeroporto: condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes)
    query = f"""
    WITH OperatorStats AS (
        SELECT
            NR_AERONAVE_OPERADOR,
            COUNT(*) AS TotalVoos,
            SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalPassageiros
        FROM read_parquet({arquivos_parquet})
        {where_clause}
        GROUP BY NR_AERONAVE_OPERADOR
    ),
    TotalStats AS (
        SELECT
            SUM(TotalVoos) AS GrandTotalVoos,
            SUM(TotalPassageiros) AS GrandTotalPassageiros
        FROM OperatorStats
    )
    SELECT
        os.NR_AERONAVE_OPERADOR,
        (CAST(os.TotalVoos AS DOUBLE) * 100.0 / ts.GrandTotalVoos) AS VooShare,
        (CAST(os.TotalPassageiros AS DOUBLE) * 100.0 / ts.GrandTotalPassageiros) AS PaxShare
    FROM OperatorStats os, TotalStats ts
    WHERE os.TotalPassageiros > 0
    ORDER BY PaxShare DESC
    """
    try:
        resultado = con.execute(query).fetchdf()
        if not resultado.empty:
            threshold = 1.0
            maiores_operadores_df = resultado[resultado['PaxShare'] >= threshold]
            demais_df = resultado[resultado['PaxShare'] < threshold]
            final_data = maiores_operadores_df.to_dict('records')
            if not demais_df.empty:
                demais_voo_share = demais_df['VooShare'].sum()
                demais_pax_share = demais_df['PaxShare'].sum()
                demais_record = {
                    'NR_AERONAVE_OPERADOR': 'Demais',
                    'VooShare': demais_voo_share,
                    'PaxShare': demais_pax_share
                }
                final_data.append(demais_record)
            return {"data": final_data, "ano": ano, "mes": mes, "aeroporto": aeroporto}
        return None
    except Exception as e:
        print(f"DEBUG: Erro ao calcular market share: {e}")
        return None
    finally:
        con.close()

def gerar_grafico_market_share(share_data, logo_path=None):
    try:
        labels = [operador_icao_para_nome.get(item['NR_AERONAVE_OPERADOR'], item['NR_AERONAVE_OPERADOR']) for item in share_data]
        sizes = [item['PaxShare'] for item in share_data]
        colors = plt.cm.Paired(range(len(labels)))
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Adiciona a marca d'água PRIMEIRO
        if logo_path and os.path.exists(logo_path):
            logo_img = plt.imread(logo_path)
            # Posição e tamanho da marca d'água no canto inferior direito
            logo_ax = fig.add_axes([0.65, 0.05, 0.3, 0.3], anchor='SE', zorder=0)
            logo_ax.imshow(logo_img)
            logo_ax.axis('off')  # Esconde os eixos da imagem
            logo_ax.patch.set_alpha(0.0) # Torna o fundo do eixo transparente
            logo_ax.images[0].set_alpha(0.3) # Define a transparência da imagem


        wedges, texts, autotexts = ax.pie(
            sizes, 
            labels=None,
            autopct='%1.1f%%', 
            startangle=140, 
            radius=0.8,
            pctdistance=0.85,
            colors=colors,
            wedgeprops=dict(width=0.4, edgecolor='w'),
            shadow=True
        )
        ax.axis('equal')
        ax.legend(wedges, labels,
                  title="Operadores",
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))
        plt.setp(autotexts, size=8, weight="bold", color="white")
        ax.set_title("Participação de Mercado por Passageiros", pad=20)
        
        # Define o fundo do eixo como transparente para ver a marca d'água
        ax.patch.set_alpha(0.0)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', transparent=True)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"Erro ao gerar gráfico de market share: {e}")
        return None

def obter_historico_movimentacao(pasta_parquet, tipo_consulta="passageiros", aeroporto=None):
    if not os.path.exists(pasta_parquet): return None
    con = duckdb.connect(database=':memory:', read_only=False)
    arquivos_parquet = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]
    if not arquivos_parquet:
        con.close()
        return None
    condicoes = []
    if aeroporto:
        condicoes.append(f"NR_AEROPORTO_REFERENCIA = '{aeroporto.upper()}'")
    where_clause = "WHERE " + " AND ".join(condicoes) if condicoes else ""
    if tipo_consulta == "passageiros":
        select_clause = "SUM(QT_PAX_LOCAL + QT_PAX_CONEXAO_DOMESTICO + QT_PAX_CONEXAO_INTERNACIONAL) AS TotalValor"
    else:
        select_clause = "SUM(QT_CARGA) AS TotalValor"
    query = f"""
    SELECT ANO, {select_clause}
    FROM read_parquet({arquivos_parquet})
    {where_clause}
    GROUP BY ANO
    ORDER BY ANO
    """
    try:
        resultado = con.execute(query).fetchdf()
        return resultado if not resultado.empty else None
    except Exception as e:
        print(f"DEBUG: Erro ao obter histórico de movimentação: {e}")
        return None
    finally:
        con.close()

def gerar_grafico_historico(df_historico, tipo_consulta, local, logo_path=None):
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Adiciona a marca d'água PRIMEIRO
        if logo_path and os.path.exists(logo_path):
            logo_img = plt.imread(logo_path)
            fig_width, fig_height = fig.get_size_inches() * fig.dpi
            logo_width, logo_height = logo_img.shape[1], logo_img.shape[0]
            x_pos = (fig_width - logo_width) / 2
            y_pos = (fig_height - logo_height) / 2
            fig.figimage(logo_img, xo=x_pos, yo=y_pos, alpha=0.5, zorder=0)
            
        ax.plot(df_historico['ANO'], df_historico['TotalValor'], marker='o', linestyle='-', color='b')
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_title(f'Evolução Anual de {tipo_consulta.capitalize()} - {local.title()}', fontsize=16, pad=20)
        ax.set_xlabel('Ano', fontsize=12)
        ylabel = f'Total de {tipo_consulta.capitalize()}'
        if tipo_consulta == 'cargas':
            ylabel += ' (kg)'
        ax.set_ylabel(ylabel, fontsize=12)
        formatter = mticker.FuncFormatter(lambda x, p: format(int(x), ','))
        ax.yaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        
        # Define o fundo do eixo como transparente
        ax.patch.set_alpha(0.0)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"Erro ao gerar gráfico de histórico: {e}")
        return None

def transcrever_audio(audio_data):
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
    """
    Usa o LLM para reescrever a resposta factual de forma mais fluida e elaborada.
    """
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

# --- Função de Parsing da Pergunta do Usuário com LLM ---
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_result(lambda result: not result))
def parse_pergunta_com_llm(pergunta_usuario):
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
