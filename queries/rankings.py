import os
import duckdb

def obter_aeroporto_mais_movimentado(pasta_parquet, ano=None):
    """Obtém o aeroporto mais movimentado no ano especificado"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém o aeroporto com mais voos internacionais no ano especificado"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém o operador que mais transportou passageiros no ano e aeroporto especificados"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém o operador que mais transportou cargas no ano e aeroporto especificados"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém o principal destino a partir do aeroporto de origem no ano especificado"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém o operador com maiores atrasos no ano e aeroporto especificados"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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
    """Obtém os 10 aeroportos mais movimentados no ano especificado"""
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
    """Calcula o market share dos operadores no período e aeroporto especificados"""
    if not os.path.exists(pasta_parquet): return None
    if ano is None:
        from utils.helpers import obter_ultimo_ano_disponivel
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