import os
import pandas as pd
import duckdb

def consultar_movimentacoes_aeroportuarias(pasta_parquet, aeroporto=None, ano=None, mes=None, tipo_movimento=None, natureza=None, tipo_consulta="passageiros"):
    """Consulta as movimentações aeroportuárias com base nos parâmetros fornecidos"""
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

def obter_historico_movimentacao(pasta_parquet, tipo_consulta="passageiros", aeroporto=None):
    """Obtém o histórico de movimentação por ano"""
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