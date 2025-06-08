import os
import duckdb

def formatar_numero_br(valor):
    """Formata um número para o padrão brasileiro de separadores"""
    return f"{int(valor):,}".replace(",", "X").replace(".", ",").replace("X", ".")

def obter_ultimo_ano_disponivel(pasta_parquet):
    """Obtém o último ano disponível nos arquivos parquet"""
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