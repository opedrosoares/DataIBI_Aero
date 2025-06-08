import os
import pandas as pd
import duckdb

from utils.constants import aeroporto_nome_para_icao, operador_icao_para_nome, mes_numero_para_nome
from utils.helpers import formatar_numero_br, obter_ultimo_ano_disponivel
from queries.database import consultar_movimentacoes_aeroportuarias, obter_historico_movimentacao
from queries.rankings import (
    obter_aeroporto_mais_movimentado,
    obter_aeroporto_mais_voos_internacionais,
    obter_operador_mais_passageiros,
    obter_operador_mais_cargas,
    obter_principal_destino,
    obter_operador_maiores_atrasos,
    obter_top_10_aeroportos,
    calcular_market_share
)
from graphics.charts import gerar_grafico_market_share, gerar_grafico_historico
from llm_services.openai_service import transcrever_audio, reescrever_resposta_com_llm, parse_pergunta_com_llm
