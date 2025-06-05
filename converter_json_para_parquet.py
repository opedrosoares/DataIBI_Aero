import pandas as pd
import json
import os
import pyarrow.parquet as pq

# --- Documentação do Código ---
# Este script demonstra como converter arquivos JSON de movimentações aeroportuárias
# para o formato Parquet. O formato Parquet é mais eficiente para armazenamento
# e leitura de grandes volumes de dados tabulares, especialmente para consultas analíticas.

def converter_json_para_parquet(pasta_entrada, pasta_saida):
    """
    Converte todos os arquivos JSON em uma pasta de entrada para o formato Parquet
    e os salva em uma pasta de saída.

    Args:
        pasta_entrada (str): O caminho para a pasta que contém os arquivos JSON.
        pasta_saida (str): O caminho para a pasta onde os arquivos Parquet serão salvos.
    """
    if not os.path.exists(pasta_entrada):
        print(f"Erro: A pasta de entrada '{pasta_entrada}' não foi encontrada.")
        return

    os.makedirs(pasta_saida, exist_ok=True) # Cria a pasta de saída se não existir

    # Adicionando depuração para mostrar o que está sendo lido
    print(f"\nDEBUG: Conteúdo da pasta de entrada '{pasta_entrada}':")
    for item in os.listdir(pasta_entrada):
        item_path = os.path.join(pasta_entrada, item)
        if os.path.isfile(item_path):
            print(f"  - {item} (Tamanho: {os.path.getsize(item_path) / (1024*1024):.2f} MB)")
        else:
            print(f"  - {item} (Diretório)")


    for nome_arquivo in os.listdir(pasta_entrada):
        if nome_arquivo.endswith('.json'):
            caminho_json = os.path.join(pasta_entrada, nome_arquivo)
            nome_base, _ = os.path.splitext(nome_arquivo)
            caminho_parquet = os.path.join(pasta_saida, f"{nome_base}.parquet")

            print(f"\nConvertendo {nome_arquivo} para Parquet...")
            print(f"DEBUG: Caminho completo do JSON: {caminho_json}")
            print(f"DEBUG: Tamanho do JSON a ser lido: {os.path.getsize(caminho_json) / (1024*1024):.2f} MB")

            try:
                # --- ALTERAÇÃO AQUI: USANDO encoding='utf-8-sig' ---
                with open(caminho_json, 'r', encoding='utf-8-sig') as f: #
                    dados = json.load(f)

                # Verifica se os dados carregados estão vazios ou muito pequenos
                if not dados:
                    print(f"AVISO: O arquivo JSON '{nome_arquivo}' foi lido, mas resultou em dados vazios.")
                    continue # Pula para o próximo arquivo, se houver

                df = pd.DataFrame(dados)

                print(f"DEBUG: DataFrame carregado. Número de linhas: {len(df)}")
                # Comentar as próximas linhas de head() e info() para evitar saída muito grande para 215MB
                # print(f"DEBUG: Primeiras 5 linhas do DataFrame (antes do tratamento de tipo):")
                # print(df.head())
                # print(f"DEBUG: Tipos de dados (antes do tratamento):")
                # df.info()


                # --- Tratamento de Tipos de Dados ---
                df['QT_PAX_LOCAL'] = pd.to_numeric(df['QT_PAX_LOCAL'], errors='coerce').fillna(0).astype(int)
                df['QT_PAX_CONEXAO_DOMESTICO'] = pd.to_numeric(df['QT_PAX_CONEXAO_DOMESTICO'], errors='coerce').fillna(0).astype(int)
                df['QT_CARGA'] = pd.to_numeric(df['QT_CARGA'], errors='coerce').fillna(0).astype(int)

                # Comentar as próximas linhas de info() para evitar saída muito grande para 215MB
                # print(f"DEBUG: Tipos de dados (após tratamento):")
                # df.info()

                # Salva o DataFrame como Parquet
                df.to_parquet(caminho_parquet, index=False)
                print(f"  -> Salvo como {nome_base}.parquet (Tamanho: {os.path.getsize(caminho_parquet) / (1024*1024):.2f} MB)")
            except json.JSONDecodeError as e:
                print(f"  Erro ao decodificar JSON no arquivo '{nome_arquivo}': {e}")
                print(f"  Por favor, verifique se o arquivo '{nome_arquivo}' é um JSON válido.")
            except Exception as e:
                print(f"  Ocorreu um erro inesperado ao converter {nome_arquivo}: {e}")

# --- Exemplo de Uso ---
if __name__ == "__main__":
    pasta_json_origem = 'dados_aeroportuarios'
    pasta_parquet_destino = 'dados_aeroportuarios_parquet'

    # Cria as pastas de dados se elas não existirem
    os.makedirs(pasta_json_origem, exist_ok=True)
    os.makedirs(pasta_parquet_destino, exist_ok=True)

    # Lembre-se: O SEU ARQUIVO Movimentacoes_Aeroportuarias_202401.json (215.04 MB)
    # DEVE JÁ ESTAR NA PASTA 'dados_aeroportuarios' ANTES DE RODAR ESTE SCRIPT!

    converter_json_para_parquet(pasta_json_origem, pasta_parquet_destino)

    print(f"\nConversão concluída. Verifique a pasta '{pasta_parquet_destino}' para os arquivos Parquet.")

    # Opcional: Verificar o conteúdo de um arquivo Parquet
    try:
        arquivos_parquet = [f for f in os.listdir(pasta_parquet_destino) if f.endswith('.parquet')]
        if arquivos_parquet:
            primeiro_parquet = os.path.join(pasta_parquet_destino, arquivos_parquet[0])
            df_teste_parquet = pd.read_parquet(primeiro_parquet)
            print(f"\nConteúdo do primeiro arquivo Parquet ({arquivos_parquet[0]}):")
            print(df_teste_parquet.head())
            print("\nInformações sobre o DataFrame do Parquet:")
            df_teste_parquet.info()
            print("\nColunas do DataFrame do Parquet:")
            print(df_teste_parquet.columns.tolist())
        else:
            print("Nenhum arquivo Parquet encontrado para verificação.")
    except Exception as e:
        print(f"Erro ao ler arquivo Parquet para verificação: {e}")