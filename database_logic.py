import sqlite3
import datetime
import os
import pandas as pd

# Define o nome do arquivo do banco de dados
DB_FILE = "chat_history.db"

def init_db():
    """
    Inicializa o banco de dados.
    Cria a tabela 'conversations' se ela ainda não existir.
    """
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()
        # Cria a tabela para armazenar as perguntas e respostas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                user_question TEXT NOT NULL,
                chatbot_response TEXT NOT NULL
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erro ao inicializar o banco de dados: {e}")
    finally:
        if conn:
            conn.close()

def save_conversation(question, response):
    """
    Salva um par de pergunta e resposta no banco de dados.

    Args:
        question (str): A pergunta feita pelo usuário.
        response (str): A resposta gerada pelo chatbot.
    """
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()
        timestamp = datetime.datetime.now()
        # Insere a nova conversa na tabela
        cursor.execute("""
            INSERT INTO conversations (timestamp, user_question, chatbot_response)
            VALUES (?, ?, ?)
        """, (timestamp, question, response))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erro ao salvar a conversa: {e}")
    finally:
        if conn:
            conn.close()

def get_all_conversations_as_df():
    """
    Recupera todas as conversas do banco de dados e as retorna como um DataFrame do Pandas.
    As conversas são ordenadas da mais recente para a mais antiga.

    Returns:
        pandas.DataFrame: Um DataFrame contendo o histórico de conversas.
    """
    try:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        # Usar o pandas para ler a consulta SQL diretamente em um DataFrame é mais eficiente
        query = "SELECT timestamp, user_question, chatbot_response FROM conversations ORDER BY timestamp DESC"
        df = pd.read_sql_query(query, conn)
        
        # Renomeia as colunas para uma melhor apresentação em português
        df.rename(columns={
            'timestamp': 'Data e Hora',
            'user_question': 'Pergunta do Usuário',
            'chatbot_response': 'Resposta do Chatbot'
        }, inplace=True)

        # Formata a coluna de data e hora para o padrão brasileiro
        if not df.empty:
            df['Data e Hora'] = pd.to_datetime(df['Data e Hora']).dt.strftime('%d/%m/%Y %H:%M:%S')

        return df
    except sqlite3.Error as e:
        print(f"Erro ao buscar as conversas: {e}")
        return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro
    finally:
        if conn:
            conn.close()
