import sqlite3
from datetime import datetime

DATABASE_NAME = 'chatbot_history.db'

def init_db():
    """Initializes the database and creates the conversations table if it doesn't exist."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            user_question TEXT NOT NULL,
            chatbot_answer TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def save_conversation(user_question: str, chatbot_answer: str):
    """Saves a question-answer pair to the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    timestamp = datetime.now()
    try:
        cursor.execute('''
            INSERT INTO conversations (timestamp, user_question, chatbot_answer)
            VALUES (?, ?, ?)
        ''', (timestamp, user_question, chatbot_answer))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error when saving conversation: {e}")
    finally:
        conn.close()

# Optional: Function to retrieve conversations (can be implemented later)
# def get_conversations(limit=10, offset=0):
#     conn = sqlite3.connect(DATABASE_NAME)
#     cursor = conn.cursor()
#     cursor.execute("SELECT id, timestamp, user_question, chatbot_answer FROM conversations ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))
#     rows = cursor.fetchall()
#     conn.close()
#     return rows

if __name__ == '__main__':
    # This part is for testing the database_utils.py script directly
    init_db()
    print(f"Database '{DATABASE_NAME}' initialized.")
    # Example usage (optional, for testing)
    # save_conversation("Test question?", "Test answer.")
    # conversations = get_conversations()
    # print(f"Retrieved {len(conversations)} conversations:")
    # for conv in conversations:
    #     print(conv)
