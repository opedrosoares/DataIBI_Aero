import sys
# Add current directory to sys.path to allow importing database_utils
import os
sys.path.append(os.getcwd())

from database_utils import init_db, save_conversation, DATABASE_NAME
import sqlite3
from datetime import datetime

# Ensure DB is initialized
init_db()
print(f"Database {DATABASE_NAME} should be initialized.")

# Add a few sample conversations
print("Saving sample conversations...")
save_conversation("Hello chatbot!", "Hello user! How can I help you today?")
save_conversation("What is the capital of France?", "The capital of France is Paris.")
save_conversation("What is 2 + 2?", "2 + 2 equals 4.")
print("Sample conversations saved.")

# Part 2: Read and verify data

print(f"Reading conversations from {DATABASE_NAME}...")
conn = sqlite3.connect(DATABASE_NAME)
cursor = conn.cursor()
cursor.execute("SELECT id, timestamp, user_question, chatbot_answer FROM conversations ORDER BY timestamp ASC") # Order by ASC to get them in insertion order
rows = cursor.fetchall()
conn.close()

if not rows:
    print("Error: No conversations found in the database!")
    exit(1)
else:
    print(f"Found {len(rows)} conversations:")
    for row in rows:
        print(f"ID: {row[0]}, Timestamp: {row[1]}, User: '{row[2]}', Bot: '{row[3]}'")

    # Basic verification
    if len(rows) >= 3: # Check if at least the 3 saved conversations are there
        print("Test basic verification: At least 3 conversations found. PASSED.")
    else:
        print(f"Test basic verification: Expected at least 3 conversations, found {len(rows)}. FAILED.")
        exit(1)

    # Check content of the last inserted conversation (most recent one if ordered by ASC)
    # This depends on the test data. Let's assume the last one inserted was "What is 2 + 2?"
    last_q = "What is 2 + 2?"
    last_a = "2 + 2 equals 4."

    # Find the specific conversation to check (could be any of them)
    found_specific_test = False
    for row in rows:
        if row[2] == last_q and row[3] == last_a:
            found_specific_test = True
            break

    if found_specific_test:
        print(f"Test content verification: Found '{last_q}' -> '{last_a}'. PASSED.")
    else:
        print(f"Test content verification: Did not find '{last_q}' -> '{last_a}'. FAILED.")
        exit(1)

print("Test script completed successfully.")
