import os
import time
import logging
import psycopg2
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI
from config import load_config 
from dotenv import load_dotenv
load_dotenv()  

api_key = os.environ.get("OPENAI_API_KEY")
if not api_key:
    raise SystemExit("OPENAI_API_KEY not set")

client = OpenAI(api_key=api_key)

app = FastAPI()

# Load database config from ini
db_config = load_config(filename='database.ini', section='postgresql')

# Database connection
try:
    conn = psycopg2.connect(**db_config)
    logging.info("Connected to PostgreSQL database successfully.")
except Exception as e:
    logging.error(f"Error connecting to PostgreSQL database: {e}")
    conn = None

# Pydantic model for input
class UserQuery(BaseModel):
    query: str

def call_model(prompt, max_retries=3):
    """
    Call OpenAI GPT to generate SQL or answer.
    """
    for attempt in range(max_retries):
        try:
            resp = client.responses.create(
                model="gpt-5-mini",
                input=[{"role": "user", "content": prompt}]
            )
            return resp.output_text if hasattr(resp, "output_text") else resp['output'][0]['content'][0]['text']
        except Exception as e:
            wait = 2 ** attempt
            logging.warning(f"Model call failed (attempt {attempt+1}): {e}; retrying in {wait}s")
            time.sleep(wait)
    return None

def execute_sql(sql):
    """
    Execute SQL query on the Postgres database and return results as DataFrame
    """
    if conn is None:
        logging.error("Database connection is not established.")
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(sql, conn)

        return df
    except Exception as e:
        logging.error(f"SQL execution error: {e}")
        return pd.DataFrame()

@app.post("/ask")
def ask_question(user_query: UserQuery):
    question = user_query.query
    
    # Step 1: Translate natural language to SQL
    sql_prompt = f"""
    You are an AI helper specialized in U.S. Federal Election Commission (FEC) contribution data.
    Your task is to translate the following natural language question into a **single, safe, and directly executable SQL query** 
    for the FEC contributions database.

    ⚠️ STRICT RULES:
    - Return ONLY the SQL query, with no explanation or commentary.
    - Do not use placeholders like $1, $2, or ?. Inline all constants (numbers, strings).
    - Use fully qualified column names with table aliases (e.g., c.full_name, co.contribution_receipt_amount).
    - When filtering by text values (like occupations, employers, or names), always use flexible matching with ILIKE '%value%'.
    Example: If the user asks for "engineer", match "Engineer", "Software Engineer", "Civil Engineer", etc.
    - Use contributor_addresses.lat and contributor_addresses.lng for geospatial filtering if the question involves location or radius.
    - Always GROUP BY when aggregating with SUM or COUNT.
    - If the user specifies an occupation or employer, treat it as a keyword.
    - Always match using ILIKE '%keyword%'.
    - If the keyword has common synonyms (e.g., "educator" → "teacher", "professor"), automatically include them in the WHERE clause with OR.
    - Example: cb.occupation ILIKE '%educator%' OR cb.occupation ILIKE '%teacher%' OR cb.occupation ILIKE '%professor%'.

    - Add sensible LIMITs (e.g., LIMIT 100 for large result sets).
    - The SQL must be valid PostgreSQL syntax and run without modification.

    Database schema:

    1. contributors
    - id (PK)
    - full_name
    - contributor_type ('individual' or 'organization')

    2. contributor_addresses
    - id (PK)
    - contributor_id (FK → contributors.id)
    - street_1
    - street_2
    - city
    - state
    - zip
    - lat
    - lng

    3. contributor_business
    - id (PK)
    - contributor_id (FK → contributors.id)
    - business_address
    - employer
    - occupation

    4. contributions
    - transaction_id (PK)
    - contributor_id (FK → contributors.id)
    - contribution_receipt_amount
    - contribution_receipt_date
    - sub_id
    - year_cycle
    - contributor_year
    - committee_id

    User question: "{question}"

    Return only the SQL query.
    """
    sql_query = call_model(sql_prompt)
    
    if sql_query is None:
        return {"answer": "Error calling GPT API."}
    
    # Step 2: Execute SQL query
    df_result = execute_sql(sql_query)
    
    # Step 3: Generate final answer
    if not df_result.empty:
        result_prompt = f"""
    You are an expert in U.S. Federal Election Commission (FEC) contributions.
    The user asked: "{question}".

    Here are the SQL results:

    {df_result.to_string(index=False)}

    Instructions:
    - Provide a clear and concise answer to the user based strictly on these results.
    - Summarize key findings (e.g., top donors, totals, counts).
    - Do not explain SQL or database internals.
    - Keep the tone professional and informative.
    - If there are many rows, summarize the top results rather than listing everything.

    Final answer:
    """
    else:
        result_prompt = f"""
    You are an expert in U.S. Federal Election Commission (FEC) contributions.
    The user asked: "{question}".

    The database returned no results.

    Instructions:
    - Politely explain that no matching records were found in the database.
    - Suggest plausible reasons why (e.g., missing occupation info, no contributions in that ZIP, data entry issues).
    - Keep the explanation concise and professional.
    - Do not speculate beyond these practical reasons.

    Final answer:
    """
    
    answer = call_model(result_prompt)
    
    return {"answer": answer}


# To run the app: uvicorn main:app --reload