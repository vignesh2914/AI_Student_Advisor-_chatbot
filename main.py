from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain.agents.agent_types import AgentType
from sqlalchemy import create_engine
from langchain_groq import ChatGroq
from urllib.parse import quote
from dotenv import load_dotenv
from langchain.prompts import PromptTemplate
from sentence_transformers import SentenceTransformer
import faiss
import os
import numpy as np

from src.bot.logger import logging
from src.bot.exception import CustomException

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


# """
# ### ImportantNote- >  You can use both for me rate limit came when i keep on useing
# llama 70b kindly switch while testing for me both gave proper output but 70 b was given in a detailed way
# """
llm = ChatGroq(api_key=GROQ_API_KEY, model="llama-3.1-8b-instant", streaming=True)
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

SQL_PROMPT_TEMPLATE = PromptTemplate(
    input_variables=["query", "context"],
    template="""
    You are an intelligent assistant that helps users get answers from a SQL database. 
    Given the user's request and the database context, your job is to:

    1. Understand what information the user is asking for (columns, conditions, filters).
    2. Write a clean and accurate SQL query using the context provided.
    3. Retrieve the necessary data using the SQL toolkit.
    4. Respond with a clear, concise, and friendly explanation, as if you're chatting with the user.

    User Query: {query}
    Database Context: {context}

    --- Example Response Style ---
    Query: “What are Alice’s academic records?”
    If the context includes a table 'academic_records' with columns like 'name', 'course', 'grade', and 'date',
    you might respond:
    "Here’s what I found for Alice: She scored an A in Math on Jan 10, 2025, and an A- in Science on Feb 1, 2025."

    Now, based on the query and context, write the SQL and explain the result in a friendly, human-like way.
    """
)

def connectDatabase(username, port, host, password, database):
    try:
        encoded_password = quote(password)
        mysql_uri = f"mysql+mysqlconnector://{username}:{encoded_password}@{host}:{port}/{database}"
        db = SQLDatabase(create_engine(mysql_uri))
        logging.info("Successfully connected to the database.")
        return db
    except Exception as e:
        logging.error("Error while connecting to the database.")
        raise CustomException(e)

def embed_database_content(db):
    try:
        table_names = db.get_table_names()
        context_texts = []
        logging.info(f"Found tables: {table_names}")
        
        for table in table_names:
            table_info = db.get_table_info([table])
            context_texts.append(table_info)
            sample_query = f"SELECT * FROM {table} LIMIT 5"
            sample_data = db.run(sample_query)
            context_texts.append(f"Sample data from {table}: {sample_data}")
        
        embeddings = embedding_model.encode(context_texts, convert_to_numpy=True)
        logging.info("Successfully created embeddings from database context.")
        return context_texts, embeddings
    except Exception as e:
        logging.error("Error during embedding of database content.")
        raise CustomException(e)

def build_faiss_index(embeddings):
    try:
        dimension = embeddings.shape[1]
        index = faiss.IndexFlatL2(dimension)
        index.add(embeddings)
        logging.info("FAISS index built successfully.")
        return index
    except Exception as e:
        logging.error("Error while building FAISS index.")
        raise CustomException(e)

def search_relevant_context(query_text, context_texts, faiss_index, k=3):
    try:
        query_embedding = embedding_model.encode([query_text], convert_to_numpy=True)
        distances, indices = faiss_index.search(query_embedding, k)
        relevant_context = [context_texts[idx] for idx in indices[0]]
        logging.info("Successfully retrieved relevant context using FAISS.")
        return "\n".join(relevant_context)
    except Exception as e:
        logging.error("Error during FAISS context retrieval.")
        raise CustomException(e)

def generate_response(db, query_text, llm, faiss_index, context_texts):
    try:
        relevant_context = search_relevant_context(query_text, context_texts, faiss_index)
        augmented_query = SQL_PROMPT_TEMPLATE.format(query=query_text, context=relevant_context)

        toolkit = SQLDatabaseToolkit(db=db, llm=llm)
        agent = create_sql_agent(
            llm=llm,
            toolkit=toolkit,
            verbose=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        )

        logging.info("Invoking the SQL agent with the generated prompt.")
        response = agent.invoke(augmented_query)
        return response
    except Exception as e:
        logging.error("Error during response generation from SQL agent.")
        raise CustomException(e)

if __name__ == "__main__":
    try:
        db = connectDatabase(username="root", host="localhost", port=3306, password="Hesengiv1429+", database="student_database")
        context_texts, embeddings = embed_database_content(db)
        faiss_index = build_faiss_index(embeddings)

        query = "I am Ali Abdulla Aldhaheri. Can you suggest electives that align with my major and strengths?"
        response = generate_response(db, query, llm, faiss_index, context_texts)
        print(response["output"])
        logging.info("Successfully processed the query and retrieved the response.")
    except Exception as e:
        logging.error("Unhandled exception in main execution block.")
        raise CustomException(e)
