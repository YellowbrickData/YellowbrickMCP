#!/usr/bin/env python3
"""
Yellowbrick MCP Server
Exposes Yellowbrick database tables, schemas, and query functionality as MCP resources and tools.
Uses FastMCP for server functionality and psycopg2 for database interaction.
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from mcp.server.fastmcp import FastMCP
import logging


DEFAULT_DB_CONNECTION = "postgresql://user:password@localhost:5432/database"
OPEN_API_KEY = os.getenv("OPEN_API_KEY", "your-openai-api-key")
EMBEDDING_TABE_NAME = os.getenv("EMBEDDING_TABLE", "my_embeddings")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("Yellowbrick Database")

connection_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None

def get_connection_string() -> str:
    return os.getenv("YELLOWBRICK_CONNECTION", DEFAULT_DB_CONNECTION)

def init_connection_pool():
    global connection_pool
    try:
        connection_string = get_connection_string()
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,  # min and max connections
            connection_string,
            cursor_factory=RealDictCursor
        )
        logger.info("Connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise

def get_db_connection():
    if connection_pool is None:
        init_connection_pool()
    return connection_pool.getconn()

def return_db_connection(conn):
    if connection_pool:
        connection_pool.putconn(conn)

async def execute_query(sql: str, params: tuple = None) -> List[Dict[str, Any]]:
    """Execute a query and return results"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            if cursor.description:
                return [dict(row) for row in cursor.fetchall()]
            return []
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise
    finally:
        if conn:
            return_db_connection(conn)

@mcp.resource("yellowbrick://database/schemas")
def get_database_schemas() -> str:
    """Get list of all available schemas in the database"""
    try:
        # Use a sync wrapper since resources can't be async in FastMCP
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        schemas = loop.run_until_complete(execute_query("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """))
        
        loop.close()
        
        info = "# Database Schemas\n\n"
        if schemas:
            for schema in schemas:
                info += f"- **{schema['schema_name']}**\n"
        else:
            info += "No user schemas found."
        
        return info
        
    except Exception as e:
        return f"Error getting schemas: {e}"

# Dynamic schema info resources - we'll register these dynamically
# But for now, let's use a static approach with common schema names

@mcp.resource("yellowbrick://schema/public")
def get_public_schema_info() -> str:
    """Get detailed information about the public schema"""
    return get_schema_info_sync("public")

@mcp.resource("yellowbrick://schema/information_schema")
def get_information_schema_info() -> str:
    """Get detailed information about the information_schema"""
    return get_schema_info_sync("information_schema")

def get_schema_info_sync(schema_name: str) -> str:
    """Get detailed information about a schema (synchronous wrapper)"""
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get tables in schema
        tables = loop.run_until_complete(execute_query("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema_name,)))
        
        loop.close()
        
        info = f"# Schema: {schema_name}\n\n"
        
        if tables:
            info += "## Tables and Views:\n"
            for table in tables:
                info += f"- **{table['table_name']}** ({table['table_type']})\n"
                
        return info
        
    except Exception as e:
        return f"Error getting schema info: {e}"

# Tools

@mcp.tool()
async def execute_sql_query(sql: str, limit: int = 1000) -> str:
    """
    Execute a SQL query against the Yellowbrick database
    
    Args:
        sql: The SQL query to execute
        limit: Maximum number of rows to return (default: 1000)
    """
    try:
        if sql.strip().upper().startswith('SELECT') and 'LIMIT' not in sql.upper():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"        
        results = await execute_query(sql)
        if not results:
            return "Query executed successfully. No results returned."
        if len(results) > 0:
            columns = list(results[0].keys())
            
            output = "| " + " | ".join(columns) + " |\n"
            output += "|" + "|".join([" --- " for _ in columns]) + "|\n"
            
            for row in results:
                values = [str(row[col]) if row[col] is not None else "NULL" for col in columns]
                output += "| " + " | ".join(values) + " |\n"
            
            if len(results) == limit:
                output += f"\n*Note: Results limited to {limit} rows*"
            
            return output
        return "Query executed successfully."
    except Exception as e:
        return f"Error executing query: {e}"

@mcp.tool()
async def describe_table(schema_name: str, table_name: str) -> str:
    """
    Get detailed information about a specific table
    
    Args:
        schema_name: The schema name
        table_name: The table name
    """
    try:
        # Use DESCRIBE [schema].[table] WITH DDL for table description
        sql = f"DESCRIBE {schema_name}.{table_name} WITH DDL"
        results = await execute_query(sql)
        if not results:
            return f"No description found for table {schema_name}.{table_name}."
        output = f"# Table: {schema_name}.{table_name}\n\n"
        output += "```sql\n"
        for row in results:
            output += " ".join(str(v) for v in row.values()) + "\n"
        output += "```\n"

        # Now get column descriptions (comments) if available
        comment_sql = f"""
            SELECT
                attname AS column_name,
                col_description(('"{schema_name}"."{table_name}"'::regclass)::oid, attnum) AS column_comment
            FROM pg_attribute
            WHERE attrelid = '"{schema_name}"."{table_name}"'::regclass
              AND attnum > 0
              AND NOT attisdropped;
        """
        comments = await execute_query(comment_sql)
        # Only include columns with a non-null comment
        comments = [c for c in comments if c.get('column_comment')]
        if comments:
            output += "\n## Column Descriptions\n"
            for c in comments:
                output += f"- **{c['column_name']}**: {c['column_comment']}\n"
        return output
    except Exception as e:
        return f"Error getting table info: {e}"

@mcp.tool()
async def list_schemas() -> str:
    """List all available schemas in the database"""
    try:
        schemas = await execute_query("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        
        if schemas:
            output = "## Available Schemas:\n"
            for schema in schemas:
                output += f"- {schema['schema_name']}\n"
            return output
        else:
            return "No schemas found."
            
    except Exception as e:
        return f"Error listing schemas: {e}"

@mcp.tool()
async def list_tables(schema_name: str) -> str:
    """
    List all tables in a specific schema
    
    Args:
        schema_name: The schema name to list tables from
    """
    try:
        tables = await execute_query("""
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema_name,))
        
        if tables:
            output = f"## Tables in schema '{schema_name}':\n"
            for table in tables:
                output += f"- **{table['table_name']}** ({table['table_type']})\n"
            return output
        else:
            return f"No tables found in schema '{schema_name}'."
            
    except Exception as e:
        return f"Error listing tables: {e}"

@mcp.tool()
async def search_documents(query: str, limit: int = 5) -> str:
    """
    Search documents in the database using a vector similarity returns limit results
    
    Args:
        query: The search query
        limit: Maximum number of results to return (default: 10)
    """
    """
    Search documents in the database using LangChain and Yellowbrick vector store.
    """
    try:
        from langchain.embeddings import OpenAIEmbeddings
        from langchain.vectorstores import Yellowbrick

        embeddings = OpenAIEmbeddings(openai_api_key=OPEN_API_KEY)
        vectorstore = Yellowbrick(embeddings,
                                  get_connection_string(),
                                  EMBEDDING_TABE_NAME
                )
# 
        docs = vectorstore.similarity_search(query, k=limit)
        if not docs:
            return "No documents found matching the query."

        output = "## Search Results:\n\n"
        for doc in docs:
            title = doc.metadata.get("document_title", "")
            snippet = doc.page_content.strip().replace("\n", " ")
            output += f"- **{title}**: {snippet}...\n"
        return output
    except Exception as e:
        return f"Error searching documents: {e}"


@mcp.tool()
async def get_table_sample(schema_name: str, table_name: str, limit: int = 10) -> str:
    """
    Get a sample of rows from a table
    
    Args:
        schema_name: The schema name
        table_name: The table name  
        limit: Number of sample rows to return (default: 10)
    """
    try:
        # Build the query with proper escaping
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT %s"
        results = await execute_query(query, (limit,))
        
        if not results:
            return f"Table {schema_name}.{table_name} is empty."
        
        # Format results as a table
        columns = list(results[0].keys())
        
        # Create header
        output = f"## Sample data from {schema_name}.{table_name}\n\n"
        output += "| " + " | ".join(columns) + " |\n"
        output += "|" + "|".join([" --- " for _ in columns]) + "|\n"
        
        # Add rows
        for row in results:
            values = [str(row[col]) if row[col] is not None else "NULL" for col in columns]
            output += "| " + " | ".join(values) + " |\n"
        
        return output
        
    except Exception as e:
        return f"Error getting table sample: {e}"

def main():
    try:
        init_connection_pool()
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
    finally:
        if connection_pool:
            connection_pool.closeall()

if __name__ == "__main__":
    main()
