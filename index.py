#!/usr/bin/env python3
"""
Yellowbrick MCP Server
Exposes Yellowbrick database tables, schemas, and query functionality as MCP resources and tools.
Uses FastMCP for server functionality and psycopg2 for database interaction.
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional, Tuple
import logging

import psycopg2
from psycopg2 import pool
from psycopg2 import sql as psql
from psycopg2.extras import RealDictCursor

from mcp.server.fastmcp import FastMCP

# -----------------------
# Config / environment
# -----------------------

DEFAULT_DB_CONNECTION = "postgresql://user:password@localhost:5432/database"

# Use standard OPENAI_API_KEY; fail fast if missing when search_documents is called.
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

EMBEDDING_TABLE_NAME = os.getenv("EMBEDDING_TABLE", "my_embeddings")
READ_ONLY = os.getenv("READ_ONLY", "true").lower() == "true"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("yb-mcp")

mcp = FastMCP("Yellowbrick Database")

connection_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None


def get_connection_string() -> str:
    return os.getenv("YELLOWBRICK_CONNECTION", DEFAULT_DB_CONNECTION)


def init_connection_pool() -> None:
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


def return_db_connection(conn) -> None:
    if connection_pool:
        connection_pool.putconn(conn)


# -----------------------
# Query helpers
# -----------------------

WRITE_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE",
    "GRANT", "REVOKE", "COMMENT", "ANALYZE", "VACUUM", "REFRESH"
}


def _is_write(sql_text: str) -> bool:
    first = (sql_text or "").lstrip().split(None, 1)
    return bool(first) and first[0].upper() in WRITE_KEYWORDS


def _execute_query_sync(
    sql_text_or_composed,
    params: Optional[Tuple[Any, ...]] = None,
    autocommit: bool = False
) -> List[Dict[str, Any]]:
    """Blocking psycopg2 execution (runs in a thread when called from async)."""
    conn = None
    try:
        conn = get_db_connection()
        if autocommit:
            conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql_text_or_composed, params)
            if cur.description:
                rows = cur.fetchall()
                if not autocommit:
                    conn.commit()
                # RealDictCursor already returns dict-like rows
                return [dict(r) for r in rows]
            # No resultset (DDL/DML); ensure commit if not autocommit
            if not autocommit:
                conn.commit()
            return []
    except Exception:
        # attempt rollback if needed
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn:
            return_db_connection(conn)


async def execute_query(
    sql_text_or_composed,
    params: Optional[Tuple[Any, ...]] = None,
    autocommit: bool = False
) -> List[Dict[str, Any]]:
    """Async-friendly wrapper that pushes blocking work onto a thread."""
    return await asyncio.to_thread(_execute_query_sync, sql_text_or_composed, params, autocommit)


# For sync MCP resources (FastMCP resources are sync), call the sync helper directly.
def run_query_sync(sql_text_or_composed, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    return _execute_query_sync(sql_text_or_composed, params, autocommit=False)


# -----------------------
# Resources (sync)
# -----------------------

@mcp.resource("yellowbrick://database/schemas")
def get_database_schemas() -> str:
    """Get list of all available schemas in the database (resource)."""
    try:
        schemas = run_query_sync("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        info = "# Database Schemas\n\n"
        if schemas:
            for schema in schemas:
                info += f"- **{schema['schema_name']}**\n"
        else:
            info += "No user schemas found."
        return info
    except Exception as e:
        logger.exception("Error getting schemas")
        return f"Error getting schemas: {e}"


@mcp.resource("yellowbrick://schema/public")
def get_public_schema_info() -> str:
    """Get detailed information about the public schema."""
    return get_schema_info_sync("public")


@mcp.resource("yellowbrick://schema/information_schema")
def get_information_schema_info() -> str:
    """Get detailed information about the information_schema."""
    return get_schema_info_sync("information_schema")


def get_schema_info_sync(schema_name: str) -> str:
    """Get detailed information about a schema (sync)."""
    try:
        tables = run_query_sync(
            """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema_name,)
        )
        info = f"# Schema: {schema_name}\n\n"
        if tables:
            info += "## Tables and Views:\n"
            for table in tables:
                info += f"- **{table['table_name']}** ({table['table_type']})\n"
        else:
            info += "_No tables found._\n"
        return info
    except Exception as e:
        logger.exception("Error getting schema info")
        return f"Error getting schema info: {e}"


# -----------------------
# Tools (async)
# -----------------------

@mcp.tool()
async def execute_sql_query(sql: str, limit: int = 1000) -> str:
    """
    Execute a SQL query against the Yellowbrick database.

    Args:
        sql: The SQL query to execute
        limit: Maximum number of rows to return for SELECTs (default: 1000)
    """
    try:
        if _is_write(sql):
            if READ_ONLY:
                return "Write operations are disabled on this endpoint."
            # If you ever toggle READ_ONLY off, autocommit writes.
            rows = await execute_query(sql, autocommit=True)
            return "Statement executed and committed." if not rows else "Statement executed with a result set."

        # SELECT path (apply LIMIT if absent)
        if sql.strip().upper().startswith("SELECT") and "LIMIT" not in sql.upper():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        results = await execute_query(sql)
        if not results:
            return "Query executed successfully. No results returned."

        columns = list(results[0].keys())
        out = []
        out.append("| " + " | ".join(columns) + " |")
        out.append("|" + "|".join([" --- " for _ in columns]) + "|")
        for row in results:
            values = [str(row.get(col)) if row.get(col) is not None else "NULL" for col in columns]
            out.append("| " + " | ".join(values) + " |")
        if len(results) >= limit and "LIMIT" in sql.upper():
            out.append(f"\n*Note: Results limited to {limit} rows*")
        return "\n".join(out)
    except Exception as e:
        logger.exception("Error executing query")
        return f"Error executing query: {e}"


@mcp.tool()
async def describe_table(schema_name: str, table_name: str) -> str:
    """
    Get detailed information about a specific table (Yellowbrick specific)
    using DESCRIBE <schema>.<table> WITH DDL, plus column comments.
    """
    try:
        # DESCRIBE WITH DDL (Yellowbrick)
        describe_sql = psql.SQL("DESCRIBE {}.{} WITH DDL").format(
            psql.Identifier(schema_name), psql.Identifier(table_name)
        )
        ddl_rows = await execute_query(describe_sql)

        if not ddl_rows:
            return f"No description found for table {schema_name}.{table_name}."

        output = [f"# Table: {schema_name}.{table_name}", "```sql"]
        for row in ddl_rows:
            # Row is dict-like; preserve values in a stable order
            output.append(" ".join(str(v) for v in row.values()))
        output.append("```")

        # Column comments (Postgres-compatible)
        comment_rows = await execute_query(
            """
            SELECT a.attname AS column_name,
                   col_description(a.attrelid, a.attnum) AS column_comment
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (schema_name, table_name)
        )
        comment_rows = [c for c in comment_rows if c.get("column_comment")]
        if comment_rows:
            output.append("\n## Column Descriptions")
            for c in comment_rows:
                output.append(f"- **{c['column_name']}**: {c['column_comment']}")
        return "\n".join(output)
    except Exception as e:
        logger.exception("Error getting table info")
        return f"Error getting table info: {e}"


@mcp.tool()
async def list_schemas() -> str:
    """List all available schemas in the database."""
    try:
        schemas = await execute_query("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name
        """)
        if not schemas:
            return "No schemas found."
        lines = ["## Available Schemas:"]
        lines += [f"- {s['schema_name']}" for s in schemas]
        return "\n".join(lines)
    except Exception as e:
        logger.exception("Error listing schemas")
        return f"Error listing schemas: {e}"


@mcp.tool()
async def list_tables(schema_name: str) -> str:
    """
    List all tables in a specific schema.
    """
    try:
        tables = await execute_query(
            """
            SELECT table_name, table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
            """,
            (schema_name,)
        )
        if not tables:
            return f"No tables found in schema '{schema_name}'."
        lines = [f"## Tables in schema '{schema_name}':"]
        lines += [f"- **{t['table_name']}** ({t['table_type']})" for t in tables]
        return "\n".join(lines)
    except Exception as e:
        logger.exception("Error listing tables")
        return f"Error listing tables: {e}"


@mcp.tool()
async def get_table_sample(schema_name: str, table_name: str, limit: int = 10) -> str:
    """
    Get a sample of rows from a table (safe identifier quoting).
    """
    try:
        # Compose SQL safely for identifiers; keep limit as a parameter
        query = psql.SQL("SELECT * FROM {}.{} LIMIT %s").format(
            psql.Identifier(schema_name),
            psql.Identifier(table_name)
        )
        results = await execute_query(query, (limit,))
        if not results:
            return f"Table {schema_name}.{table_name} is empty."

        columns = list(results[0].keys())
        out = [f"## Sample data from {schema_name}.{table_name}", ""]
        out.append("| " + " | ".join(columns) + " |")
        out.append("|" + "|".join([" --- " for _ in columns]) + "|")
        for row in results:
            values = [str(row.get(col)) if row.get(col) is not None else "NULL" for col in columns]
            out.append("| " + " | ".join(values) + " |")
        return "\n".join(out)
    except Exception as e:
        logger.exception("Error getting table sample")
        return f"Error getting table sample: {e}"


@mcp.tool()
async def search_documents(query: str, limit: int = 5) -> str:
    """
    Search documents in the database using a vector similarity search, returning up to `limit` results.
    """
    try:
        if not OPENAI_API_KEY:
            return "OPENAI_API_KEY is not set."

        # Prefer modern langchain-openai; fall back if user’s env is older.
        try:
            from langchain_openai import OpenAIEmbeddings
        except ImportError:
            # Legacy fallback (if installed)
            from langchain.embeddings import OpenAIEmbeddings  # type: ignore

        # Assuming a Yellowbrick vector store integration is available on sys.path
        try:
            from langchain.vectorstores import Yellowbrick  # your custom integration
        except Exception as e:
            return f"Vector store not available: {e}"

        embeddings = OpenAIEmbeddings(api_key=OPENAI_API_KEY)
        vectorstore = Yellowbrick(
            embeddings,
            get_connection_string(),
            EMBEDDING_TABLE_NAME
        )

        docs = vectorstore.similarity_search(query, k=limit)
        if not docs:
            return "No documents found matching the query."

        lines = ["## Search Results:", ""]
        for doc in docs:
            title = doc.metadata.get("document_title", "") if hasattr(doc, "metadata") else ""
            snippet = (doc.page_content or "").strip().replace("\n", " ")
            prefix = f"**{title}**: " if title else ""
            lines.append(f"- {prefix}{snippet}...")
        return "\n".join(lines)
    except Exception as e:
        logger.exception("Error searching documents")
        return f"Error searching documents: {e}"


# -----------------------
# Entrypoint
# -----------------------

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
            try:
                connection_pool.closeall()
            except Exception:
                pass


if __name__ == "__main__":
    main()
