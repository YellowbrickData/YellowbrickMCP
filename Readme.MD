
# Yellowbrick MCP Server

Yellowbrick MCP Server provides a seamless interface to query your Yellowbrick database using natural language, with optional document search powered by OpenAI embeddings.

---

## Table of Contents

- [Getting Started](#getting-started)
- [Running with Claude](#running-with-claude)
- [Running with VS Code](#running-with-vs-code)
- [Configuration](#configuration)
- [Example Questions](#example-questions)

---

## Getting Started

1. **Set your environment variables:**
   - `OPENAI_API_KEY` (required for document search)
   - `YELLOWBRICK_CONNECTION` (your Yellowbrick DB connection string)
   - `EMBEDDING_TABLE` (optional, for vector store)

2. **Build the Docker image:**
   ```sh
   source build.src
   ```

---

## Running with Claude

1. Ensure `OPENAI_API_KEY` is set in your environment.
2. Build the Docker image as above.
3. Add the following segment to your Claude config at:
   `/Users/[Your Username]/Library/Application Support/Claude/claude_desktop_config.json`

   ```json
   {
     "globalShortcut": "",
     "mcpServers": {
       "Yellowbrick": {
         "command": "docker",
         "args": [
           "run", "-i", "--rm",
           "-e", "YELLOWBRICK_CONNECTION",
           "-e", "OPEN_API_KEY",
           "-e", "EMBEDDING_TABLE",
           "ybmcp"
         ],
         "env": {
           "YELLOWBRICK_CONNECTION": "postgresql://[user]:[pwd]@[host]:[port]/[dbname]",
           "OPEN_API_KEY": "sk-XXXXXXXXX",
           "EMBEDDING_TABLE": "my_embeddings"
         }
       }
     }
   }
   ```

4. Restart Claude.
5. Ask questions like:
   - "I want to make an employee of the month plaque. Best employee is those that have the most revenue. Can you give me the winners for the last 6 months?"
   - "What is my top selling product during weekends in 2024?"
   - "Which customer buys the most books?"

---

## Running with VS Code

1. Press `CMD+SHIFT+P`, type `MCP: Add`, and choose `stdio`.
2. Enter the following command (replace placeholders with your credentials):

   ```sh
   docker run -i --rm \
     -e YELLOWBRICK_CONNECTION=postgresql://[user]:[pwd]@[host]:[port]/[dbname] \
     -e OPEN_API_KEY=sk-XXXXXXXX \
     -e EMBEDDING_TABLE=mcpvdb2 \
     ybmcp
   ```

---

## Configuration

- **YELLOWBRICK_CONNECTION**: Your Yellowbrick PostgreSQL connection string.
- **OPENAI_API_KEY**: Your OpenAI API key (required for document search).
- **EMBEDDING_TABLE**: Name of the table for vector embeddings (optional).

---

## Example Questions

- Who is the employee of the month for the last 6 months?
- What is my top selling product during weekends in 2024?
- Which customer buys the most books?

---

For more details, see the comments in `mcp.json` and the Dockerfile.

