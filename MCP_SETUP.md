# MCP (Model Context Protocol) Setup for EnergyExe

This guide explains how to set up MCP servers for use with Claude Code in the EnergyExe project.

## What is MCP?

MCP (Model Context Protocol) allows Claude to interact with external tools and services directly. For this project, we configure:
- **PostgreSQL MCP**: Direct database access for Claude
- **Filesystem MCP**: Enhanced file operations

## Quick Setup

### Automatic Setup

Run the setup script:

```bash
cd energyexe-core-backend
./scripts/setup-mcp.sh
```

This will:
1. Install required MCP servers globally
2. Create MCP configuration files
3. Update Claude Desktop configuration

### Manual Setup

#### 1. Install MCP Servers

```bash
# PostgreSQL MCP
npm install -g @modelcontextprotocol/server-postgres

# Filesystem MCP (optional)
npm install -g @modelcontextprotocol/server-filesystem
```

#### 2. Configure Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "energyexe-db": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-postgres"
      ],
      "env": {
        "DATABASE_URL": "postgresql://postgres:password@host:5432/energyexe_db"
      }
    },
    "energyexe-files": {
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-filesystem",
        "/path/to/energyexe-core-backend"
      ]
    }
  }
}
```

#### 3. Restart Claude Desktop

Close and reopen Claude Desktop to load the MCP servers.

## Docker Setup (Alternative)

Use Docker Compose for isolated MCP servers:

```bash
# Start MCP servers
docker-compose -f docker-compose.mcp.yml up -d

# Stop MCP servers
docker-compose -f docker-compose.mcp.yml down
```

## Environment Variables

Create `.env.mcp` for MCP-specific configuration:

```bash
# Database connection (from main .env)
DATABASE_URL=postgresql://user:pass@host:port/db

# Project paths
PROJECT_ROOT=/Users/mohammadfaisal/Documents/energyexe/energyexe-core-backend
```

## Available MCP Tools

Once configured, Claude Code will have access to:

### PostgreSQL Tools
- `mcp_postgres_query`: Execute SQL queries
- `mcp_postgres_schema`: Inspect database schema
- `mcp_postgres_tables`: List all tables
- `mcp_postgres_describe`: Describe table structure

### Filesystem Tools (if enabled)
- `mcp_filesystem_read`: Enhanced file reading
- `mcp_filesystem_write`: Enhanced file writing
- `mcp_filesystem_list`: Directory listing
- `mcp_filesystem_search`: File search

## Usage Examples

### Query Database
```sql
-- Claude can directly execute queries like:
SELECT COUNT(*) FROM generation_data_raw WHERE source = 'Taipower';
```

### Inspect Schema
```sql
-- Get table structure
DESCRIBE generation_units;
```

### Bulk Operations
```sql
-- Claude can perform complex operations
WITH unit_stats AS (
  SELECT 
    generation_unit_id,
    COUNT(*) as record_count,
    AVG(value_extracted) as avg_generation
  FROM generation_data_raw
  GROUP BY generation_unit_id
)
SELECT * FROM unit_stats ORDER BY record_count DESC;
```

## Troubleshooting

### MCP Not Showing in Claude

1. Ensure Claude Desktop is fully closed before editing config
2. Check config file syntax (valid JSON)
3. Verify MCP servers are installed: `npm list -g | grep mcp`
4. Check Claude logs: `~/Library/Logs/Claude/`

### Database Connection Issues

1. Verify DATABASE_URL is correct
2. Check network connectivity to database
3. Ensure database user has necessary permissions
4. Test connection: `psql "$DATABASE_URL"`

### Permission Errors

1. Filesystem MCP needs read permissions for project directory
2. Database user needs appropriate GRANT permissions
3. Check firewall rules for remote database

## Security Considerations

⚠️ **Important Security Notes:**

1. **Never commit** `.env.mcp` or config files with credentials
2. **Use read-only** database users when possible
3. **Restrict MCP** to development environments only
4. **Rotate credentials** regularly
5. **Use environment variables** instead of hardcoding credentials

## Project-Specific Configuration

The `.claude/mcp.json` file in this project defines:
- Database connection for EnergyExe
- Project filesystem access
- Custom tool configurations

This configuration is automatically loaded when Claude Code opens this project.

## Support

For issues or questions:
- Check Claude documentation: https://claude.ai/docs/mcp
- Project issues: Create an issue in the repository
- MCP server issues: Check respective GitHub repositories