#!/bin/bash
set -euo pipefail

echo "=== Daily Analysis Start: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

# Claude Code を非対話モードで実行
# --allowedTools で必要なツールを許可（対話なしで自動承認）
claude -p "分析して。ただし /publish-report はスキップして（CIで別途実行する）" \
  --allowedTools "Bash,Read,Write,Edit,Glob,Grep,Task,Skill,ToolSearch,mcp__databricks-mcp__invoke_databricks_cli,mcp__databricks-mcp__read_skill_file,mcp__databricks-mcp__databricks_configure_auth,mcp__databricks-mcp__databricks_discover"

echo "=== Daily Analysis End: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
