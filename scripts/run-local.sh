#!/bin/bash
set -euo pipefail

# ================================================================
# run-local.sh — ローカルバッチ実行（fetch → compute → analyze → publish）
#
# 使い方:
#   ./scripts/run-local.sh              # 手動実行（休日チェックなし）
#   ./scripts/run-local.sh --scheduled  # launchd定期実行（土日・祝日スキップ）
# ================================================================

# --- 定数 ---
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="${LOG_DIR}/run-${TODAY}.log"

# --- ヘルパー関数 ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}

# --- Step 0: 作業ディレクトリ ---
cd "$PROJECT_DIR"
mkdir -p "$LOG_DIR"

log "=== Pipeline Start ==="
log "Project: ${PROJECT_DIR}"

# --- Step 1: .env 読み込み ---
if [[ -f "${PROJECT_DIR}/.env" ]]; then
    log "Loading .env"
    set -a
    source "${PROJECT_DIR}/.env"
    set +a
else
    die ".env が見つかりません。cp .env.example .env でテンプレートから作成してください。"
fi

# --- Step 2: 必須環境変数のバリデーション ---
# ※ ANTHROPIC_API_KEY は Maxプランなら不要（claude login の認証を使用）
missing=()
[[ -z "${DATABRICKS_HOST:-}" ]] && missing+=("DATABRICKS_HOST")
[[ -z "${DATABRICKS_TOKEN:-}" ]] && missing+=("DATABRICKS_TOKEN")

if [[ ${#missing[@]} -gt 0 ]]; then
    die "必須環境変数が未設定です: ${missing[*]}"
fi

# publish用の環境変数は警告のみ（非致命的）
if [[ -z "${NOTION_API_KEY:-}" ]]; then
    log "WARNING: NOTION_API_KEY 未設定 — Notion投稿はスキップされます"
fi
if [[ -z "${SLACK_WEBHOOK_URL:-}" ]] && [[ -z "${SLACK_BOT_TOKEN:-}" ]]; then
    log "WARNING: SLACK_WEBHOOK_URL / SLACK_BOT_TOKEN 未設定 — Slack通知はスキップされます"
fi

# --- Step 3: 土日・祝日チェック（--scheduled 時のみ） ---
if [[ "${1:-}" == "--scheduled" ]]; then
    DOW=$(date +%u)  # 1=月曜 ... 7=日曜
    if [[ "$DOW" -ge 6 ]]; then
        log "土日のためスキップ (day=$DOW)"
        exit 0
    fi

    if python3 -c "
import datetime, sys
try:
    import jpholiday
    today = datetime.date.today()
    if jpholiday.is_holiday(today):
        name = jpholiday.is_holiday_name(today)
        print(f'祝日: {name}')
        sys.exit(1)
    sys.exit(0)
except ImportError:
    print('WARNING: jpholiday 未インストール。祝日チェックをスキップします', file=sys.stderr)
    sys.exit(0)
" 2>&1; then
        log "平日・営業日を確認。実行を続行します。"
    else
        log "祝日のためスキップ"
        exit 0
    fi
fi

# --- Step 4: Databricks認証 ---
if [[ ! -f ~/.databrickscfg ]]; then
    log "~/.databrickscfg を作成"
    printf '[DEFAULT]\nhost = %s\ntoken = %s\n' \
        "$DATABRICKS_HOST" "$DATABRICKS_TOKEN" > ~/.databrickscfg
fi

# --- Step 5: Agent Teams 有効化 ---
export CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1

# --- Step 6: 分析実行（fetch → compute → analyze） ---
log "=== Analysis Start ==="

claude -p "分析して。ただし /publish-report はスキップして（ローカルスクリプトで別途実行する）" \
    --allowedTools "Bash,Read,Write,Edit,Glob,Grep,Task,Skill,ToolSearch,mcp__databricks-mcp__invoke_databricks_cli,mcp__databricks-mcp__read_skill_file,mcp__databricks-mcp__databricks_configure_auth,mcp__databricks-mcp__databricks_discover" \
    2>&1 | tee -a "$LOG_FILE"

ANALYSIS_EXIT=${PIPESTATUS[0]}
log "=== Analysis End (exit=$ANALYSIS_EXIT) ==="

if [[ $ANALYSIS_EXIT -ne 0 ]]; then
    die "分析に失敗しました (exit=$ANALYSIS_EXIT)"
fi

# --- Step 7: Publish（Notion + Slack） ---
log "=== Publish Start ==="

if python3 "${PROJECT_DIR}/scripts/publish_report.py" 2>&1 | tee -a "$LOG_FILE"; then
    log "=== Publish End ==="
else
    log "WARNING: Publish に失敗しましたが、分析は完了しています"
    log "レポート: reports/レポート-${TODAY}.md"
fi

# --- Step 8: 古いログのクリーンアップ（30日超） ---
find "$LOG_DIR" -name "run-*.log" -mtime +30 -delete 2>/dev/null || true

# launchd ログのローテーション（1MB超なら後半500KBに切り詰め）
for f in "${LOG_DIR}/launchd-stdout.log" "${LOG_DIR}/launchd-stderr.log"; do
    if [[ -f "$f" ]] && [[ $(stat -f%z "$f" 2>/dev/null || echo 0) -gt 1048576 ]]; then
        tail -c 524288 "$f" > "${f}.tmp" && mv "${f}.tmp" "$f"
    fi
done

log "=== Pipeline Complete ==="
