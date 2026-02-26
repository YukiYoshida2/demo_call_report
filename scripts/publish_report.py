#!/usr/bin/env python3
"""
CI用レポート公開スクリプト — Notion投稿 + Slack通知

リスク最小化設計:
  - Notion: Internal Integration（対象DBのみ共有、挿入権限のみ）
  - Slack: Incoming Webhook 推奨（チャネル限定、読取不可）
  - 標準ライブラリのみ使用（サプライチェーンリスクなし）

必要な環境変数:
  NOTION_API_KEY     — Notion Internal Integration トークン
  SLACK_WEBHOOK_URL  — Slack Incoming Webhook URL（推奨）

オプション:
  NOTION_DATABASE_ID — Notion DB ID（デフォルト: 対象DB）
  SLACK_BOT_TOKEN    — Slack Bot Token（Webhook未設定時のDM送信用）
  SLACK_CHANNEL      — Slack チャネル/DM ID（Bot Token使用時）
  SLACK_MENTION_USER — メンション先ユーザーID
"""

import json
import os
import re
import sys
from glob import glob
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ================================================================
# 定数
# ================================================================

JST = timezone(timedelta(hours=9))
NOTION_API = "https://api.notion.com/v1"
NOTION_VER = "2022-06-28"
MAX_BLOCKS = 100
MAX_RT_LEN = 2000

DEFAULT_DB_ID = "311eea80-adae-80a5-a798-000bc1a1a73f"
DEFAULT_MENTIONS = ["U07EJ6YKUPK", "U05V0RAF09M", "U07LNE4G2R0"]
DEFAULT_CHANNEL = "C08PMM3C601"

CHANNEL_ORDER = ["全体", "TOP", "LIS", "DIS", "FAX・EDM", "その他"]


# ================================================================
# Computed table パーサー
# ================================================================

def strip_md_bold(text):
    """Remove **bold** markers from text"""
    return re.sub(r'\*\*(.+?)\*\*', r'\1', text)


def parse_frontmatter(filepath):
    """Extract frontmatter key-value pairs from a computed markdown file."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()
    lines = content.split("\n")
    meta = {}
    in_fm = False
    for line in lines:
        if line.strip() == "---":
            if in_fm:
                break
            in_fm = True
            continue
        if in_fm and ":" in line:
            key, _, val = line.partition(":")
            meta[key.strip()] = val.strip()
    return meta


def parse_computed_table(filepath):
    """Parse a computed markdown table file (with frontmatter) into list of dicts."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")

    # Skip frontmatter (between first and second ---)
    fm_count = 0
    start = 0
    for i, line in enumerate(lines):
        if line.strip() == "---":
            fm_count += 1
            if fm_count == 2:
                start = i + 1
                break

    # Find and parse table rows
    table_rows = []
    for line in lines[start:]:
        stripped = line.strip()
        if not (stripped.startswith("|") and stripped.endswith("|")):
            if table_rows:
                break
            continue
        if re.match(r'^\s*\|[-:\s|]+\|\s*$', line):
            continue
        cells = [c.strip() for c in stripped.split("|")]
        if cells and cells[0] == "":
            cells = cells[1:]
        if cells and cells[-1] == "":
            cells = cells[:-1]
        table_rows.append(cells)

    if len(table_rows) < 2:
        return []

    # Handle duplicate headers (e.g. multiple "前月比" columns)
    raw_headers = table_rows[0]
    seen = {}
    headers = []
    for h in raw_headers:
        if h in seen:
            seen[h] += 1
            headers.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            headers.append(h)

    result = []
    for row in table_rows[1:]:
        d = {}
        for j, h in enumerate(headers):
            d[h] = strip_md_bold(row[j]) if j < len(row) else ""
        result.append(d)
    return result


def parse_cv_tables(filepath):
    """Parse step2_CVコンテンツ.md (multi-section) into {channel: [row_dicts]}."""
    with open(filepath, encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")

    # Skip frontmatter
    fm_count = 0
    start = 0
    for i, line in enumerate(lines):
        if line.strip() == "---":
            fm_count += 1
            if fm_count == 2:
                start = i + 1
                break

    result = {}
    current_channel = None
    table_lines = []

    def _flush():
        if not current_channel or not table_lines:
            return
        # Parse accumulated table lines as a mini table
        rows = []
        for tl in table_lines:
            stripped = tl.strip()
            if re.match(r'^\s*\|[-:\s|]+\|\s*$', tl):
                continue
            cells = [c.strip() for c in stripped.split("|")]
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]
            rows.append(cells)
        if len(rows) >= 2:
            hdrs = rows[0]
            seen = {}
            unique_hdrs = []
            for h in hdrs:
                if h in seen:
                    seen[h] += 1
                    unique_hdrs.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    unique_hdrs.append(h)
            parsed = []
            for row in rows[1:]:
                d = {}
                for j, h in enumerate(unique_hdrs):
                    d[h] = strip_md_bold(row[j]) if j < len(row) else ""
                parsed.append(d)
            result[current_channel] = parsed

    for line in lines[start:]:
        m = re.match(r'^####\s+(\S+)\s+Top10', line)
        if m:
            _flush()
            current_channel = m.group(1)
            table_lines = []
            continue
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            table_lines.append(line)

    _flush()
    return result


# ================================================================
# 進捗・課題抽出
# ================================================================

def extract_achievement_progress(computed_dir):
    """Read step1 tables and return per-channel achievement data."""
    files = [
        ("着電", computed_dir / "step1_着電着予.md"),
        ("SAL", computed_dir / "step1_SAL着予.md"),
        ("商談", computed_dir / "step1_商談実施着予.md"),
    ]

    progress = {}
    for label, path in files:
        if not path.exists():
            continue
        rows = parse_computed_table(path)
        for row in rows:
            ch = row.get("チャネル", "")
            rate = row.get("達成率", "")
            mark = row.get("判定", "")
            if ch not in progress:
                progress[ch] = {}
            progress[ch][label] = (rate, mark)

    return progress if progress else None


def extract_critical_issues(computed_dir):
    """Build critical issue descriptions with rate diagnosis and bad CVs."""
    issues_path = computed_dir / "step1_課題チャネル.md"
    funnel_path = computed_dir / "step2_ファネル転換率.md"
    cv_path = computed_dir / "step2_CVコンテンツ.md"

    if not issues_path.exists():
        return None

    issues_table = parse_computed_table(issues_path)
    funnel_by_ch = {}
    if funnel_path.exists():
        for row in parse_computed_table(funnel_path):
            funnel_by_ch[row.get("チャネル", "")] = row

    cv_by_ch = {}
    if cv_path.exists():
        cv_by_ch = parse_cv_tables(cv_path)

    result = []
    for row in issues_table:
        position = row.get("位置づけ", "")
        if "重点課題" not in position:
            continue

        ch = row.get("チャネル", "")
        funnel = funnel_by_ch.get(ch, {})

        # Determine which rate is the problem
        # 前月比 = リード前月比, 前月比_1 = CN率前月比, 前月比_2 = SAL率前月比
        cn_rate = funnel.get("CN率", "")
        cn_mom = funnel.get("前月比_1", "")
        sal_rate = funnel.get("SAL率", "")
        sal_mom = funnel.get("前月比_2", "")
        lead_mom = funnel.get("前月比", "")

        # Build headline: what rate is the primary problem?
        headline_parts = []

        cn_is_bad = "📉" in cn_mom
        sal_is_bad = "📉" in sal_mom
        lead_drop = ""
        try:
            lv = float(lead_mom.replace("%", "").replace("+", ""))
            if lv <= -30:
                lead_drop = lead_mom
        except (ValueError, AttributeError):
            pass

        if cn_is_bad:
            headline_parts.append(f"CN率{cn_rate}（{cn_mom.strip()}）")
        if sal_is_bad:
            headline_parts.append(f"SAL率{sal_rate}（{sal_mom.strip()}）")
        if lead_drop:
            desc = "半減" if float(lead_drop.replace("%", "").replace("+", "")) <= -40 else "大幅減"
            headline_parts.append(f"リード{lead_drop}{desc}")

        # Fallback: show failing KPIs from step1
        if not headline_parts:
            bad_kpis = []
            for label, col in [("着電", "着電"), ("SAL", "SAL"), ("商談", "商談実施")]:
                val = row.get(col, "")
                if "❌" in val:
                    pct = re.search(r'(\d+%)', val)
                    bad_kpis.append(f"{label}{pct.group(1) if pct else ''}❌")
            headline_parts.append("・".join(bad_kpis))

        headline = f"{ch}: {'、'.join(headline_parts)}"

        # Find bad CVs for this channel
        cv_detail = ""
        cv_rows = cv_by_ch.get(ch, [])
        if cv_rows:
            # Determine which rate to focus on
            focus_cn = cn_is_bad or (not sal_is_bad and not lead_drop)
            bad_cvs = []
            for cv in cv_rows:
                cv_name = cv.get("CVコンテンツ", "")
                cv_leads = cv.get("リード数", "0")
                # Check for ⚠️ marks on the problematic rate
                if focus_cn:
                    diff = cv.get("差分", "")
                    if "⚠️" in diff:
                        cv_cn = cv.get("CN率", "")
                        bad_cvs.append((cv_name, f"CN{cv_cn}", int(cv_leads.replace(",", "") or 0)))
                else:
                    # 差分_1 = SAL率の差分 (due to duplicate header handling)
                    diff = cv.get("差分_1", "")
                    if "⚠️" in diff:
                        cv_sal = cv.get("SAL率", "")
                        bad_cvs.append((cv_name, f"SAL{cv_sal}", int(cv_leads.replace(",", "") or 0)))

            # Sort by lead count desc, take top 3
            bad_cvs.sort(key=lambda x: x[2], reverse=True)
            if bad_cvs:
                cv_parts = [f"{name}({metric})" for name, metric, _ in bad_cvs[:3]]
                cv_detail = "  → " + ", ".join(cv_parts)

        result.append(headline)
        if cv_detail:
            result.append(cv_detail)

    return result if result else None


# ================================================================
# レポートファイル操作
# ================================================================

def find_latest_report():
    """reports/ 配下の最新レポートを返す"""
    files = sorted(glob("reports/レポート-*.md"))
    return files[-1] if files else None


def read_report(path):
    """レポートを (タイトル, 本文) に分離"""
    with open(path, encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")
    title_parts = []
    body_start = 0

    for i, line in enumerate(lines):
        if line.startswith("# "):
            title_parts.append(line[2:].strip())
            body_start = i + 1
        elif title_parts:
            break

    title = " / ".join(title_parts) if title_parts else "レポート"
    body = "\n".join(lines[body_start:]).strip()
    return title, body


def extract_executive_summary(body):
    """エグゼクティブサマリセクションを抽出"""
    lines = body.split("\n")
    out = []
    capturing = False

    for line in lines:
        if re.match(r"^##\s+.*エグゼクティブサマリ", line):
            capturing = True
            continue
        if capturing and re.match(r"^##\s+", line):
            break
        if capturing and line.strip():
            out.append(line)

    return "\n".join(out).strip()


# ================================================================
# Markdown → Notion ブロック変換
# ================================================================

def _chunk(text):
    """rich_text の 2000文字制限を分割"""
    while text:
        yield text[:MAX_RT_LEN]
        text = text[MAX_RT_LEN:]


def parse_rich_text(text):
    """Markdown inline → Notion rich_text 配列（**bold** 対応）"""
    if not text:
        return [{"type": "text", "text": {"content": ""}}]

    result = []
    for part in re.split(r"(\*\*[^*]+\*\*)", text):
        if not part:
            continue
        if part.startswith("**") and part.endswith("**"):
            for c in _chunk(part[2:-2]):
                result.append({
                    "type": "text",
                    "text": {"content": c},
                    "annotations": {"bold": True},
                })
        else:
            for c in _chunk(part):
                result.append({"type": "text", "text": {"content": c}})

    return result or [{"type": "text", "text": {"content": ""}}]


def _is_separator(line):
    return bool(re.match(r"^\s*\|[-:\s|]+\|\s*$", line))


def _is_table_row(line):
    s = line.strip()
    return s.startswith("|") and s.endswith("|")


def _parse_table(lines, start):
    """テーブル行群 → Notion table ブロック"""
    rows = []
    i = start
    while i < len(lines) and _is_table_row(lines[i]):
        if not _is_separator(lines[i]):
            cells = [c.strip() for c in lines[i].split("|")]
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]
            rows.append(cells)
        i += 1

    if not rows:
        return None, i

    width = max(len(r) for r in rows)
    children = []
    for row in rows:
        while len(row) < width:
            row.append("")
        children.append({
            "type": "table_row",
            "table_row": {"cells": [parse_rich_text(c) for c in row[:width]]},
        })

    block = {
        "type": "table",
        "table": {
            "table_width": width,
            "has_column_header": True,
            "has_row_header": False,
            "children": children,
        },
    }
    return block, i


def markdown_to_blocks(body):
    """Markdown → Notion API ブロック配列"""
    blocks = []
    lines = body.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i]

        # 空行
        if not line.strip():
            i += 1
            continue

        # 区切り線
        if re.match(r"^---+\s*$", line):
            blocks.append({"type": "divider", "divider": {}})
            i += 1
            continue

        # テーブル
        if _is_table_row(line):
            tbl, i = _parse_table(lines, i)
            if tbl:
                blocks.append(tbl)
            continue

        # 見出し（h4以上は h3 にマッピング）
        m = re.match(r"^(#{1,6})\s+(.+)$", line)
        if m:
            lvl = min(len(m.group(1)), 3)
            ht = f"heading_{lvl}"
            blocks.append({
                "type": ht,
                ht: {"rich_text": parse_rich_text(m.group(2))},
            })
            i += 1
            continue

        # 引用（連続する > 行をグループ化）
        if line.startswith(">"):
            qlines = []
            while i < len(lines) and lines[i].startswith(">"):
                qlines.append(re.sub(r"^>\s?", "", lines[i]))
                i += 1
            blocks.append({
                "type": "quote",
                "quote": {"rich_text": parse_rich_text("\n".join(qlines))},
            })
            continue

        # 番号付きリスト
        m = re.match(r"^(\d+)\.\s+(.+)$", line)
        if m:
            blocks.append({
                "type": "numbered_list_item",
                "numbered_list_item": {"rich_text": parse_rich_text(m.group(2))},
            })
            i += 1
            # インデント子項目
            while i < len(lines) and re.match(r"^\s+[-*]\s", lines[i]):
                sub = re.sub(r"^\s+[-*]\s+", "", lines[i])
                blocks.append({
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": parse_rich_text(sub)},
                })
                i += 1
            continue

        # 箇条書き
        m = re.match(r"^[-*]\s+(.+)$", line)
        if m:
            blocks.append({
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": parse_rich_text(m.group(1))},
            })
            i += 1
            continue

        # 通常段落
        blocks.append({
            "type": "paragraph",
            "paragraph": {"rich_text": parse_rich_text(line)},
        })
        i += 1

    return blocks


# ================================================================
# Notion API
# ================================================================

def _notion_req(method, path, api_key, payload=None):
    data = json.dumps(payload).encode() if payload else None
    req = Request(f"{NOTION_API}{path}", data=data, method=method, headers={
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VER,
    })
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"  Notion API error: {e.code} {body}", file=sys.stderr)
        return None


def create_notion_page(api_key, database_id, title, blocks):
    """Notionページ作成 → URL を返す"""
    now = datetime.now(JST)
    page_title = f"レポート {now.strftime('%Y-%m-%d %H:%M')}"

    first = blocks[:MAX_BLOCKS]
    result = _notion_req("POST", "/pages", api_key, {
        "parent": {"database_id": database_id},
        "properties": {
            "ページ名": {"title": [{"text": {"content": page_title}}]},
        },
        "children": first,
    })

    if not result:
        print("  Notion page creation FAILED (see error above)", file=sys.stderr)
        return ""

    page_id = result["id"]
    page_url = result["url"]

    # 残りブロックを追記
    for s in range(MAX_BLOCKS, len(blocks), MAX_BLOCKS):
        chunk = blocks[s : s + MAX_BLOCKS]
        _notion_req("PATCH", f"/blocks/{page_id}/children", api_key, {
            "children": chunk,
        })

    return page_url


# ================================================================
# Slack
# ================================================================

def build_slack_message(mention_users, now, progress, issues, notion_url,
                        summary_fallback=None, period_start="", period_end=""):
    parts = []
    if mention_users:
        parts.append(" ".join(f"<@{u}>" for u in mention_users))
    title = (
        f"デモ電話チーム 月次進捗レポート"
        f"（{now.strftime('%Y年%m月%d日 %H:%M')} 実行）"
    )
    if period_start and period_end:
        title += f"\n📅 参照期間: {period_start} 〜 {period_end}"
    parts.append(title)

    if progress:
        lines = ["📊 *達成進捗（チャネル別）*"]
        for ch in CHANNEL_ORDER:
            if ch not in progress:
                continue
            d = progress[ch]
            kpis = []
            for label in ["着電", "SAL", "商談"]:
                if label in d:
                    rate, mark = d[label]
                    kpis.append(f"{label}{rate}{mark}")
            if kpis:
                lines.append(f"• {ch}: {' / '.join(kpis)}")
        parts.append("\n".join(lines))

    if issues:
        lines = ["🚨 *クリティカルな課題*"]
        for line in issues:
            if line.startswith("  →"):
                lines.append(line)
            else:
                lines.append(f"• {line}")
        parts.append("\n".join(lines))

    # Fallback: use executive summary if computed tables unavailable
    if not progress and not issues and summary_fallback:
        parts.append(summary_fallback)

    if notion_url:
        parts.append(f"📎 {notion_url}")
    return "\n\n".join(parts)


def send_slack_webhook(url, message):
    data = json.dumps({"text": message}).encode()
    req = Request(url, data=data, method="POST", headers={
        "Content-Type": "application/json",
    })
    try:
        with urlopen(req):
            print("  Slack webhook: sent")
            return True
    except (HTTPError, URLError) as e:
        print(f"  Slack webhook error: {e}", file=sys.stderr)
        return False


def send_slack_api(token, channel, message):
    data = json.dumps({"channel": channel, "text": message}).encode()
    req = Request("https://slack.com/api/chat.postMessage", data=data, method="POST",
                  headers={
                      "Authorization": f"Bearer {token}",
                      "Content-Type": "application/json",
                  })
    try:
        with urlopen(req) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                print("  Slack API: sent")
                return True
            print(f"  Slack API error: {result.get('error')}", file=sys.stderr)
            return False
    except (HTTPError, URLError) as e:
        print(f"  Slack API error: {e}", file=sys.stderr)
        return False


# ================================================================
# メイン
# ================================================================

def main():
    notion_key = os.environ.get("NOTION_API_KEY", "")
    notion_db = os.environ.get("NOTION_DATABASE_ID", DEFAULT_DB_ID)
    slack_webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "")
    slack_channel = os.environ.get("SLACK_CHANNEL", DEFAULT_CHANNEL)
    mention_env = os.environ.get("SLACK_MENTION_USERS", "")
    mention_users = mention_env.split(",") if mention_env else DEFAULT_MENTIONS

    if not notion_key and not slack_webhook and not slack_token:
        print("No credentials set. Skipping publish.")
        return

    report_path = find_latest_report()
    if not report_path:
        print("No report found in reports/. Skipping.")
        return

    print(f"Report: {report_path}")
    title, body = read_report(report_path)

    # --- Notion ---
    notion_url = ""
    if notion_key:
        print("Publishing to Notion...")
        blocks = markdown_to_blocks(body)
        print(f"  Blocks: {len(blocks)}")
        notion_url = create_notion_page(notion_key, notion_db, title, blocks)
        if notion_url:
            print(f"  URL: {notion_url}")
        else:
            print("  WARNING: Notion page creation failed, URL will not be in Slack message")
    else:
        print("Notion: skipped (NOTION_API_KEY not set)")

    # --- Slack ---
    now = datetime.now(JST)

    # Try structured format from computed tables
    computed_dir = Path("data/computed")
    progress = None
    issues = None
    period_start = ""
    period_end = ""
    if computed_dir.exists():
        try:
            progress = extract_achievement_progress(computed_dir)
            issues = extract_critical_issues(computed_dir)
        except Exception as e:
            print(f"  Warning: computed table parse failed: {e}", file=sys.stderr)
        # Extract period from any computed table's frontmatter
        try:
            fm_file = computed_dir / "step2_ファネル転換率.md"
            if fm_file.exists():
                meta = parse_frontmatter(fm_file)
                period_start = meta.get("period_start", "")
                period_end = meta.get("period_end", "")
        except Exception:
            pass

    # Fallback to executive summary if computed tables unavailable
    summary_fallback = extract_executive_summary(body) if not progress else None
    message = build_slack_message(
        mention_users, now, progress, issues, notion_url,
        summary_fallback=summary_fallback,
        period_start=period_start, period_end=period_end,
    )

    if slack_webhook:
        print("Sending Slack notification (webhook)...")
        send_slack_webhook(slack_webhook, message)
    elif slack_token:
        print("Sending Slack notification (bot API)...")
        send_slack_api(slack_token, slack_channel, message)
    else:
        print("Slack: skipped (no credentials)")

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Publish error: {e}", file=sys.stderr)
        # レポートは既にコミット済みなのでCIを失敗させない
        sys.exit(0)
