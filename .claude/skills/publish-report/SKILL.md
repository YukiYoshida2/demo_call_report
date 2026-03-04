---
name: publish-report
description: 生成済みレポートをNotionデータベースに投稿し、そのURLをSlackに通知する。
---

# レポート公開スキル

`reports/` 配下の最新レポートMarkdownをNotionデータベースに新規ページとして追加し、そのURLをSlackに投稿する。

## 前提

- `/generate-report` でレポートが `reports/レポート-YYYY-MM-DD.md` に生成済みであること
- Notion MCP および Slack MCP が利用可能であること

## 公開先

| サービス | 宛先 | 備考 |
|---------|------|------|
| Notion | デモ電話日報レポートDB（data_source: `311eea80-adae-80a5-a798-000bc1a1a73f`） | DB URL: https://www.notion.so/ivry-jp/311eea80adae80f189f6f23ab7422be6 |
| Slack | チャンネル `C08PMM3C601` | https://ivry-jp.slack.com/archives/C08PMM3C601 |

## 実行手順

### Step 1: レポートファイル特定

1. `reports/` ディレクトリから最新の `レポート-YYYY-MM-DD.md` を取得する
2. ファイルが存在しない場合はエラー終了（先に `/generate-report` を実行するよう案内）

### Step 2: Markdown → Notion形式変換

レポート内のMarkdownテーブル（`| ... |` 形式）をNotion flavored Markdownの `<table>` 形式に変換する。

**変換ルール:**
- `| header1 | header2 |` 行 → `<td>header1</td><td>header2</td>` の `<tr>` に変換
- `|---|---|` のセパレータ行 → 除去
- 各テーブルに `header-row="true"` を付与
- テーブル内のリッチテキスト（`**bold**`, `\`code\``, 絵文字）はそのまま維持
- `[DIS平均]` など `[]` が含まれるセルは `\[\]` にエスケープ

**変換方法:** Pythonスクリプトで一括変換する。

```python
import re

def md_table_to_notion(table_lines):
    data_lines = [l for l in table_lines if not re.match(r'^\s*\|[-:\s|]+\|\s*$', l)]
    rows = []
    for line in data_lines:
        cells = [c.strip() for c in line.split('|')]
        if cells and cells[0] == '': cells = cells[1:]
        if cells and cells[-1] == '': cells = cells[:-1]
        rows.append(cells)
    result = '<table header-row="true">\n'
    for row in rows:
        result += '\t<tr>\n'
        for cell in row:
            result += f'\t\t<td>{cell}</td>\n'
        result += '\t</tr>\n'
    result += '</table>'
    return result
```

**追加処理:**
- レポート先頭の `# タイトル行` は除去（Notionページのプロパティ `ページ名` で設定するため）
- タイトル以外のコンテンツをすべてNotion形式に変換

### Step 3: Notionページ作成

1. `ToolSearch` で `select:mcp__claude_ai_Notion__notion-create-pages` を実行し、deferred toolをロードする（必須。ロードしないとツール呼び出しが失敗する）
2. `notion-create-pages` で以下のパラメータでページ作成:
   - **parent**: `{"data_source_id": "311eea80-adae-80a5-a798-000bc1a1a73f"}`
   - **properties**: `{"ページ名": "レポート {YYYY-MM-DD} {HH:MM}"}`（現在日時を使用）
   - **content**: Step 2 で変換したNotion flavored Markdown
3. 作成結果からページURLを取得

### Step 4: Slack通知（Pythonスクリプト経由）

**重要:** 必ず Step 3 の Notion ページ作成が完了し URL を取得してから実行すること。

Slack通知は `scripts/publish_report.py` に `--notion-url` オプションを渡して実行する。
スクリプトが computed tables から達成進捗・クリティカルな課題を自動抽出し、Notion URLを含むメッセージを生成・送信する。

```bash
python3 scripts/publish_report.py --notion-url "{Notion URL}"
```

テストモード（CLAUDE.md参照）の場合:
```bash
SLACK_WEBHOOK_URL="$SLACK_WEBHOOK_URL_TEST" python3 scripts/publish_report.py --notion-url "{Notion URL}"
```

**Slackメッセージの構成**（スクリプトが自動生成）:
- **📊 達成進捗（チャネル別）**: 全チャネルの着電/SAL/商談の達成率と判定
- **🚨 クリティカルな課題**: 重点課題チャネルごとのボトルネック
- **📎 Notion URL**: Step 3 で取得したURL
- **注意**: 課題でIS個人名は出さない（チーム単位・チャネル単位で記述）

### Step 5: 完了報告

ユーザーに以下を報告:
- NotionページURL
- SlackメッセージURL
