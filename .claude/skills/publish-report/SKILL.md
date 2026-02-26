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

1. Notion enhanced markdown spec を `notion://docs/enhanced-markdown-spec` で確認（初回のみ）
2. `notion-create-pages` で以下のパラメータでページ作成:
   - **parent**: `{"data_source_id": "311eea80-adae-80a5-a798-000bc1a1a73f"}`
   - **properties**: `{"ページ名": "レポート {YYYY-MM-DD} {HH:MM}"}`（現在日時を使用）
   - **content**: Step 2 で変換したNotion flavored Markdown
3. 作成結果からページURLを取得

### Step 4: Slack通知

1. **computed tables から直接データを読み取る**（レポートのエグゼクティブサマリではなく）:
   - `data/computed/step1_着電着予.md`, `step1_SAL着予.md`, `step1_商談実施着予.md` → チャネル別の達成率・判定
   - `data/computed/step1_課題チャネル.md` → 重点課題チャネルの特定
   - `data/computed/step2_ファネル転換率.md` → CN率・SAL率の前月比（📉マーク）
   - `data/computed/step2_CVコンテンツ.md` → 課題チャネルのボトルネックCV特定

2. 以下の **2セクション** でサマリを構成する:
   - **📊 達成進捗（チャネル別）**: 全チャネルの着電/SAL/商談の達成率と判定を1行ずつ。数値はcomputed tableからそのまま転記（再計算禁止）
   - **🚨 クリティカルな課題**: 重点課題チャネルごとに「CN率が低いのか / SAL率が低いのか → その中でどの流入経路（CV）が悪いのか」の順で構造化

3. `slack_send_message` で以下を投稿:
   - **channel_id**: `C08PMM3C601`
   - **message**: 実行日時・サマリ・NotionページURLを含むメッセージ
   - **注意**: 課題でIS個人名を出さない（チーム単位・チャネル単位で記述する）

```
<@U07EJ6YKUPK> <@U05V0RAF09M> <@U07LNE4G2R0>
デモ電話チーム 月次進捗レポート（YYYY年MM月DD日 HH:MM 実行）
📅 参照期間: YYYY-MM-DD 〜 YYYY-MM-DD

📊 *達成進捗（チャネル別）*
• 全体: 着電{X}%{判定} / SAL{Y}%{判定} / 商談{Z}%{判定}
• DIS: 着電{X}%{判定} / SAL{Y}%{判定} / 商談{Z}%{判定}
• LIS: ...
• TOP: ...
• FAX・EDM: ...
• その他: ...

🚨 *クリティカルな課題*
• {チャネル}: {CN率 or SAL率}が{値}（{前月比pp📉}）
  → {ボトルネックCV1}({問題の率}), {CV2}({問題の率})
• {チャネル}: ...

📎 {Notion URL}
```

### Step 5: 完了報告

ユーザーに以下を報告:
- NotionページURL
- SlackメッセージURL
