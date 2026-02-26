# demo_call_report

デモ電話チームの月次進捗を自動分析し、レポートを生成・配信するツールです。

## 何をするか

1. **データ取得** — Databricks（MCP経由）から6種類のCSVデータを並列取得
2. **テーブル計算** — Pythonスクリプトで分析テーブルを確定計算（LLMによる数値計算を排除）
3. **インサイト生成** — 全テーブルを横断的に読み込み、B2B SaaS営業マネージャー視点でレポートを生成
4. **配信** — Notionへ投稿し、SlackにURL通知

## 運用フロー

```
1. Claudeに「分析して」と指示
2. /fetch-data     → DBXからCSVデータを取得（Agent Teamで並列化）
3. /compute-tables → Pythonによる確定テーブル計算
4. /analyze-and-report → 全テーブル通読 → インサイト生成 → レポート合成
5. /publish-report → Notionに投稿 → SlackにURL通知
```

## フォルダ構成

```
├── CLAUDE.md                # 分析ルール・仕様の定義
├── .claude/skills/          # Claude Code スキル定義
├── .github/workflows/       # GitHub Actions（平日 JST 19:00 に自動実行、祝日スキップ）
├── scripts/
│   ├── run-analysis.sh      # CI/CD用の実行スクリプト
│   ├── compute_tables.py    # 確定テーブル計算（Python標準ライブラリのみ）
│   └── publish_report.py    # Notion投稿 + Slack通知
├── data/                    # CSVデータ（日付サフィックス付き、日次蓄積）
│   └── computed/            # Python計算済みテーブル（自動生成、手動編集禁止）
├── reports/                 # 生成されたMarkdownレポート
└── logs/                    # 実行ログ
```

## 入力データ（6種類のCSV）

| # | ファイル | 用途 | 粒度 |
| --- | ------- | ---- | ---- |
| Q1 | 着地予想 | 着電数の着地予測 | 日 x チャネル |
| Q2 | SAL着予 | SAL（アポ獲得）の着地予測 | 日 x チャネル |
| Q3 | 商談実施着予 | 商談実施数の着地予測 | 日 x チャネル |
| Q4 | デモ電話 | リード単位の実績明細 | リード単位 |
| Q5 | SAL率_積み上げ | リード獲得〜SALまでの日数分布 | リード単位 |
| Q6 | デモ電話_商談 | 商談明細（リード→商談の紐付き） | 商談単位 |

## 計算の原則

- **数値計算はPythonが行う** — 集計・率の算出・前月比はすべて `scripts/compute_tables.py` で実行
- **LLMはインサイトのみ** — テーブルの数値はPython出力をそのまま使用し、LLMは分析・提案のみ担当
- **テーブル間の横断解釈を重視** — インサイト生成は単一Agentが全テーブルを通読して統合分析

## CI/CD

GitHub Actionsで平日 JST 19:00（UTC 10:00）に自動実行されます。土日はcronで除外、祝日は `jpholiday` でスキップします。手動実行も可能です。

```text
データ取得 → テーブル計算 → レポート生成 → git commit/push → Notion投稿 → Slack通知
```

## 使い方

Claude Codeで「分析して」と指示すると、データ取得→計算→分析→レポート生成→配信まで一気通貫で実行されます。
# demo_call_report
