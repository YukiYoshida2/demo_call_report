# demo_call_report

デモ電話チームの月次進捗を自動分析し、レポートを生成・配信するパイプライン。

## 処理の全体フロー

```
[launchd / 手動]
       │
       ▼
 scripts/run-local.sh
       │  .env 読み込み → 環境変数チェック → 土日祝スキップ判定
       │
       ▼
 claude -p "分析して"
       │
       │  ┌──────────────────────────────────────────────────┐
       │  │ Claude Code が 4つのスキルを順に実行              │
       │  │                                                  │
       │  │  1. /fetch-data                                  │
       │  │     Databricks MCP → data/YYYY-MM-DD/*.csv       │
       │  │                                                  │
       │  │  2. /compute-tables                              │
       │  │     python3 compute_tables.py → data/computed/   │
       │  │                                                  │
       │  │  3. /analyze-and-report                          │
       │  │     computed tables を通読 → reports/*.md        │
       │  │                                                  │
       │  │  4. /publish-report                              │
       │  │     Notion MCP でページ作成 → URL 取得           │
       │  │     publish_report.py --notion-url <URL>         │
       │  │     → Slack Webhook で通知                       │
       │  └──────────────────────────────────────────────────┘
       │
       ▼
     完了
```

## 各ステップの詳細

### 1. /fetch-data — データ取得

Databricks MCP 経由で 6 つの SQL クエリを実行し、CSV を `data/YYYY-MM-DD/` に保存する。

- **Q1〜Q3**（着地予想・SAL着予・商談実施着予）: 各24行程度。Agent A が直接取得
- **Q4**（デモ電話）: 約20,000行。Agent B が LIMIT/OFFSET チャンク分割で取得
- **Q5**（SAL率_積み上げ）: 数千行。Agent C がチャンク分割で取得
- **Q6**（デモ電話_商談）: 約13,000行。Agent D がチャンク分割で取得

4つのフォアグラウンド Agent を同時起動し、全完了をブロッキングで待つ。

**入出力:**
```
Databricks SQL  →  data/YYYY-MM-DD/着地予想-YYYY-MM-DD.csv
                   data/YYYY-MM-DD/SAL着予-YYYY-MM-DD.csv
                   data/YYYY-MM-DD/商談実施着予-YYYY-MM-DD.csv
                   data/YYYY-MM-DD/デモ電話-YYYY-MM-DD.csv
                   data/YYYY-MM-DD/SAL率_積み上げ-YYYY-MM-DD.csv
                   data/YYYY-MM-DD/デモ電話_商談-YYYY-MM-DD.csv
```

### 2. /compute-tables — テーブル計算

```bash
python3 scripts/compute_tables.py --date YYYY-MM-DD
```

CSV から 13 個の Markdown テーブルを `data/computed/` に生成する。Python 標準ライブラリのみ使用。

LLM は数値計算を行わない。COUNT, SUM, 率の算出、前月比の計算はすべてこのスクリプトが担う。

**出力ファイル:**

| ファイル | 内容 |
|---------|------|
| `_validation.md` | データ検証結果（行数・スキーマチェック） |
| `step1_着電着予.md` | 着電の着地予測 vs 目標 |
| `step1_SAL着予.md` | SAL の着地予測 vs 目標 |
| `step1_商談実施着予.md` | 商談実施の着地予測 vs 目標 |
| `step1_課題チャネル.md` | 課題チャネル一覧（Bad数ランキング） |
| `step2_ファネル転換率.md` | チャネル別 CN率・SAL率 + 前月比 |
| `step2_CVコンテンツ.md` | チャネル別 CV コンテンツ Top10 |
| `step2_SALスピード.md` | SAL 日数分布（1日/3日/7日/14日/30日以内） |
| `step2_時系列.md` | 週別・営業時間帯別・平休日別トレンド |
| `step2_担当者サマリ.md` | 担当者別パフォーマンスサマリ |
| `step2_担当者チャネル.md` | 担当者 x チャネルのクロス分析 |
| `step2_インパクト試算.md` | 担当者要因の SAL インパクト試算 |
| `step2_週次急落.md` | 週次で急落した担当者の検知 |

### 3. /analyze-and-report — レポート生成

単一の Claude Agent が `data/computed/` の全 12 テーブルを通読し、Markdown レポートを生成する。

- テーブル間の横断解釈を重視するため、Agent 並列に分割しない
- computed table の数値は一切変更しない（そのまま転記）
- LLM が担うのはインサイト・所見・アクション提案のみ

**入出力:**
```
data/computed/step*.md  →  reports/レポート-YYYY-MM-DD.md
```

### 4. /publish-report — Notion 投稿 + Slack 通知

2段階で実行する。Notion URL を取得してからでないと Slack 通知しない。

**Step A: Notion ページ作成（MCP 経由）**

1. レポート内の Markdown テーブルを Notion `<table>` 形式に変換
2. `notion-create-pages` MCP ツールでページ作成
3. 作成結果から URL を取得

**Step B: Slack 通知（Python スクリプト）**

```bash
python3 scripts/publish_report.py --notion-url "<Step A で取得した URL>"
```

- `data/computed/` から達成進捗・クリティカルな課題を自動抽出
- Notion URL を含む Slack メッセージを構成・送信
- **Notion URL が空の場合は Slack 送信をブロックして exit 1 で停止する**（URL なし通知の防止）

## スクリプト一覧

| ファイル | 役割 | 実行タイミング |
|---------|------|-------------|
| `scripts/run-local.sh` | パイプライン全体のエントリポイント。.env 読み込み → 土日祝スキップ → `claude -p "分析して"` 実行 | launchd（平日）/ 手動 |
| `scripts/compute_tables.py` | CSV → 13 テーブルの確定計算。Python 標準ライブラリのみ | `/compute-tables` スキルから呼ばれる |
| `scripts/publish_report.py` | Slack 通知。computed tables からサマリを自動構成。`--notion-url` 必須 | `/publish-report` スキルから呼ばれる |

## フォルダ構成

```
├── CLAUDE.md                    # 分析ルール・仕様の定義（スキルが参照）
├── .claude/skills/              # Claude Code スキル定義
│   ├── fetch-data/              #   データ取得
│   ├── compute-tables/          #   テーブル計算
│   ├── analyze-and-report/      #   インサイト生成 + レポート合成
│   └── publish-report/          #   Notion投稿 + Slack通知
├── scripts/
│   ├── run-local.sh             # launchd / 手動実行のエントリポイント
│   ├── compute_tables.py        # 確定テーブル計算
│   └── publish_report.py        # Slack通知（Notion URL必須）
├── data/
│   ├── YYYY-MM-DD/              # 取得日ごとの CSV（日次蓄積）
│   └── computed/                # Python 計算済みテーブル（自動生成、手動編集禁止）
├── reports/                     # 生成された Markdown レポート（日付付きで蓄積）
├── logs/                        # 実行ログ（30日超で自動削除）
└── .env                         # 環境変数（git管理外）
```

## 環境変数（.env）

| 変数 | 必須 | 用途 |
|------|------|------|
| `DATABRICKS_HOST` | Yes | Databricks ワークスペース URL |
| `DATABRICKS_TOKEN` | Yes | Databricks アクセストークン |
| `SLACK_WEBHOOK_URL` | Yes | Slack Incoming Webhook URL（本番チャネル） |
| `SLACK_WEBHOOK_URL_TEST` | No | テスト用 Slack Webhook URL |
| `NOTION_API_KEY` | No | Notion Internal Integration トークン（Python 直接投稿時のみ） |

## テストモード

「テストで分析して」と指示すると、Slack 通知先がテスト用チャネルに切り替わる。

```bash
SLACK_WEBHOOK_URL="$SLACK_WEBHOOK_URL_TEST" python3 scripts/publish_report.py --notion-url "<URL>"
```

Notion 投稿先は本番と同じ（テスト用 DB は分けていない）。
