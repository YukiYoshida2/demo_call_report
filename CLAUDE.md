デモ電話チーム 月次進捗分析プラン

## 概要

MCP経由でDatabricks（DBX）の6つのクエリを実行し、取得したデータからMarkdownレポートを自動生成する。
**レポートの視点**: B2B SaaSの凄腕営業マネージャーとして、目標達成リスクを鋭く見抜き、具体的なアクションにつながるレポートを作成する。

## 運用フロー

```
1. Claudeに「分析して」と指示
2. /fetch-data でDBXからCSVデータを取得（Agent Teamで並列化）
3. /compute-tables でPythonスクリプトによる確定テーブル計算を実行
4. /analyze-and-report で全テーブルを通読しインサイト生成 → レポート合成
5. /publish-report でNotionに投稿 → SlackにURL通知
```

## 計算の原則

1. **数値計算はPythonが行う**: COUNT, SUM, 率の算出、前月比の計算は全て `scripts/compute_tables.py` で実行する
2. **LLMはインサイトのみ生成**: テーブル内の数値はPythonの出力をそのまま使用し、LLMは所見・分析コメント・アクション提案のみを担当する
3. **テーブルの数値変更は禁止**: computed tableの数値をLLMが変更することは禁止。誤りを発見した場合はPythonスクリプトを修正する
4. **テーブル間の横断解釈を重視**: インサイト生成はAgent Teamに分割せず、単一Agentが全テーブルを見て統合的に分析する

## グローバルフィルタ条件（全STEPに共通）

1. **リード数** = `COUNT(DISTINCT id)`（重複排除）
2. **集計対象** = `reasons_for_ineligible_leads` が空文字 or NULL のレコードのみ
3. **当月判定** = CSVファイル内の最新月のデータを「当月」として扱う
4. **前月判定** = 当月の1ヶ月前を「前月」として扱う（例: 当月=2026-02 → 前月=2026-01）

## 前月比の表記ルール

- 率の比較 → 差分をpp（ポイント）で表記。例: 当月45% vs 前月50% → **-5.0pp**
- 件数の比較 → 増減率で表記。例: 当月100件 vs 前月120件 → **-16.7%**
- 悪化判定 → 前月比でCN率またはSAL率が **-5pp以上悪化** の場合に📉マークを付ける

## 重要な制約事項

- **クエリの変更は禁止**: 共有されたSQLクエリは一切変更してはならない
- **CSVデータの書き換えは禁止**: 取得したCSVデータの値を加工・修正してはならない

## 入力CSVファイル

| # | ファイル名 | 用途 | 粒度 |
|---|-----------|------|------|
| Q1 | `着地予想-YYYY-MM-DD.csv` | 着電数の着地予測 | 日 × チャネル |
| Q2 | `SAL着予-YYYY-MM-DD.csv` | SAL（アポ獲得）の着地予測 | 日 × チャネル |
| Q3 | `商談実施着予-YYYY-MM-DD.csv` | 商談実施数の着地予測 | 日 × チャネル |
| Q4 | `デモ電話-YYYY-MM-DD.csv` | リード単位の実績明細 | リード単位 |
| Q5 | `SAL率_積み上げ-YYYY-MM-DD.csv` | リード獲得〜SALまでの日数分布 | リード単位 |
| Q6 | `デモ電話_商談-YYYY-MM-DD.csv` | 商談明細（リード→商談の紐付き） | 商談単位 |

## 前月比較のデータ可用性

| CSV | 前月データ | 前月比較の方法 |
|-----|----------|-------------|
| Q1〜Q3 | 当月のみ | 前月分のCSVも一緒にフォルダに入れてもらう。なければスキップ |
| Q4 | `month`カラムに過去月あり | CSV内の前月データを直接使用 |
| Q5 | `created_date_jst`に過去月あり | CSV内の前月データを直接使用 |

## フォルダ構成

```
デモ電話/
├── CLAUDE.md             # このファイル
├── scripts/
│   ├── run-analysis.sh   # CI/CD用の実行スクリプト
│   └── compute_tables.py # 確定テーブル計算（Python標準ライブラリのみ）
├── data/                 # CSVデータ（日付サフィックス付き、日次蓄積）
│   ├── 着地予想-YYYY-MM-DD.csv
│   ├── SAL着予-YYYY-MM-DD.csv
│   ├── 商談実施着予-YYYY-MM-DD.csv
│   ├── デモ電話-YYYY-MM-DD.csv
│   ├── SAL率_積み上げ-YYYY-MM-DD.csv
│   ├── デモ電話_商談-YYYY-MM-DD.csv
│   └── computed/         # Python計算済みテーブル（自動生成）
│       ├── _validation.md
│       ├── step1_*.md
│       └── step2_*.md
└── reports/              # 生成されたMarkdownレポート
    └── レポート-YYYY-MM-DD.md
```

- `data/` にはCSVが日付付きで蓄積される。前月Q1〜Q3 CSVも同フォルダに置くことで前月比較が可能
- `data/computed/` はPythonスクリプトが自動生成する。手動編集禁止
- `reports/` にはレポートが日付付きで蓄積される

## データ取得の並列化（Agent Team）

`/fetch-data` のみAgent Teamで並列化する（MCP応答をメインコンテキストから隔離するため）:
```
Team Lead → Agent A: Q1+Q2+Q3（小規模、即完了）
          → Agent B: Q4（~20,000行、チャンク分割）
          → Agent C: Q5（数千行、チャンク分割）
          → Agent D: Q6（~13,000行、チャンク分割）
```

※ `/analyze-and-report` はAgent Teamに分割しない。テーブル間の横断解釈を維持するため、単一Agentが全computed tablesを通読して分析する。
