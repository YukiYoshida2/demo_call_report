---
name: fetch-data
description: Databricks MCP経由でQ1〜Q6のCSVデータを取得する。小規模(Q1-Q3)は直接取得、大規模(Q4-Q6)はチャンク分割で取得。Agent Teamでの並列取得に対応。
---

# データ取得スキル

MCP経由でDatabricksに接続し、6つのクエリを実行してCSVデータを取得する。

## SQLクエリ

全クエリは [queries.sql](queries.sql) に格納。`[QUERY_START:Q1]` 〜 `[QUERY_END:Q1]` のマーカーで各クエリを識別する。
**クエリの変更は禁止。そのまま実行すること。**

## Step 0: 環境確認

1. `databricks_discover` でDBX環境（warehouse、接続状態）を確認
2. `data/` ディレクトリを作成（なければ）

## Step 1: Q1〜Q3（小規模データ: ~170行）

Q1〜Q3は行数が少ないため、`invoke_databricks_cli` の `experimental aitools tools query` で直接取得可能。

1. 各クエリをそのまま実行（**クエリ変更禁止**）
2. 返却されたJSON結果をPythonスクリプトでCSVに変換保存
3. 保存先: `data/{クエリ名}-{YYYY-MM-DD}.csv`

## Step 2: Q4〜Q6（大規模データ: 数千〜2万行）

Q4〜Q6は行数が多く、MCP queryの出力上限に引っかかる。
**元クエリは変更せず**、ラッパーで分割取得する。

### ラッパー分割取得の手順

```
1. 元クエリをサブクエリとして囲み、LIMIT/OFFSETで分割取得
   例: SELECT * FROM ({元クエリ}) AS sub LIMIT 500 OFFSET 0
        SELECT * FROM ({元クエリ}) AS sub LIMIT 500 OFFSET 500
        ...
2. 各チャンクのJSON結果をPythonでCSVに変換
3. 全チャンクを結合して1つのCSVファイルに保存
4. 行数を検証（全行取得できたことを確認）
```

### チャンクサイズの目安

- 500行/チャンクを基本とする
- 出力上限エラーが出たら250行に縮小
- Q4（~20,000行）: 約40チャンク
- Q5（数千行）: 約10〜20チャンク
- Q6（~13,000行）: 約26チャンク

### チャンクの並列取得（重要: 必ず実施）

1ターンで複数のツール呼び出しを並列実行することで取得ターン数を大幅削減する。

```
❌ 逐次取得（従来）:
  OFFSET=0 → 待機 → OFFSET=500 → 待機 → OFFSET=1000 → ... （40ターン）

✅ 並列取得（改善後）:
  バッチ1: [OFFSET=0, OFFSET=500, OFFSET=1000, ..., OFFSET=4500] を同時発行 → 待機
  バッチ2: [OFFSET=5000, OFFSET=5500, ..., OFFSET=9500] を同時発行 → 待機
  ...（4ターンで完了）
```

**バッチサイズ**: 1バッチあたり10チャンクを並列発行する。
- Q4（~20,000行 / 500行チャンク = 40チャンク）: 4バッチ
- Q5（~5,000行 / 500行チャンク = 10チャンク）: 1バッチ
- Q6（~13,000行 / 500行チャンク = 26チャンク）: 3バッチ

**エラー時の対応**: 並列バッチ内で一部チャンクがエラーになった場合、そのチャンクのみ単独で再取得する。

### 失敗時の対策

- タイムアウト → ユーザーに報告し、warehouse変更を相談
- チャンク取得でエラー → チャンクサイズを半分に縮小して再試行
- ファイル保存された場合 → Readツールで分割読み込みしてCSV変換

## Step 3: データ検証

1. 各CSVファイルの行数を確認
2. 先頭数行のデータ内容を確認
3. Q4: 必須13カラムの存在確認:
   `id, reasons_for_ineligible_leads, inflow_route_media, cv_content_sub__c, is_connect, is_sal, is_task_complete, created_date_jst, month, business_hours_class, is_holiday, phone_type_flag, user_name`
4. Q5: 必須カラムの存在確認:
   `created_date_jst, demo_call_type_summary_v2, cv_content_sub__c, total_leads, total_sal, sal_within_1d, sal_within_3d, sal_7d_diff, sal_14d_diff, sal_21d_diff, sal_30d_diff, sal_after_30d`
5. Q4: `created_date_jst` から月を算出可能であること確認
6. 前日CSVが存在する場合、Q4の行数変動が±20%を超えていたら警告を出力

## Agent Team並列化パターン

大規模データ取得時はAgent Teamで並列化を推奨:

```
Team Lead（メイン）
  ├── Agent A: Q1 + Q2 + Q3 を取得（小規模、即完了）
  ├── Agent B: Q4 取得（~20,000行、チャンク分割）
  ├── Agent C: Q5 取得（数千行、チャンク分割）
  └── Agent D: Q6 取得（~13,000行、チャンク分割）
```

各Agentは `data/` ディレクトリにCSVを保存する。全Agent完了後、Team Leadがデータ検証を実行。

## CSVカラム定義

### Q1〜Q3 共通スキーマ

| カラム | 型 | 説明 |
|--------|---|------|
| `lead_date` | date | 日付 |
| `dimension` | string | チャネル（DIS / LIS / TOP / FAX・EDM / その他 / 全体） |
| `daily_leads` | int | その日の件数 |
| `cumulative_actual` | int | 当月累計実績 |
| `landing_forecast` | float | 着地予測値 |
| `monthly_target` | int | 月間目標 |
| `achievement_pct` | float | 達成率（着地予測 / 月間目標）。1.0 = 100% |

### Q4: デモ電話

| カラム | 型 | 説明 | 使い方 |
|--------|---|------|--------|
| `id` | string | リードID（一意キー） | COUNT(DISTINCT id) でリード数を算出 |
| `reasons_for_ineligible_leads` | string | 不適格リード理由 | **NULLのもののみ集計対象** |
| `inflow_route_media` | string | チャネル | チャネル軸 |
| `cv_content_sub__c` | string | CVコンテンツ小分類 | 広告/LP単位の分析軸 |
| `is_connect` | int (0/1) | コネクト済みフラグ | コネクト率算出 |
| `is_sal` | int (0/1) | SAL済みフラグ | SAL率算出 |
| `is_task_complete` | string | タスク完了状態 | タスク完了割合算出 |
| `created_date_jst` | datetime | リード作成日時（JST） | 日別・週別集計の軸 |
| `month` | date | 月（YYYY-MM-01） | 当月フィルタ |
| `business_hours_class` | string | 営業時間区分 | 時間帯分析 |
| `is_holiday` | string | 平日/休日 | 曜日分析 |
| `phone_type_flag` | string | 電話種別 | 電話種別分析 |
| `user_name` | string | 担当者名 | 担当者別分析 |

### Q5: SAL率_積み上げ

| カラム | 型 | 説明 |
|--------|---|------|
| `created_date_jst` | datetime | リード作成日時 |
| `demo_call_type_summary_v2` | string | チャネル |
| `cv_content_sub__c` | string | CVコンテンツ小分類 |
| `total_leads` | int | リード数 |
| `total_sal` | int | SAL数 |
| `sal_within_1d` | int | 1日以内SAL数 |
| `sal_within_3d` | int | 3日以内SAL数 |
| `sal_7d_diff` | int | 4〜7日目のSAL数（差分） |
| `sal_14d_diff` | int | 8〜14日目のSAL数（差分） |
| `sal_21d_diff` | int | 15〜21日目のSAL数（差分） |
| `sal_30d_diff` | int | 22〜30日目のSAL数（差分） |
| `sal_after_30d` | int | 31日以上のSAL数 |

### Q6: デモ電話_商談

| カラム | 型 | 説明 |
|--------|---|------|
| `created_date` | datetime | リード作成日 |
| `f_initial_deal_acquisition_date` | date | 初回商談獲得日 |
| `first_meeting_date` | date | 初回面談日 |
| `business_meeting_scheduled_date` | date | 商談予定日 |
| `inflow_route_media_lasttouch` | string | チャネル（ラストタッチ） |
| `cv_content_sub_lasttouch` | string | CVコンテンツ小分類（ラストタッチ） |
| `stage_name` | string | 商談ステージ |
| `reasons_not_negotiated` | string | 商談不成立理由 |
| `scheduled_initialdead_interval_days` | int | 商談設定〜初回商談の日数 |
| `scheduled_initialdead_interval_days_category` | string | 上記の区分 |
