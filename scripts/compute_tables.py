#!/usr/bin/env python3
"""
デモ電話チーム 月次進捗分析 — 確定計算スクリプト

全分析テーブルをPythonで計算し、Markdownテーブルとして data/computed/ に出力する。
LLMによる計算を排除し、再現可能で正確な数値を保証する。

Usage:
    python3 scripts/compute_tables.py --date 2026-02-25
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# ================================================================
# 定数
# ================================================================

IS_REPS = ["湯本 隆嗣", "永野 雪", "村松 亜茉音", "中本 陽介"]
OUTSOURCE_REPS = ["中里 奎太", "金 旭光"]
CHANNEL_ORDER = ["全体", "TOP", "LIS", "DIS", "FAX・EDM", "その他"]
CHANNELS = ["TOP", "LIS", "DIS", "FAX・EDM", "その他"]

PP_WORSEN = -5.0
BELOW_AVG_RATIO = 0.20
IMPACT_WARN = -3
IMPACT_CRIT = -5
WEEKLY_DROP = -15.0
CROSS_CHANNEL_WARN = -10.0

CSV_PREFIXES = {
    "q1": "着地予想",
    "q2": "SAL着予",
    "q3": "商談実施着予",
    "q4": "デモ電話",
    "q5": "SAL率_積み上げ",
    "q6": "デモ電話_商談",
}

Q4_REQUIRED = [
    "id", "reasons_for_ineligible_leads", "inflow_route_media",
    "cv_content_sub__c", "is_connect", "is_sal", "is_task_complete",
    "created_date_jst", "month", "business_hours_class", "is_holiday",
    "phone_type_flag", "user_name",
]

Q5_REQUIRED = [
    "created_date_jst", "demo_call_type_summary_v2", "cv_content_sub__c",
    "total_leads", "total_sal", "sal_within_1d", "sal_within_3d",
    "sal_7d_diff", "sal_14d_diff", "sal_21d_diff", "sal_30d_diff",
    "sal_after_30d",
]


# ================================================================
# ユーティリティ
# ================================================================

def safe_div(num, den):
    if den == 0:
        return None
    return num / den


def fmt_pct(val, dec=1):
    if val is None:
        return "-"
    return f"{val * 100:.{dec}f}%"


def fmt_pp(val, warn_threshold=PP_WORSEN):
    if val is None:
        return "N/A"
    sign = "+" if val >= 0 else ""
    mark = "📉" if val <= warn_threshold else ""
    return f"{sign}{val:.1f}pp{mark}"


def fmt_count_diff(cur, prev):
    if prev is None or prev == 0:
        return "N/A"
    diff_pct = (cur - prev) / prev * 100
    sign = "+" if diff_pct >= 0 else ""
    return f"{sign}{diff_pct:.1f}%"


def fmt_int(val):
    if val is None:
        return "-"
    return f"{val:,}"


def prev_month_str(ym):
    """'2026-02' → '2026-01'"""
    y, m = int(ym[:4]), int(ym[5:7])
    if m == 1:
        return f"{y - 1}-12"
    return f"{y}-{m - 1:02d}"


def parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, IndexError):
        return None


def iso_week_label(d):
    iso = d.isocalendar()
    monday = d - timedelta(days=d.weekday())
    return f"W{iso[1]:02d} ({monday.month}/{monday.day}-)"


def iso_week_key(d):
    iso = d.isocalendar()
    return (iso[0], iso[1])


def md_table(headers, rows):
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def frontmatter(data_date, current_month, previous_month,
                 period_start="", period_end=""):
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return f"""---
computed_at: {now}
data_date: {data_date}
current_month: {current_month}
previous_month: {previous_month}
period_start: {period_start}
period_end: {period_end}
---

"""


def write_file(path, content):
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


# ================================================================
# データ読み込み
# ================================================================

def find_csv(data_dir, query_id, date_str):
    prefix = CSV_PREFIXES[query_id]
    expected = f"{prefix}-{date_str}.csv"
    path = data_dir / expected
    return path if path.exists() else None


def find_prev_month_csv(data_dir, query_id, current_date_str):
    prefix = CSV_PREFIXES[query_id]
    current = datetime.strptime(current_date_str, "%Y-%m-%d").date()
    if current.month == 1:
        prev_y, prev_m = current.year - 1, 12
    else:
        prev_y, prev_m = current.year, current.month - 1

    candidates = []
    for f in sorted(data_dir.iterdir()):
        if not f.name.startswith(prefix + "-"):
            continue
        date_part = f.name[len(prefix) + 1 : -4]  # "着地予想-2026-01-25.csv" → "2026-01-25"
        try:
            file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
            if file_date.year == prev_y and file_date.month == prev_m:
                candidates.append(f)
        except ValueError:
            pass
    return candidates[-1] if candidates else None


def load_csv_file(filepath):
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ================================================================
# グローバルフィルタ
# ================================================================

def is_eligible(row):
    val = row.get("reasons_for_ineligible_leads", "")
    return val == "" or val.lower() == "null"


def get_row_month(row):
    m = row.get("month", "")
    if m and m != "" and m.lower() != "null":
        return m[:7]
    dt = row.get("created_date_jst", "")
    if dt:
        return dt[:7]
    return None


def detect_current_month_q4(rows):
    months = set()
    for row in rows:
        if is_eligible(row):
            m = get_row_month(row)
            if m:
                months.add(m)
    return sorted(months)[-1] if months else None


def detect_current_month_q5(rows):
    months = set()
    for row in rows:
        dt = row.get("created_date_jst", "")
        if dt and len(dt) >= 7:
            months.add(dt[:7])
    return sorted(months)[-1] if months else None


def filter_q4(rows, month_str):
    return [r for r in rows if is_eligible(r) and get_row_month(r) == month_str]


def filter_q5(rows, month_str):
    return [r for r in rows if r.get("created_date_jst", "")[:7] == month_str]


# ================================================================
# Q1-Q3 前月フォールバック（前月CSVがない場合、Q4/Q6から代替計算）
# ================================================================

def build_prev_actuals_from_q4(q4_prev):
    """Q4前月データから各チャネルの着電数を集計。Q1前月CSVの代替。"""
    ch_ids = defaultdict(set)
    for r in q4_prev:
        ch = r.get("inflow_route_media", "その他")
        rid = r.get("id", "")
        if rid:
            ch_ids[ch].add(rid)
    result = {}
    total = 0
    for ch in CHANNELS:
        c = len(ch_ids.get(ch, set()))
        result[ch] = c
        total += c
    result["全体"] = total
    return result


def build_prev_sal_from_q4(q4_prev):
    """Q4前月データから各チャネルのSAL数を集計。Q2前月CSVの代替。"""
    ch_ids = defaultdict(set)
    for r in q4_prev:
        if str(r.get("is_sal", "0")) == "1":
            ch = r.get("inflow_route_media", "その他")
            rid = r.get("id", "")
            if rid:
                ch_ids[ch].add(rid)
    result = {}
    total = 0
    for ch in CHANNELS:
        c = len(ch_ids.get(ch, set()))
        result[ch] = c
        total += c
    result["全体"] = total
    return result


def build_prev_meetings_from_q6(q6, prev_month):
    """Q6から前月の商談実施数をチャネル別に集計。Q3前月CSVの代替。"""
    valid_channels = {"TOP", "LIS", "DIS", "FAX・EDM"}
    ch_count = defaultdict(int)
    for r in q6:
        fmt = r.get("first_meeting_date", "")
        if not fmt or fmt[:7] != prev_month:
            continue
        ch = r.get("inflow_route_media_lasttouch", "")
        if ch not in valid_channels:
            ch = "その他"
        ch_count[ch] += 1
    result = {}
    total = 0
    for ch in CHANNELS:
        c = ch_count.get(ch, 0)
        result[ch] = c
        total += c
    result["全体"] = total
    return result


def extract_targets(q_rows):
    """Q1-Q3 CSVからチャネル別月間目標を抽出。"""
    targets = {}
    for row in q_rows:
        dim = row.get("dimension", "")
        t = row.get("monthly_target", "")
        if dim and t:
            targets[dim] = int(t)
    return targets


# ================================================================
# 集計ヘルパー
# ================================================================

def count_distinct(rows, key="id"):
    return len(set(r[key] for r in rows if r.get(key)))


def count_distinct_where(rows, predicate, key="id"):
    return len(set(r[key] for r in rows if predicate(r) and r.get(key)))


def compute_funnel(rows):
    leads = count_distinct(rows)
    connects = count_distinct_where(
        rows, lambda r: str(r.get("is_connect", "0")) == "1"
    )
    sals = count_distinct_where(
        rows, lambda r: str(r.get("is_sal", "0")) == "1"
    )
    tasks = count_distinct_where(
        rows, lambda r: r.get("is_task_complete", "") == "完了"
    )
    return {
        "leads": leads,
        "connects": connects,
        "sals": sals,
        "tasks": tasks,
        "cn_rate": safe_div(connects, leads),
        "sal_rate": safe_div(sals, connects),
        "task_rate": safe_div(tasks, leads),
    }


def group_by(rows, key_func):
    groups = defaultdict(list)
    for row in rows:
        k = key_func(row)
        if k is not None:
            groups[k].append(row)
    return dict(groups)


def classify_user(user_name):
    if user_name in IS_REPS:
        return user_name
    if user_name in OUTSOURCE_REPS:
        return "外注（合算）"
    return None


def pp_diff(cur_rate, prev_rate):
    if cur_rate is not None and prev_rate is not None:
        return (cur_rate - prev_rate) * 100
    return None


# ================================================================
# STEP 1: 数値進捗サマリ
# ================================================================

def compute_step1_landing(q_rows, prev_q_rows, label,
                          fallback_prev_actuals=None):
    if not q_rows:
        return f"※ {label}のCSVデータがありません", {}

    latest = {}
    for row in q_rows:
        dim = row["dimension"]
        ld = row["lead_date"]
        if dim not in latest or ld > latest[dim]["lead_date"]:
            latest[dim] = row

    prev_latest = {}
    if prev_q_rows:
        # 従来通り: 前月CSVから前月達成率を取得
        for row in prev_q_rows:
            dim = row["dimension"]
            ld = row["lead_date"]
            if dim not in prev_latest or ld > prev_latest[dim]["lead_date"]:
                prev_latest[dim] = row
    elif fallback_prev_actuals:
        # フォールバック: Q4/Q6から代替計算した前月確定実績を使用
        targets = extract_targets(q_rows)
        for ch, actual in fallback_prev_actuals.items():
            target = targets.get(ch, 0)
            ach = safe_div(actual, target) if target > 0 else None
            prev_latest[ch] = {"achievement_pct": ach, "lead_date": "fallback"}

    headers = [
        "チャネル", "実績累計", "着地予測", "月目標", "達成率",
        "前月達成率", "前月比", "判定",
    ]
    rows_out = []
    results = {}

    for ch in CHANNEL_ORDER:
        if ch not in latest:
            continue
        r = latest[ch]
        cum = int(r["cumulative_actual"]) if r["cumulative_actual"] else 0
        forecast_raw = r.get("landing_forecast", "")
        forecast = int(float(forecast_raw)) if forecast_raw else None
        target = int(r["monthly_target"]) if r["monthly_target"] else 0
        ach_raw = r.get("achievement_pct", "")
        ach = float(ach_raw) if ach_raw else None

        prev_ach = None
        if ch in prev_latest:
            pa = prev_latest[ch].get("achievement_pct", "")
            if isinstance(pa, (int, float)):
                prev_ach = pa
            elif pa:
                prev_ach = float(pa)

        if ach is not None:
            judgment = "✅" if ach >= 1.0 else "❌"
            ach_display = f"{ach * 100:.0f}%"
        else:
            judgment = "-"
            ach_display = "-"

        prev_display = f"{prev_ach * 100:.0f}%" if prev_ach is not None else "N/A"

        if ach is not None and prev_ach is not None:
            diff_val = (ach - prev_ach) * 100
            diff_display = fmt_pp(diff_val)
        else:
            diff_display = "N/A"

        ch_display = f"**{ch}**" if ch == "全体" else ch
        rows_out.append([
            ch_display, fmt_int(cum),
            fmt_int(forecast) if forecast is not None else "-",
            fmt_int(target), ach_display, prev_display, diff_display, judgment,
        ])
        results[ch] = {"ach": ach, "judgment": judgment, "prev_ach": prev_ach}

    table = md_table(headers, rows_out)
    if not prev_q_rows and not fallback_prev_actuals:
        table += "\n\n※ 前月CSVがフォルダにないため、前月達成率・前月比はN/A"
    elif fallback_prev_actuals:
        table += "\n\n※ 前月達成率はQ4/Q6実績データからの代替計算値"

    return table, results


def compute_step1_issues(results_1, results_2, results_3):
    headers = ["チャネル", "着電", "SAL", "商談実施", "Bad数", "位置づけ"]
    rows_out = []

    for ch in CHANNELS:
        r1 = results_1.get(ch, {})
        r2 = results_2.get(ch, {})
        r3 = results_3.get(ch, {})

        j1 = r1.get("judgment", "-")
        j2 = r2.get("judgment", "-")
        j3 = r3.get("judgment", "-")
        a1 = r1.get("ach")
        a2 = r2.get("ach")
        a3 = r3.get("ach")

        ach1 = f"{j1} {a1 * 100:.0f}%" if a1 is not None else "-"
        ach2 = f"{j2} {a2 * 100:.0f}%" if a2 is not None else "-"
        ach3 = f"{j3} {a3 * 100:.0f}%" if a3 is not None else "-"

        bad_count = sum(1 for j in [j1, j2, j3] if j == "❌")

        if bad_count >= 2:
            position = "**重点課題**"
        elif bad_count == 1:
            position = "課題"
        else:
            position = "-"

        rows_out.append([
            f"**{ch}**" if bad_count >= 2 else ch,
            ach1, ach2, ach3, str(bad_count), position,
        ])

    rows_out.sort(key=lambda r: (-int(r[4]), r[0]))

    issue_channels = [r[0].replace("**", "") for r in rows_out if int(r[4]) > 0]

    table = md_table(headers, rows_out)

    # Add commentary for issue channels
    commentary = []
    for ch in CHANNELS:
        r1 = results_1.get(ch, {})
        r2 = results_2.get(ch, {})
        r3 = results_3.get(ch, {})
        bads = []
        if r1.get("judgment") == "❌":
            bads.append(f"着電{r1['ach'] * 100:.0f}%")
        if r2.get("judgment") == "❌":
            bads.append(f"SAL{r2['ach'] * 100:.0f}%")
        if r3.get("judgment") == "❌":
            bads.append(f"商談{r3['ach'] * 100:.0f}%")
        if bads:
            commentary.append(f"- **{ch}**: {', '.join(bads)}")

    if commentary:
        table += "\n\n" + "\n".join(commentary)

    return table


# ================================================================
# STEP 2-1: ファネル転換率
# ================================================================

def compute_step2_funnel(q4_cur, q4_prev):
    cur_groups = group_by(q4_cur, lambda r: r.get("inflow_route_media", ""))
    prev_groups = group_by(q4_prev, lambda r: r.get("inflow_route_media", ""))

    headers = [
        "チャネル", "リード数", "前月比", "CN率", "前月比",
        "SAL率", "前月比", "タスク完了率", "前月比",
    ]
    rows_out = []
    cur_metrics = {}
    prev_metrics = {}

    for ch in CHANNELS:
        cm = compute_funnel(cur_groups.get(ch, []))
        pm = compute_funnel(prev_groups.get(ch, []))
        cur_metrics[ch] = cm
        prev_metrics[ch] = pm

        rows_out.append([
            ch,
            fmt_int(cm["leads"]),
            fmt_count_diff(cm["leads"], pm["leads"]),
            fmt_pct(cm["cn_rate"]),
            fmt_pp(pp_diff(cm["cn_rate"], pm["cn_rate"])),
            fmt_pct(cm["sal_rate"]),
            fmt_pp(pp_diff(cm["sal_rate"], pm["sal_rate"])),
            fmt_pct(cm["task_rate"]),
            fmt_pp(pp_diff(cm["task_rate"], pm["task_rate"])),
        ])

    main_table = md_table(headers, rows_out)

    # Reference: absolute numbers
    ref_headers = [
        "チャネル", "当月リード", "当月CN", "当月SAL",
        "前月リード", "前月CN", "前月SAL",
    ]
    ref_rows = []
    for ch in CHANNELS:
        cm = cur_metrics[ch]
        pm = prev_metrics[ch]
        ref_rows.append([
            ch,
            fmt_int(cm["leads"]), fmt_int(cm["connects"]), fmt_int(cm["sals"]),
            fmt_int(pm["leads"]), fmt_int(pm["connects"]), fmt_int(pm["sals"]),
        ])

    ref_table = md_table(ref_headers, ref_rows)

    return main_table + "\n\n参考: 絶対数\n\n" + ref_table, cur_metrics, prev_metrics


# ================================================================
# STEP 2-2: CVコンテンツ別
# ================================================================

def compute_step2_cv(q4_cur, q4_prev, cur_channel_metrics):
    output_sections = []

    for ch in CHANNELS:
        ch_rows_cur = [r for r in q4_cur if r.get("inflow_route_media") == ch]
        ch_rows_prev = [r for r in q4_prev if r.get("inflow_route_media") == ch]

        if not ch_rows_cur:
            continue

        ch_avg = cur_channel_metrics.get(ch, {})
        ch_cn_avg = ch_avg.get("cn_rate")
        ch_sal_avg = ch_avg.get("sal_rate")

        cv_groups_cur = group_by(
            ch_rows_cur,
            lambda r: r.get("cv_content_sub__c") or "(空)",
        )
        cv_groups_prev = group_by(
            ch_rows_prev,
            lambda r: r.get("cv_content_sub__c") or "(空)",
        )

        cv_data = []
        for cv, rows in cv_groups_cur.items():
            m = compute_funnel(rows)
            pm = compute_funnel(cv_groups_prev.get(cv, []))
            cv_data.append((cv, m, pm))

        cv_data.sort(key=lambda x: -x[1]["leads"])
        top10 = cv_data[:10]

        headers = [
            "CVコンテンツ", "リード数", "CN率", "差分",
            "SAL率", "差分", "前月CN比", "前月SAL比",
        ]
        rows_out = []

        for cv, m, pm in top10:
            # vs channel average
            cn_vs_avg = pp_diff(m["cn_rate"], ch_cn_avg)
            sal_vs_avg = pp_diff(m["sal_rate"], ch_sal_avg)

            cn_flag = ""
            if (ch_cn_avg and m["cn_rate"] is not None
                    and m["cn_rate"] < ch_cn_avg * (1 - BELOW_AVG_RATIO)):
                cn_flag = "⚠️"
            sal_flag = ""
            if (ch_sal_avg and m["sal_rate"] is not None
                    and m["sal_rate"] < ch_sal_avg * (1 - BELOW_AVG_RATIO)):
                sal_flag = "⚠️"

            cn_diff_s = (
                f"{cn_vs_avg:+.1f}pp{cn_flag}"
                if cn_vs_avg is not None else "-"
            )
            sal_diff_s = (
                f"{sal_vs_avg:+.1f}pp{sal_flag}"
                if sal_vs_avg is not None else "-"
            )

            # vs previous month
            if pm["leads"] == 0:
                cn_prev_s = "新規"
                sal_prev_s = "新規"
            else:
                cn_prev_val = pp_diff(m["cn_rate"], pm["cn_rate"])
                sal_prev_val = pp_diff(m["sal_rate"], pm["sal_rate"])
                cn_prev_s = fmt_pp(cn_prev_val)
                sal_prev_s = fmt_pp(sal_prev_val)

            rows_out.append([
                cv, fmt_int(m["leads"]),
                fmt_pct(m["cn_rate"]), cn_diff_s,
                fmt_pct(m["sal_rate"]), sal_diff_s,
                cn_prev_s, sal_prev_s,
            ])

        ch_section = f"#### {ch} Top10 CVコンテンツ\n\n"
        ch_section += (
            f"チャネル平均: CN率={fmt_pct(ch_cn_avg)}, "
            f"SAL率={fmt_pct(ch_sal_avg)}\n\n"
        )
        ch_section += md_table(headers, rows_out)
        output_sections.append(ch_section)

    return "\n\n".join(output_sections)


# ================================================================
# STEP 2-3: SALスピード分析
# ================================================================

def compute_step2_sal_speed(q5_cur, q5_prev):
    def aggregate(rows):
        groups = group_by(rows, lambda r: r.get("demo_call_type_summary_v2", ""))
        result = {}
        for ch, ch_rows in groups.items():
            if not ch:
                continue
            tl = sum(int(r.get("total_leads", 0)) for r in ch_rows)
            ts = sum(int(r.get("total_sal", 0)) for r in ch_rows)
            w1 = sum(int(r.get("sal_within_1d", 0)) for r in ch_rows)
            w3 = sum(int(r.get("sal_within_3d", 0)) for r in ch_rows)
            d7 = sum(int(r.get("sal_7d_diff", 0)) for r in ch_rows)
            d14 = sum(int(r.get("sal_14d_diff", 0)) for r in ch_rows)
            d21 = sum(int(r.get("sal_21d_diff", 0)) for r in ch_rows)
            d30 = sum(int(r.get("sal_30d_diff", 0)) for r in ch_rows)

            c3 = w1 + w3
            c7 = c3 + d7
            c14 = c7 + d14
            c21 = c14 + d21
            c30 = c21 + d30

            result[ch] = {
                "total_leads": tl, "total_sal": ts,
                "w1d_rate": safe_div(w1, tl),
                "cum_3d_rate": safe_div(c3, tl),
                "cum_7d_rate": safe_div(c7, tl),
                "cum_14d_rate": safe_div(c14, tl),
                "cum_30d_rate": safe_div(c30, tl),
            }
        return result

    cur = aggregate(q5_cur)
    prev = aggregate(q5_prev)

    headers = [
        "チャネル", "リード数", "SAL数", "1日以内", "前月比",
        "3日以内", "前月比", "7日以内", "前月比", "14日以内", "30日以内",
    ]
    rows_out = []

    all_channels = sorted(set(list(cur.keys()) + list(prev.keys())))
    for ch in all_channels:
        cm = cur.get(ch, {})
        pm = prev.get(ch, {})

        rows_out.append([
            ch,
            fmt_int(cm.get("total_leads", 0)),
            fmt_int(cm.get("total_sal", 0)),
            fmt_pct(cm.get("w1d_rate")),
            fmt_pp(pp_diff(cm.get("w1d_rate"), pm.get("w1d_rate"))),
            fmt_pct(cm.get("cum_3d_rate")),
            fmt_pp(pp_diff(cm.get("cum_3d_rate"), pm.get("cum_3d_rate"))),
            fmt_pct(cm.get("cum_7d_rate")),
            fmt_pp(pp_diff(cm.get("cum_7d_rate"), pm.get("cum_7d_rate"))),
            fmt_pct(cm.get("cum_14d_rate")),
            fmt_pct(cm.get("cum_30d_rate")),
        ])

    return md_table(headers, rows_out)


# ================================================================
# STEP 2-4: 時系列トレンド
# ================================================================

def compute_step2_timeseries(q4_cur):
    sections = []

    # Weekly trend per channel
    for ch in CHANNELS:
        ch_rows = [r for r in q4_cur if r.get("inflow_route_media") == ch]
        if not ch_rows:
            continue

        weekly_groups = defaultdict(list)
        for row in ch_rows:
            d = parse_date(row.get("created_date_jst", ""))
            if d:
                weekly_groups[iso_week_key(d)].append(row)

        wk_headers = ["週", "リード数", "CN率", "SAL率"]
        wk_rows = []
        for wk in sorted(weekly_groups.keys()):
            rows = weekly_groups[wk]
            m = compute_funnel(rows)
            dates = [
                parse_date(r.get("created_date_jst", ""))
                for r in rows
                if parse_date(r.get("created_date_jst", ""))
            ]
            label = iso_week_label(min(dates)) if dates else f"W{wk[1]:02d}"
            wk_rows.append([
                label, fmt_int(m["leads"]),
                fmt_pct(m["cn_rate"]), fmt_pct(m["sal_rate"]),
            ])

        if wk_rows:
            sections.append(
                f"#### {ch} 週別トレンド\n\n" + md_table(wk_headers, wk_rows)
            )

    # Business hours comparison
    bh_headers = ["区分", "チャネル", "リード数", "CN率", "SAL率"]
    bh_rows = []
    for ch in CHANNELS:
        ch_rows = [r for r in q4_cur if r.get("inflow_route_media") == ch]
        bh_groups = group_by(ch_rows, lambda r: r.get("business_hours_class", ""))
        for bh in ["営業時間内(10_19)", "営業時間外"]:
            bh_r = bh_groups.get(bh, [])
            if bh_r:
                m = compute_funnel(bh_r)
                lbl = "営業時間内" if "内" in bh else "営業時間外"
                bh_rows.append([
                    lbl, ch, fmt_int(m["leads"]),
                    fmt_pct(m["cn_rate"]), fmt_pct(m["sal_rate"]),
                ])

    if bh_rows:
        sections.append(
            "#### 営業時間帯別比較\n\n" + md_table(bh_headers, bh_rows)
        )

    # Holiday comparison
    hol_headers = ["区分", "チャネル", "リード数", "CN率", "SAL率"]
    hol_rows = []
    for ch in CHANNELS:
        ch_rows = [r for r in q4_cur if r.get("inflow_route_media") == ch]
        hol_groups = group_by(ch_rows, lambda r: r.get("is_holiday", ""))
        for hol in ["平日", "休日"]:
            hol_r = hol_groups.get(hol, [])
            if hol_r:
                m = compute_funnel(hol_r)
                hol_rows.append([
                    hol, ch, fmt_int(m["leads"]),
                    fmt_pct(m["cn_rate"]), fmt_pct(m["sal_rate"]),
                ])

    if hol_rows:
        sections.append(
            "#### 平日/休日比較\n\n" + md_table(hol_headers, hol_rows)
        )

    return "\n\n".join(sections)


# ================================================================
# STEP 2-5: 担当者別パフォーマンス
# ================================================================

def filter_analysis_reps(rows):
    result = []
    for row in rows:
        cls = classify_user(row.get("user_name", ""))
        if cls is not None:
            row_copy = dict(row)
            row_copy["_rep"] = cls
            result.append(row_copy)
    return result


def compute_step2_user_summary(q4_cur, q4_prev):
    cur_reps = filter_analysis_reps(q4_cur)
    prev_reps = filter_analysis_reps(q4_prev)

    overall_cur = compute_funnel(cur_reps)
    overall_prev = compute_funnel(prev_reps)

    cur_groups = group_by(cur_reps, lambda r: r["_rep"])
    prev_groups = group_by(prev_reps, lambda r: r["_rep"])

    headers = [
        "担当者", "リード数", "CN率", "vs平均", "vs前月",
        "SAL率", "vs平均", "vs前月", "タスク完了率", "vs前月", "要注意",
    ]
    rows_out = []

    # Overall average row
    rows_out.append([
        "**全体平均**",
        fmt_int(overall_cur["leads"]),
        fmt_pct(overall_cur["cn_rate"]),
        "-",
        fmt_pp(pp_diff(overall_cur["cn_rate"], overall_prev["cn_rate"])),
        fmt_pct(overall_cur["sal_rate"]),
        "-",
        fmt_pp(pp_diff(overall_cur["sal_rate"], overall_prev["sal_rate"])),
        fmt_pct(overall_cur["task_rate"]),
        fmt_pp(pp_diff(overall_cur["task_rate"], overall_prev["task_rate"])),
        "-",
    ])

    rep_order = IS_REPS + ["外注（合算）"]
    for rep in rep_order:
        cm = compute_funnel(cur_groups.get(rep, []))
        pm = compute_funnel(prev_groups.get(rep, []))

        cn_va = pp_diff(cm["cn_rate"], overall_cur["cn_rate"])
        sal_va = pp_diff(cm["sal_rate"], overall_cur["sal_rate"])
        cn_vp = pp_diff(cm["cn_rate"], pm["cn_rate"])
        sal_vp = pp_diff(cm["sal_rate"], pm["sal_rate"])
        task_vp = pp_diff(cm["task_rate"], pm["task_rate"])

        warnings = []
        if (overall_cur["cn_rate"] and cm["cn_rate"] is not None
                and cm["cn_rate"] < overall_cur["cn_rate"] * (1 - BELOW_AVG_RATIO)):
            warnings.append("⚠️CN率")
        if (overall_cur["sal_rate"] and cm["sal_rate"] is not None
                and cm["sal_rate"] < overall_cur["sal_rate"] * (1 - BELOW_AVG_RATIO)):
            warnings.append("⚠️SAL率")
        if cn_vp is not None and cn_vp <= PP_WORSEN:
            warnings.append("📉CN")
        if sal_vp is not None and sal_vp <= PP_WORSEN:
            warnings.append("📉SAL")

        rep_display = f"**{rep}**" if rep == "外注（合算）" else rep
        rows_out.append([
            rep_display,
            fmt_int(cm["leads"]),
            fmt_pct(cm["cn_rate"]),
            fmt_pp(cn_va, warn_threshold=-999),
            fmt_pp(cn_vp),
            fmt_pct(cm["sal_rate"]),
            fmt_pp(sal_va, warn_threshold=-999),
            fmt_pp(sal_vp),
            fmt_pct(cm["task_rate"]),
            fmt_pp(task_vp),
            ", ".join(warnings) if warnings else "-",
        ])

    return md_table(headers, rows_out)


def compute_step2_user_channel(q4_cur):
    cur_reps = filter_analysis_reps(q4_cur)

    ch_groups = group_by(cur_reps, lambda r: r.get("inflow_route_media", ""))
    ch_avgs = {ch: compute_funnel(rows) for ch, rows in ch_groups.items()}

    headers = [
        "担当者", "チャネル", "リード数", "CN率", "差分",
        "SAL率", "差分", "要注意",
    ]
    rows_out = []

    rep_order = IS_REPS + ["外注（合算）"]
    for rep in rep_order:
        rep_rows = [r for r in cur_reps if r["_rep"] == rep]
        rep_ch = group_by(rep_rows, lambda r: r.get("inflow_route_media", ""))

        for ch in CHANNELS:
            ch_rows = rep_ch.get(ch, [])
            if not ch_rows:
                continue

            m = compute_funnel(ch_rows)
            avg = ch_avgs.get(ch, {})

            cn_d = pp_diff(m["cn_rate"], avg.get("cn_rate"))
            sal_d = pp_diff(m["sal_rate"], avg.get("sal_rate"))

            warns = []
            if cn_d is not None and cn_d <= CROSS_CHANNEL_WARN:
                warns.append("⚠️CN率")
            if sal_d is not None and sal_d <= CROSS_CHANNEL_WARN:
                warns.append("⚠️SAL率")

            rows_out.append([
                rep, ch, fmt_int(m["leads"]),
                fmt_pct(m["cn_rate"]),
                fmt_pp(cn_d, warn_threshold=-999),
                fmt_pct(m["sal_rate"]),
                fmt_pp(sal_d, warn_threshold=-999),
                ", ".join(warns) if warns else "-",
            ])

    return md_table(headers, rows_out)


def compute_step2_user_impact(q4_cur):
    cur_reps = filter_analysis_reps(q4_cur)

    # Channel SAL/leads rate (overall, from analysis reps)
    ch_groups = group_by(cur_reps, lambda r: r.get("inflow_route_media", ""))
    ch_sal_rates = {}
    for ch, rows in ch_groups.items():
        m = compute_funnel(rows)
        ch_sal_rates[ch] = safe_div(m["sals"], m["leads"])

    headers = [
        "チャネル", "担当者", "リード数", "実SAL", "期待SAL", "差分", "判定",
    ]
    rows_out = []

    rep_order = IS_REPS + ["外注（合算）"]
    for ch in CHANNELS:
        ch_reps_rows = [r for r in cur_reps if r.get("inflow_route_media") == ch]
        if not ch_reps_rows:
            continue

        ch_rate = ch_sal_rates.get(ch)
        if ch_rate is None:
            continue

        rep_groups = group_by(ch_reps_rows, lambda r: r["_rep"])

        for rep in rep_order:
            rep_rows = rep_groups.get(rep, [])
            if not rep_rows:
                continue

            leads = count_distinct(rep_rows)
            sals = count_distinct_where(
                rep_rows, lambda r: str(r.get("is_sal", "0")) == "1"
            )
            expected = leads * ch_rate
            diff = sals - expected

            if diff <= IMPACT_CRIT:
                judgment = "🚨"
            elif diff <= IMPACT_WARN:
                judgment = "⚠️"
            else:
                judgment = "-"

            diff_s = f"{diff:+.1f}"
            if judgment != "-":
                diff_s = f"**{diff_s}**"

            rows_out.append([
                ch, rep, fmt_int(leads), fmt_int(sals),
                f"{expected:.1f}", diff_s, judgment,
            ])

    return md_table(headers, rows_out)


def compute_step2_user_weekly(q4_cur):
    cur_reps = filter_analysis_reps(q4_cur)
    alerts = []

    rep_order = IS_REPS + ["外注（合算）"]
    for rep in rep_order:
        rep_rows = [r for r in cur_reps if r["_rep"] == rep]

        weekly = defaultdict(list)
        for row in rep_rows:
            d = parse_date(row.get("created_date_jst", ""))
            if d:
                weekly[iso_week_key(d)].append(row)

        prev_cn = None
        prev_sal = None

        for wk in sorted(weekly.keys()):
            rows = weekly[wk]
            m = compute_funnel(rows)

            dates = [
                parse_date(r.get("created_date_jst", ""))
                for r in rows
                if parse_date(r.get("created_date_jst", ""))
            ]
            label = iso_week_label(min(dates)) if dates else f"W{wk[1]:02d}"

            if prev_cn is not None and m["cn_rate"] is not None:
                cn_d = (m["cn_rate"] - prev_cn) * 100
                if cn_d <= WEEKLY_DROP:
                    alerts.append({
                        "rep": rep, "week": label, "metric": "CN率",
                        "value": fmt_pct(m["cn_rate"]),
                        "diff": f"{cn_d:+.1f}pp",
                        "leads": fmt_int(m["leads"]),
                    })

            if prev_sal is not None and m["sal_rate"] is not None:
                sal_d = (m["sal_rate"] - prev_sal) * 100
                if sal_d <= WEEKLY_DROP:
                    alerts.append({
                        "rep": rep, "week": label, "metric": "SAL率",
                        "value": fmt_pct(m["sal_rate"]),
                        "diff": f"{sal_d:+.1f}pp",
                        "leads": fmt_int(m["leads"]),
                    })

            prev_cn = m["cn_rate"]
            prev_sal = m["sal_rate"]

    if not alerts:
        return "急落は検知されませんでした。"

    headers = ["担当者", "週", "指標", "値", "前週比", "リード数"]
    rows_out = [
        [a["rep"], a["week"], a["metric"], a["value"], a["diff"], a["leads"]]
        for a in alerts
    ]
    return md_table(headers, rows_out)


# ================================================================
# データ検証
# ================================================================

def validate_data(data_dir, date_str, q1, q2, q3, q4, q5, q6):
    lines = ["# データ検証レポート\n"]
    warnings = []
    errors = []

    lines.append("## ファイル一覧\n")
    files_info = [
        ("Q1 着地予想", q1), ("Q2 SAL着予", q2), ("Q3 商談実施着予", q3),
        ("Q4 デモ電話", q4), ("Q5 SAL率_積み上げ", q5), ("Q6 デモ電話_商談", q6),
    ]
    lines.append("| クエリ | 行数 | ステータス |")
    lines.append("|--------|------|----------|")
    for name, rows in files_info:
        if rows is not None:
            lines.append(f"| {name} | {len(rows):,} | OK |")
        else:
            lines.append(f"| {name} | - | ファイルなし |")
            if "Q4" in name or "Q1" in name or "Q2" in name or "Q3" in name:
                errors.append(f"{name}: ファイルが見つかりません")

    # Column checks
    if q4:
        missing = [c for c in Q4_REQUIRED if c not in q4[0]]
        if missing:
            errors.append(f"Q4 必須カラム不足: {missing}")
        else:
            lines.append("\n- Q4 必須カラム: 全て存在 ✓")

    if q5:
        missing = [c for c in Q5_REQUIRED if c not in q5[0]]
        if missing:
            errors.append(f"Q5 必須カラム不足: {missing}")
        else:
            lines.append("- Q5 必須カラム: 全て存在 ✓")

    # Previous day row count comparison
    prev_date = (
        datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    prev_q4_path = find_csv(data_dir, "q4", prev_date)
    if prev_q4_path and q4:
        prev_q4 = load_csv_file(prev_q4_path)
        if prev_q4:
            ratio = len(q4) / len(prev_q4)
            if ratio < 0.8 or ratio > 1.2:
                warnings.append(
                    f"Q4 行数変動: 前日{len(prev_q4):,}行 → "
                    f"当日{len(q4):,}行 ({ratio:.1%})"
                )

    if errors:
        lines.append("\n## エラー\n")
        for e in errors:
            lines.append(f"- ❌ {e}")
    if warnings:
        lines.append("\n## 警告\n")
        for w in warnings:
            lines.append(f"- ⚠️ {w}")
    if not errors and not warnings:
        lines.append("\n## 結果: 全チェック通過 ✅\n")

    return "\n".join(lines), len(errors) > 0


# ================================================================
# メイン
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="デモ電話チーム 月次分析テーブル確定計算"
    )
    parser.add_argument("--date", required=True, help="データ日付 (YYYY-MM-DD)")
    parser.add_argument("--data-dir", default="data", help="データディレクトリ")
    parser.add_argument("--output-dir", default="data/computed", help="出力ディレクトリ")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = args.date

    # ---- Load CSVs ----
    print(f"[1/7] CSVファイル読み込み中... (date={date_str})")

    q1 = q2 = q3 = q4 = q5 = q6 = None
    for qid in ["q1", "q2", "q3", "q4", "q5", "q6"]:
        path = find_csv(data_dir, qid, date_str)
        if path:
            data = load_csv_file(path)
            print(f"   {qid}: {path.name} ({len(data):,}行)")
            if qid == "q1": q1 = data
            elif qid == "q2": q2 = data
            elif qid == "q3": q3 = data
            elif qid == "q4": q4 = data
            elif qid == "q5": q5 = data
            elif qid == "q6": q6 = data
        else:
            print(f"   {qid}: ファイルなし")

    # ---- Validate ----
    print("[2/7] データ検証中...")
    validation_report, has_errors = validate_data(
        data_dir, date_str, q1, q2, q3, q4, q5, q6
    )
    write_file(output_dir / "_validation.md", validation_report)

    if has_errors:
        print(
            f"❌ データ検証エラー。{output_dir}/_validation.md を確認してください。"
        )
        sys.exit(1)

    # ---- Detect months ----
    current_month = detect_current_month_q4(q4) if q4 else None
    if not current_month:
        print("❌ 当月データが見つかりません")
        sys.exit(1)

    previous_month = prev_month_str(current_month)
    print(f"   当月: {current_month}, 前月: {previous_month}")

    # ---- Filter Q4 ----
    q4_cur = filter_q4(q4, current_month) if q4 else []
    q4_prev = filter_q4(q4, previous_month) if q4 else []
    print(f"   Q4 eligible: 当月={len(q4_cur):,}行, 前月={len(q4_prev):,}行")

    # ---- Detect period ----
    cur_dates = sorted(set(
        r.get("created_date_jst", "")[:10]
        for r in q4_cur if r.get("created_date_jst")
    ))
    period_start = cur_dates[0] if cur_dates else ""
    period_end = cur_dates[-1] if cur_dates else ""
    if period_start:
        print(f"   参照期間: {period_start} 〜 {period_end}")

    fm = frontmatter(date_str, current_month, previous_month,
                     period_start, period_end)

    # Previous month Q1-Q3 CSVs
    prev_q1_path = find_prev_month_csv(data_dir, "q1", date_str)
    prev_q2_path = find_prev_month_csv(data_dir, "q2", date_str)
    prev_q3_path = find_prev_month_csv(data_dir, "q3", date_str)
    prev_q1 = load_csv_file(prev_q1_path) if prev_q1_path else None
    prev_q2 = load_csv_file(prev_q2_path) if prev_q2_path else None
    prev_q3 = load_csv_file(prev_q3_path) if prev_q3_path else None

    # Fallback: Q4/Q6から前月実績を構築（前月CSVがない場合）
    fallback_q1 = fallback_q2 = fallback_q3 = None
    if not prev_q1 and q4_prev:
        fallback_q1 = build_prev_actuals_from_q4(q4_prev)
        print(f"   Q1前月フォールバック: Q4から着電数代替計算")
    if not prev_q2 and q4_prev:
        fallback_q2 = build_prev_sal_from_q4(q4_prev)
        print(f"   Q2前月フォールバック: Q4からSAL数代替計算")
    if not prev_q3 and q6:
        fallback_q3 = build_prev_meetings_from_q6(q6, previous_month)
        print(f"   Q3前月フォールバック: Q6から商談実施数代替計算")

    # ---- STEP 1 ----
    print("[3/7] STEP1 計算中...")
    table_1_1, results_1 = compute_step1_landing(q1, prev_q1, "着電",
                                                  fallback_q1)
    table_1_2, results_2 = compute_step1_landing(q2, prev_q2, "SAL",
                                                  fallback_q2)
    table_1_3, results_3 = compute_step1_landing(q3, prev_q3, "商談実施",
                                                  fallback_q3)
    table_1_4 = compute_step1_issues(results_1, results_2, results_3)

    write_file(output_dir / "step1_着電着予.md", fm + table_1_1)
    write_file(output_dir / "step1_SAL着予.md", fm + table_1_2)
    write_file(output_dir / "step1_商談実施着予.md", fm + table_1_3)
    write_file(output_dir / "step1_課題チャネル.md", fm + table_1_4)

    # ---- STEP 2 ----
    print("[4/7] STEP2 ファネル・CV計算中...")
    funnel_table, cur_ch, prev_ch = compute_step2_funnel(q4_cur, q4_prev)
    cv_table = compute_step2_cv(q4_cur, q4_prev, cur_ch)

    write_file(output_dir / "step2_ファネル転換率.md", fm + funnel_table)
    write_file(output_dir / "step2_CVコンテンツ.md", fm + cv_table)

    print("[5/7] STEP2 SALスピード・時系列計算中...")
    q5_cur = filter_q5(q5, current_month) if q5 else []
    q5_prev = filter_q5(q5, previous_month) if q5 else []
    print(f"   Q5: 当月={len(q5_cur):,}行, 前月={len(q5_prev):,}行")

    sal_speed_table = compute_step2_sal_speed(q5_cur, q5_prev)
    timeseries_table = compute_step2_timeseries(q4_cur)

    write_file(output_dir / "step2_SALスピード.md", fm + sal_speed_table)
    write_file(output_dir / "step2_時系列.md", fm + timeseries_table)

    print("[6/7] STEP2 担当者分析計算中...")
    user_summary = compute_step2_user_summary(q4_cur, q4_prev)
    user_channel = compute_step2_user_channel(q4_cur)
    user_impact = compute_step2_user_impact(q4_cur)
    user_weekly = compute_step2_user_weekly(q4_cur)

    write_file(output_dir / "step2_担当者サマリ.md", fm + user_summary)
    write_file(output_dir / "step2_担当者チャネル.md", fm + user_channel)
    write_file(output_dir / "step2_インパクト試算.md", fm + user_impact)
    write_file(output_dir / "step2_週次急落.md", fm + user_weekly)

    # ---- Summary ----
    print("[7/7] 完了!")
    print(f"   出力先: {output_dir}/")
    print(f"   ファイル数: 13")
    total_leads = count_distinct(q4_cur)
    print(f"   当月eligible リード数: {total_leads:,} ({current_month})")


if __name__ == "__main__":
    main()
