-- ================================================================
-- デモ電話チーム 月次分析用クエリ集
-- ※ クエリの変更は禁止。そのまま実行すること。
-- ================================================================


-- ================================================================
-- Q1: 着地予想（着電数の着地予測）
-- ================================================================
-- [QUERY_START:Q1]
WITH params AS (
  SELECT
    DATE_TRUNC('MONTH', CURRENT_DATE()) AS month_start,
    LAST_DAY(CURRENT_DATE())            AS month_end,
    CURRENT_DATE()                      AS today
),

holidays AS (
  SELECT DATE '2026-02-11' AS holiday -- 建国記念の日
  UNION ALL SELECT DATE '2026-02-23' -- 天皇誕生日
  UNION ALL SELECT DATE '2026-03-20' -- 春分の日
  UNION ALL SELECT DATE '2026-04-29' -- 昭和の日
  UNION ALL SELECT DATE '2026-05-03' -- 憲法記念日
  UNION ALL SELECT DATE '2026-05-04' -- みどりの日
  UNION ALL SELECT DATE '2026-05-05' -- こどもの日
  UNION ALL SELECT DATE '2026-05-06' -- 振替休日
  UNION ALL SELECT DATE '2026-07-20' -- 海の日
  UNION ALL SELECT DATE '2026-08-11' -- 山の日
  UNION ALL SELECT DATE '2026-09-21' -- 敬老の日
  UNION ALL SELECT DATE '2026-09-22' -- 秋分の日 (振替休日)
  UNION ALL SELECT DATE '2026-09-23' -- 秋分の日
  UNION ALL SELECT DATE '2026-10-12' -- スポーツの日
  UNION ALL SELECT DATE '2026-11-03' -- 文化の日
  UNION ALL SELECT DATE '2026-11-23' -- 勤労感謝の日
),

targets AS (
  SELECT '全体' AS dimension,   2008 AS monthly_target
  UNION ALL SELECT 'TOP',       335
  UNION ALL SELECT 'LIS',       366
  UNION ALL SELECT 'DIS',       817
  UNION ALL SELECT 'FAX・EDM',  400
  UNION ALL SELECT 'その他',     90
),

calendar AS (
  SELECT DATE_ADD(p.month_start, pos) AS cal_date
  FROM params p
  LATERAL VIEW POSEXPLODE(SEQUENCE(0, DATEDIFF(p.month_end, p.month_start))) t AS pos, val
),

business_days AS (
  SELECT
    c.cal_date,
    CASE
      WHEN DAYOFWEEK(c.cal_date) IN (1, 7) THEN 0
      WHEN h.holiday IS NOT NULL            THEN 0
      ELSE 1
    END AS is_business_day
  FROM calendar c
  LEFT JOIN holidays h ON c.cal_date = h.holiday
),

bd_per_date AS (
  SELECT
    cal_date,
    is_business_day,
    SUM(is_business_day) OVER (ORDER BY cal_date)                                              AS elapsed_bd,
    SUM(is_business_day) OVER () - SUM(is_business_day) OVER (ORDER BY cal_date)               AS remaining_bd
  FROM business_days
),

leads_raw AS (
  SELECT
    id,
    CAST(FROM_UTC_TIMESTAMP(created_date, 'Asia/Tokyo') AS DATE) AS lead_date,
    CASE
      WHEN inflow_route_media__c NOT IN ('TOP', 'LIS', 'FAX・EDM', 'DIS') THEN 'その他'
      ELSE inflow_route_media__c
    END AS dimension
  FROM ivry_source.salesforce.lead
  CROSS JOIN params p
  WHERE inflow_route_pd__c = 'デモ電話'
    AND created_date IS NOT NULL
    AND CAST(FROM_UTC_TIMESTAMP(created_date, 'Asia/Tokyo') AS DATE) >= p.month_start
    AND CAST(FROM_UTC_TIMESTAMP(created_date, 'Asia/Tokyo') AS DATE) <= p.today
    AND reasons_for_ineligible_leads__c is null
),

daily_all AS (
  SELECT lead_date, dimension, COUNT(distinct id) AS daily_leads
  FROM leads_raw
  GROUP BY 1, 2
  UNION ALL
  SELECT lead_date, '全体' AS dimension, COUNT(distinct id) AS daily_leads
  FROM leads_raw
  GROUP BY 1
),

result_base AS (
  SELECT
    bd.cal_date                       AS lead_date,
    d.dimension,
    COALESCE(da.daily_leads, 0)       AS daily_leads,
    SUM(COALESCE(da.daily_leads, 0))
      OVER (PARTITION BY d.dimension ORDER BY bd.cal_date) AS cumulative_actual,
    t.monthly_target,
    bd.elapsed_bd,
    bd.remaining_bd
  FROM bd_per_date bd
  CROSS JOIN (SELECT DISTINCT dimension FROM targets) d
  LEFT JOIN daily_all da
    ON bd.cal_date  = da.lead_date
   AND d.dimension  = da.dimension
  JOIN targets t
    ON d.dimension  = t.dimension
  WHERE bd.cal_date <= (SELECT today FROM params)
)

SELECT
  lead_date,
  dimension,
  daily_leads,
  cumulative_actual,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND(cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd, 2)
    ELSE NULL
  END AS landing_forecast,
  monthly_target,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND((cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd) / monthly_target, 2)
    ELSE NULL
  END AS achievement_pct
FROM result_base
ORDER BY dimension, lead_date;
-- [QUERY_END:Q1]


-- ================================================================
-- Q2: SAL着予（SAL数の着地予測）
-- ================================================================
-- [QUERY_START:Q2]
WITH params AS (
  SELECT
    DATE_TRUNC('MONTH', CURRENT_DATE()) AS month_start,
    LAST_DAY(CURRENT_DATE())            AS month_end,
    CURRENT_DATE()                      AS today
),

holidays AS (
  SELECT DATE '2026-02-11' AS holiday -- 建国記念の日
  UNION ALL SELECT DATE '2026-02-23' -- 天皇誕生日
  UNION ALL SELECT DATE '2026-03-20' -- 春分の日
  UNION ALL SELECT DATE '2026-04-29' -- 昭和の日
  UNION ALL SELECT DATE '2026-05-03' -- 憲法記念日
  UNION ALL SELECT DATE '2026-05-04' -- みどりの日
  UNION ALL SELECT DATE '2026-05-05' -- こどもの日
  UNION ALL SELECT DATE '2026-05-06' -- 振替休日
  UNION ALL SELECT DATE '2026-07-20' -- 海の日
  UNION ALL SELECT DATE '2026-08-11' -- 山の日
  UNION ALL SELECT DATE '2026-09-21' -- 敬老の日
  UNION ALL SELECT DATE '2026-09-22' -- 秋分の日 (振替休日)
  UNION ALL SELECT DATE '2026-09-23' -- 秋分の日
  UNION ALL SELECT DATE '2026-10-12' -- スポーツの日
  UNION ALL SELECT DATE '2026-11-03' -- 文化の日
  UNION ALL SELECT DATE '2026-11-23' -- 勤労感謝の日
),

targets AS (
  SELECT '全体' AS dimension,   249 AS monthly_target
  UNION ALL SELECT 'TOP',       78
  UNION ALL SELECT 'LIS',       51
  UNION ALL SELECT 'DIS',       82
  UNION ALL SELECT 'FAX・EDM',  28
  UNION ALL SELECT 'その他',     9
),

calendar AS (
  SELECT DATE_ADD(p.month_start, pos) AS cal_date
  FROM params p
  LATERAL VIEW POSEXPLODE(SEQUENCE(0, DATEDIFF(p.month_end, p.month_start))) t AS pos, val
),

business_days AS (
  SELECT
    c.cal_date,
    CASE
      WHEN DAYOFWEEK(c.cal_date) IN (1, 7) THEN 0
      WHEN h.holiday IS NOT NULL            THEN 0
      ELSE 1
    END AS is_business_day
  FROM calendar c
  LEFT JOIN holidays h ON c.cal_date = h.holiday
),

bd_running AS (
  SELECT
    bd1.cal_date,
    SUM(CASE WHEN bd2.cal_date <= bd1.cal_date THEN bd2.is_business_day ELSE 0 END) AS elapsed_bd,
    SUM(CASE WHEN bd2.cal_date >  bd1.cal_date THEN bd2.is_business_day ELSE 0 END) AS remaining_bd
  FROM business_days bd1
  CROSS JOIN business_days bd2
  GROUP BY bd1.cal_date
),

sal_raw AS (
  SELECT
    CAST(FROM_UTC_TIMESTAMP(f_initial_deal_acquisition_date, 'Asia/Tokyo') AS DATE) AS sal_date,
    CASE
      WHEN inflow_route_media_lasttouch NOT IN ('TOP', 'LIS', 'FAX・EDM', 'DIS') THEN 'その他'
      ELSE inflow_route_media_lasttouch
    END AS dimension
  FROM ivry_staff.marketing_operations.lead_monitoring
  CROSS JOIN params p
  WHERE tier_classification_campaign_member = 'デモ電話'
    AND contact_reasons_for_ineligible_leads IS NULL
    AND duplication_record = false
    AND no = 1
    AND f_initial_deal_acquisition_date IS NOT NULL
    AND CAST(FROM_UTC_TIMESTAMP(f_initial_deal_acquisition_date, 'Asia/Tokyo') AS DATE) >= p.month_start
    AND CAST(FROM_UTC_TIMESTAMP(f_initial_deal_acquisition_date, 'Asia/Tokyo') AS DATE) <= p.today
),

daily_all AS (
  SELECT sal_date AS lead_date, dimension, COUNT() AS daily_leads
  FROM sal_raw
  GROUP BY 1, 2
  UNION ALL
  SELECT sal_date AS lead_date, '全体' AS dimension, COUNT() AS daily_leads
  FROM sal_raw
  GROUP BY 1
),

result_base AS (
  SELECT
    bd.cal_date                       AS lead_date,
    d.dimension,
    COALESCE(da.daily_leads, 0)       AS daily_leads,
    SUM(COALESCE(da.daily_leads, 0))
      OVER (PARTITION BY d.dimension ORDER BY bd.cal_date) AS cumulative_actual,
    t.monthly_target,
    br.elapsed_bd,
    br.remaining_bd
  FROM business_days bd
  CROSS JOIN (SELECT DISTINCT dimension FROM targets) d
  LEFT JOIN daily_all da
    ON bd.cal_date  = da.lead_date
   AND d.dimension  = da.dimension
  JOIN bd_running br ON bd.cal_date = br.cal_date
  JOIN targets t     ON d.dimension = t.dimension
  WHERE bd.cal_date <= (SELECT today FROM params)
)

SELECT
  lead_date,
  dimension,
  daily_leads,
  cumulative_actual,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND(cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd, 2)
    ELSE NULL
  END AS landing_forecast,
  monthly_target,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND((cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd) / monthly_target, 2)
    ELSE NULL
  END AS achievement_pct
FROM result_base
ORDER BY dimension, lead_date;
-- [QUERY_END:Q2]


-- ================================================================
-- Q3: 商談実施着予（商談実施数の着地予測）
-- ================================================================
-- [QUERY_START:Q3]
WITH params AS (
  SELECT
    DATE_TRUNC('MONTH', CURRENT_DATE()) AS month_start,
    LAST_DAY(CURRENT_DATE())            AS month_end,
    CURRENT_DATE()                      AS today
),

holidays AS (
  SELECT DATE '2026-02-11' AS holiday -- 建国記念の日
  UNION ALL SELECT DATE '2026-02-23' -- 天皇誕生日
  UNION ALL SELECT DATE '2026-03-20' -- 春分の日
  UNION ALL SELECT DATE '2026-04-29' -- 昭和の日
  UNION ALL SELECT DATE '2026-05-03' -- 憲法記念日
  UNION ALL SELECT DATE '2026-05-04' -- みどりの日
  UNION ALL SELECT DATE '2026-05-05' -- こどもの日
  UNION ALL SELECT DATE '2026-05-06' -- 振替休日
  UNION ALL SELECT DATE '2026-07-20' -- 海の日
  UNION ALL SELECT DATE '2026-08-11' -- 山の日
  UNION ALL SELECT DATE '2026-09-21' -- 敬老の日
  UNION ALL SELECT DATE '2026-09-22' -- 秋分の日 (振替休日)
  UNION ALL SELECT DATE '2026-09-23' -- 秋分の日
  UNION ALL SELECT DATE '2026-10-12' -- スポーツの日
  UNION ALL SELECT DATE '2026-11-03' -- 文化の日
  UNION ALL SELECT DATE '2026-11-23' -- 勤労感謝の日
),

targets AS (
  SELECT '全体' AS dimension,   221 AS monthly_target
  UNION ALL SELECT 'TOP',       74
  UNION ALL SELECT 'LIS',       44
  UNION ALL SELECT 'DIS',       70
  UNION ALL SELECT 'FAX・EDM',  24
  UNION ALL SELECT 'その他',     8
),

calendar AS (
  SELECT DATE_ADD(p.month_start, pos) AS cal_date
  FROM params p
  LATERAL VIEW POSEXPLODE(SEQUENCE(0, DATEDIFF(p.month_end, p.month_start))) t AS pos, val
),

business_days AS (
  SELECT
    c.cal_date,
    CASE
      WHEN DAYOFWEEK(c.cal_date) IN (1, 7) THEN 0
      WHEN h.holiday IS NOT NULL            THEN 0
      ELSE 1
    END AS is_business_day
  FROM calendar c
  LEFT JOIN holidays h ON c.cal_date = h.holiday
),

bd_running AS (
  SELECT
    bd1.cal_date,
    SUM(CASE WHEN bd2.cal_date <= bd1.cal_date THEN bd2.is_business_day ELSE 0 END) AS elapsed_bd,
    SUM(CASE WHEN bd2.cal_date >  bd1.cal_date THEN bd2.is_business_day ELSE 0 END) AS remaining_bd
  FROM business_days bd1
  CROSS JOIN business_days bd2
  GROUP BY bd1.cal_date
),

meeting_raw AS (
  SELECT
    CAST(FROM_UTC_TIMESTAMP(first_meeting_date, 'Asia/Tokyo') AS DATE) AS meeting_date,
    CASE
      WHEN inflow_route_media_lasttouch NOT IN ('TOP', 'LIS', 'FAX・EDM', 'DIS') THEN 'その他'
      ELSE inflow_route_media_lasttouch
    END AS dimension
  FROM ivry_staff.marketing_operations.lead_monitoring
  CROSS JOIN params p
  WHERE tier_classification_campaign_member = 'デモ電話'
    AND contact_reasons_for_ineligible_leads IS NULL
    AND duplication_record = false
    AND no = 1
    AND first_meeting_date IS NOT NULL
    AND CAST(FROM_UTC_TIMESTAMP(first_meeting_date, 'Asia/Tokyo') AS DATE) >= p.month_start
    AND CAST(FROM_UTC_TIMESTAMP(first_meeting_date, 'Asia/Tokyo') AS DATE) <= p.today
),

daily_all AS (
  SELECT meeting_date AS lead_date, dimension, COUNT() AS daily_leads
  FROM meeting_raw
  GROUP BY 1, 2
  UNION ALL
  SELECT meeting_date AS lead_date, '全体' AS dimension, COUNT() AS daily_leads
  FROM meeting_raw
  GROUP BY 1
),

result_base AS (
  SELECT
    bd.cal_date                       AS lead_date,
    d.dimension,
    COALESCE(da.daily_leads, 0)       AS daily_leads,
    SUM(COALESCE(da.daily_leads, 0))
      OVER (PARTITION BY d.dimension ORDER BY bd.cal_date) AS cumulative_actual,
    t.monthly_target,
    br.elapsed_bd,
    br.remaining_bd
  FROM business_days bd
  CROSS JOIN (SELECT DISTINCT dimension FROM targets) d
  LEFT JOIN daily_all da
    ON bd.cal_date  = da.lead_date
   AND d.dimension  = da.dimension
  JOIN bd_running br ON bd.cal_date = br.cal_date
  JOIN targets t     ON d.dimension = t.dimension
  WHERE bd.cal_date <= (SELECT today FROM params)
)

SELECT
  lead_date,
  dimension,
  daily_leads,
  cumulative_actual,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND(cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd, 2)
    ELSE NULL
  END AS landing_forecast,
  monthly_target,
  CASE
    WHEN elapsed_bd > 0
    THEN ROUND((cumulative_actual + (cumulative_actual * 1.0 / elapsed_bd) * remaining_bd) / monthly_target, 2)
    ELSE NULL
  END AS achievement_pct
FROM result_base
ORDER BY dimension, lead_date;
-- [QUERY_END:Q3]


-- ================================================================
-- Q4: デモ電話（リード単位の実績明細）
-- ================================================================
-- [QUERY_START:Q4]
WITH Lead AS (
  SELECT
    *,
    from_utc_timestamp(created_date, 'Asia/Tokyo') AS created_date_jst,
    from_utc_timestamp(initial_deal_acquisition_date__c, 'Asia/Tokyo') AS initial_deal_acquisition_date_jst,
    from_utc_timestamp(business_meeting_scheduled_date__c, 'Asia/Tokyo') AS business_meeting_scheduled_date_jst,
    from_utc_timestamp(initial_deal_date__c, 'Asia/Tokyo') AS initial_deal_date_jst,
    is_aproach_count__c,
    CASE
      WHEN (phone LIKE '090%' OR phone LIKE '080%' OR phone LIKE '070%')
        OR (mobile_phone LIKE '090%' OR mobile_phone LIKE '080%' OR mobile_phone LIKE '070%')
      THEN '携帯'
      ELSE '固定電話'
    END AS phone_type_flag,
    CASE
      WHEN lead.is_approach_history_memo__c LIKE '%タスク%' then '完了'
      else '未完了'
    end as is_task_complete
  FROM ivry_source.salesforce.lead lead
  LEFT JOIN ivry_staff.marketing_operations.daily_mql_percentage dmp
    ON DATE(from_utc_timestamp(created_date, 'Asia/Tokyo')) = dmp.date
)

SELECT
  lead.*,
  CASE
    WHEN (
      (
        lead.stage_is__c LIKE ('アポ日時調整中')
        OR lead.stage_is__c LIKE ('%NG%')
        OR lead.stage_is__c LIKE ('おっかけ連絡中')
        OR lead.stage_is__c LIKE ('商談設定完了')
      )
      OR status LIKE ('取引開始済み%')
    )
    THEN 1
    ELSE 0
  END AS is_connect,
  CASE
    WHEN DATE(lead.created_date_jst) IN ('2025-09-15', '2025-09-23', '2025-10-13', '2025-11-03','2026-01-01','2026-01-02','2026-01-12')
    THEN '営業時間外'
    WHEN DAYOFWEEK(lead.created_date_jst) BETWEEN 2 AND 6
      AND HOUR(lead.created_date_jst) >= 10
      AND HOUR(lead.created_date_jst) < 19
    THEN '営業時間内(10_19)'
    ELSE '営業時間外'
  END AS business_hours_class,
  CASE
    WHEN mon.f_initial_deal_acquisition_date is not null
    THEN 1
    ELSE 0
  END AS is_sal,
  CASE
    WHEN DATE(lead.created_date_jst) IN ('2025-09-15', '2025-09-23', '2025-10-13', '2025-11-03')
    THEN '休日'
    WHEN DAYOFWEEK(lead.created_date_jst) BETWEEN 2 AND 6
    THEN '平日'
    ELSE '休日'
  END AS is_holiday,
  user.user_name,
  CASE
    WHEN lead.inflow_route_media__c NOT IN ('TOP', 'LIS', 'FAX・EDM', 'DIS') THEN 'その他'
    ELSE lead.inflow_route_media__c
  END AS inflow_route_media,
  is_aproach_count__c
FROM Lead AS lead
LEFT JOIN ivry_analysis.source_salesforce.user
  ON lead.is_owner__c = user.user_id
LEFT JOIN ivry_staff.marketing_operations.lead_monitoring AS mon
  ON lead.converted_contact_id = mon.contact_id
WHERE lead.inflow_route_pd__c = 'デモ電話'
ORDER BY created_date DESC;
-- [QUERY_END:Q4]


-- ================================================================
-- Q5: SAL率_積み上げ（リード獲得〜SALまでの日数分布）
-- ================================================================
-- [QUERY_START:Q5]
WITH Lead AS (
  SELECT
    *,
    from_utc_timestamp(created_date, 'Asia/Tokyo') AS created_date_jst,
    from_utc_timestamp(initial_deal_acquisition_date__c, 'Asia/Tokyo') AS initial_deal_acquisition_date_jst,
    from_utc_timestamp(business_meeting_scheduled_date__c, 'Asia/Tokyo') AS business_meeting_scheduled_date_jst,
    from_utc_timestamp(initial_deal_date__c, 'Asia/Tokyo') AS initial_deal_date_jst,
    CASE
      WHEN (phone LIKE '090%' OR phone LIKE '080%' OR phone LIKE '070%')
        OR (mobile_phone LIKE '090%' OR mobile_phone LIKE '080%' OR mobile_phone LIKE '070%')
      THEN '携帯'
      ELSE '固定電話'
    END AS phone_type_flag,
    CASE
      WHEN inflow_route_media__c LIKE '%TOP%' OR inflow_route_media__c LIKE '%フリーミアム%' THEN 'TOP LP'
      WHEN cv_content_sub__c LIKE '%デモ電話_LIS_TOP%' OR cv_content_sub__c LIKE '%広告TOP%' THEN 'LIS_TOP'
      WHEN cv_content_sub__c LIKE '%IP電話%' OR inflow_route_media__c LIKE '%IP電話%' THEN 'LIS_IP電話'
      WHEN inflow_route_media__c LIKE '%ビジネスフォン%' THEN 'LIS_ビジネスフォン'
      WHEN cv_content_sub__c LIKE '%電話代行%' OR cv_content_sub__c LIKE '%ai-demo%'
        OR inflow_route_media__c LIKE '%AI%' OR inflow_route_media__c LIKE '%代表電話%'
        OR inflow_route_media__c LIKE '%telephone-answering%' THEN 'LIS_AILP'
      WHEN cv_content_sub__c LIKE '%サイトリンク%' OR inflow_route_media__c LIKE '%サイトリンク%' THEN 'LIS_サイトリンク'
      WHEN cv_content_sub__c LIKE '%記事%' THEN '記事ページ'
      WHEN cv_content_sub__c = 'デモ電話_Facebook' OR inflow_route_media__c LIKE '%3892%'
        OR inflow_route_media__c LIKE '%4693%' THEN 'Facebook_通常CPN'
      WHEN inflow_route_media__c = 'Dis/Lis LP' AND cv_content_sub__c IS NULL THEN 'Facebook_通常CPN'
      WHEN inflow_route_media__c = 'Dis/Lis LP' AND cv_content_sub__c NOT LIKE '%LIS%'
        AND cv_content_sub__c != 'デモ電話_Facebook' THEN 'Facebook_デモ電話CPN'
      WHEN inflow_route_media__c LIKE '%LINE風%' OR inflow_route_media__c LIKE '%出られない%' THEN 'Facebook_デモ電話CPN'
      WHEN inflow_route_media__c LIKE '%FAX%' OR inflow_route_media__c LIKE '%EDM%'
        OR cv_content_sub__c LIKE '%FAX%' OR cv_content_sub__c LIKE '%EDM%' THEN 'FAX・EDM'
      WHEN inflow_route_media__c LIKE '%telsearch%' THEN 'その他LP'
      ELSE inflow_route_media__c
    END AS demo_call_type,
    CASE
      WHEN inflow_route_media__c LIKE '%TOP%' OR inflow_route_media__c LIKE '%フリーミアム%' THEN 'TOP'
      WHEN cv_content_sub__c LIKE '%LIS%' OR cv_content_sub__c LIKE '%Lis-%' OR cv_content_sub__c LIKE '%Lis・%'
        OR cv_content_sub__c LIKE '%IP電話%' OR cv_content_sub__c LIKE '%電話代行%'
        OR cv_content_sub__c LIKE '%ai-demo%' OR cv_content_sub__c LIKE '%サイトリンク%'
        OR cv_content_sub__c LIKE '%Microsoft%'
        OR inflow_route_media__c LIKE '%IP電話%' OR inflow_route_media__c LIKE '%ビジネスフォン%'
        OR inflow_route_media__c LIKE '%AI%' OR inflow_route_media__c LIKE '%代表電話%'
        OR inflow_route_media__c LIKE '%telephone-answering%' OR inflow_route_media__c LIKE '%サイトリンク%'
        OR inflow_route_media__c = 'Lis LP' OR inflow_route_media__c = 'Lis' THEN 'LIS'
      WHEN cv_content_sub__c = 'デモ電話_Facebook' THEN 'DIS'
      WHEN inflow_route_media__c LIKE '%3892%' OR inflow_route_media__c LIKE '%4693%' THEN 'DIS'
      WHEN cv_content_sub__c LIKE '%着電量確認%' THEN 'DIS'
      WHEN cv_content_sub__c LIKE '%出るべき電話%' THEN 'DIS'
      WHEN inflow_route_media__c = 'Dis/Lis LP' AND cv_content_sub__c IS NULL THEN 'DIS'
      WHEN inflow_route_media__c IN ('Dis LP', 'Dis/Lis LP') THEN 'DIS_デモ電話CPN'
      WHEN inflow_route_media__c LIKE '%LINE風%' OR inflow_route_media__c LIKE '%出られない%' THEN 'DIS_デモ電話CPN'
      WHEN inflow_route_media__c LIKE '%FAX%' OR inflow_route_media__c LIKE '%EDM%'
        OR cv_content_sub__c LIKE '%FAX%' OR cv_content_sub__c LIKE '%EDM%' THEN 'FAX・EDM'
      ELSE 'その他'
    END AS demo_call_type_summary_v2
  FROM ivry_source.salesforce.lead lead
  LEFT JOIN ivry_staff.marketing_operations.daily_mql_percentage dmp
    ON DATE(from_utc_timestamp(created_date, 'Asia/Tokyo')) = dmp.date
),

raw_data AS (
  SELECT
    lead.*,
    CASE
      WHEN lead.demo_call_type IN ('TOP LP', 'LIS_TOP', 'LIS_IP電話', 'LIS_ビジネスフォン', 'LIS_AILP', 'LIS_サイトリンク') THEN 'TOP/AI系'
      WHEN lead.demo_call_type = 'Facebook_通常CPN' THEN '広告系'
      WHEN lead.demo_call_type = 'Facebook_デモ電話CPN' THEN '広告系'
      WHEN lead.demo_call_type = 'FAX・EDM' THEN 'FAX・EDM'
      ELSE 'その他'
    END AS demo_call_type_summary_FY25Q4,
    CASE
      WHEN lead.demo_call_type = 'TOP LP' THEN 'TOP'
      WHEN lead.demo_call_type IN ('LIS_TOP', 'LIS_IP電話', 'LIS_ビジネスフォン', 'LIS_AILP', 'LIS_サイトリンク') THEN 'LIS'
      WHEN lead.demo_call_type = 'Facebook_通常CPN' THEN 'DIS'
      WHEN lead.demo_call_type = 'Facebook_デモ電話CPN' THEN 'DIS_デモ電話CPN'
      WHEN lead.demo_call_type = 'FAX・EDM' THEN 'FAX・EDM'
      ELSE 'その他'
    END AS demo_call_type_summary,
    CASE
      WHEN (
        (lead.stage_is__c LIKE ('アポ日時調整中')
          OR lead.stage_is__c LIKE ('%NG%')
          OR lead.stage_is__c LIKE ('おっかけ連絡中')
          OR lead.stage_is__c LIKE ('商談設定完了'))
        OR status LIKE ('取引開始済み%'))
      THEN 1
      ELSE 0
    END AS is_connect,
    CASE
      WHEN DATE(lead.created_date_jst) IN ('2025-09-15', '2025-09-23', '2025-10-13', '2025-11-03')
      THEN '営業時間外'
      WHEN DAYOFWEEK(lead.created_date_jst) BETWEEN 2 AND 6
        AND HOUR(lead.created_date_jst) >= 10
        AND HOUR(lead.created_date_jst) < 19
      THEN '営業時間内(10_19)'
      ELSE '営業時間外'
    END AS business_hours_class,
    CASE
      WHEN mon.f_initial_deal_acquisition_date is not null
      THEN 1
      ELSE 0
    END AS is_sal,
    CASE
      WHEN DATE(lead.created_date_jst) IN ('2025-09-15', '2025-09-23', '2025-10-13', '2025-11-03')
      THEN '休日'
      WHEN DAYOFWEEK(lead.created_date_jst) BETWEEN 2 AND 6
      THEN '平日'
      ELSE '休日'
    END AS is_holiday,
    user.user_name,
    DATEDIFF(mon.f_initial_deal_acquisition_date, lead.created_date) AS d_sal,
    f_initial_deal_acquisition_date,
    user.user_name
  FROM Lead AS lead
  LEFT JOIN ivry_analysis.source_salesforce.user
    ON lead.is_owner__c = user.user_id
  LEFT JOIN ivry_staff.marketing_operations.lead_monitoring AS mon
    ON lead.converted_contact_id = mon.contact_id
  WHERE lead.inflow_route_pd__c = 'デモ電話'
  ORDER BY created_date DESC
)

SELECT
  created_date_jst,
  f_initial_deal_acquisition_date,
  business_hours_class,
  is_holiday,
  user_name,
  demo_call_type_summary_v2,
  cv_content_sub__c,
  COUNT(*) AS total_leads,
  SUM(is_sal) AS total_sal,
  COUNT(CASE WHEN d_sal <= 1 THEN 1 END) AS sal_within_1d,
  COUNT(CASE WHEN d_sal > 1 AND d_sal <= 3 THEN 1 END) AS sal_within_3d,
  COUNT(CASE WHEN d_sal > 3 AND d_sal <= 7 THEN 1 END) AS sal_7d_diff,
  COUNT(CASE WHEN d_sal > 7 AND d_sal <= 14 THEN 1 END) AS sal_14d_diff,
  COUNT(CASE WHEN d_sal > 14 AND d_sal <= 21 THEN 1 END) AS sal_21d_diff,
  COUNT(CASE WHEN d_sal > 21 AND d_sal <= 30 THEN 1 END) AS sal_30d_diff,
  COUNT(CASE WHEN d_sal > 30 THEN 1 END) AS sal_after_30d
FROM raw_data
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2 DESC;
-- [QUERY_END:Q5]


-- ================================================================
-- Q6: デモ電話_商談（商談明細）
-- ================================================================
-- [QUERY_START:Q6]
WITH data AS (
  SELECT
    *,
    from_utc_timestamp(f_initial_deal_acquisition_date, 'Asia/Tokyo') AS f_initial_deal_acquisition_date_jst,
    from_utc_timestamp(created_date, 'Asia/Tokyo') AS created_date_jst,
    from_utc_timestamp(business_meeting_scheduled_date, 'Asia/Tokyo') AS business_meeting_scheduled_date_jst,
    from_utc_timestamp(first_meeting_date, 'Asia/Tokyo') AS first_meeting_date_jst
  FROM ivry_staff.marketing_operations.lead_monitoring
  WHERE from_utc_timestamp(f_initial_deal_acquisition_date, 'Asia/Tokyo') >= DATE '2025-09-01'
    AND tier_classification_campaign_member = 'デモ電話'
    AND contact_reasons_for_ineligible_leads IS NULL
    AND duplication_record = false
    AND no = 1
)

SELECT
  *,
  CASE
    WHEN business_meeting_scheduled_date_jst < CURRENT_DATE THEN business_meeting_scheduled_date_jst
    ELSE NULL
  END AS first_meeting_date_comming,
  CASE
    WHEN business_meeting_scheduled_date_jst <= CURRENT_DATE THEN 1
    ELSE 0
  END AS is_first_meeting_date_comming,
  DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) AS scheduled_initialdead_interval_days,
  CASE
    WHEN DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) = 0 THEN '1日以内'
    WHEN DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) BETWEEN 1 AND 2 THEN '3日以内'
    WHEN DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) BETWEEN 3 AND 6 THEN '1週間'
    WHEN DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) BETWEEN 7 AND 13 THEN '2週間以内'
    WHEN DATEDIFF(business_meeting_scheduled_date_jst, f_initial_deal_acquisition_date_jst) >= 14 THEN '2週間以上'
    ELSE 'その他'
  END AS scheduled_initialdead_interval_days_category
FROM data;
-- [QUERY_END:Q6]
