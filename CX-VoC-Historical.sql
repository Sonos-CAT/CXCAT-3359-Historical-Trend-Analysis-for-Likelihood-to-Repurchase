CREATE OR REPLACE TABLE CX_ANALYTICS_DEV.JEREMIAH_DEV_EXPLORATORY.CXCAT_3359_CX_VOC_HISTORICAL_3YR_SCORES AS
WITH SurveyData AS (
    -- CTE 1: Grab all raw data for the last 3 fiscal years
    SELECT
        owner.FISCAL_MONTH_DESC,
        owner.FISCAL_MONTH_ID,
        owner.FISCAL_QUARTER_DESC,
        owner.FISCAL_QUARTER_ID,
        owner.FISCAL_WEEK_DESC,
        owner.FISCAL_WEEK_ID,
        owner.FISCAL_YEAR_ID,
        owner.SONOS_ID,
        owner.NPS_SCORE,
        owner.purchase_additional_likelihood AS REPURCHASE_SCORE
    FROM
        DATA_WAREHOUSE.WAREHOUSE_SURVEY.VIZ_OWNERS_SURVEY_CORP AS owner
    WHERE 
        -- Filters for the current fiscal year and the two previous years
        owner.FISCAL_YEAR_ID >= (SELECT MAX(FISCAL_YEAR_ID) - 2 FROM DATA_WAREHOUSE.WAREHOUSE_SURVEY.VIZ_OWNERS_SURVEY_CORP)
),

WeeklyMetrics AS (
    -- CTE 2: Aggregations by week
    SELECT
        FISCAL_MONTH_DESC, FISCAL_MONTH_ID, FISCAL_QUARTER_DESC,
        FISCAL_QUARTER_ID, FISCAL_WEEK_DESC, FISCAL_WEEK_ID, FISCAL_YEAR_ID,
        COUNT(DISTINCT CASE WHEN NPS_SCORE >= 9 THEN SONOS_ID END) AS promoter_count,
        COUNT(DISTINCT CASE WHEN NPS_SCORE BETWEEN 7 AND 8 THEN SONOS_ID END) AS passive_count,
        COUNT(DISTINCT CASE WHEN NPS_SCORE <= 6 THEN SONOS_ID END) AS detractor_count,
        COUNT(DISTINCT SONOS_ID) AS nps_response_volume,
        SUM(REPURCHASE_SCORE) AS repurchase_sum,
        COUNT(DISTINCT CASE WHEN REPURCHASE_SCORE IS NOT NULL THEN SONOS_ID END) AS ltr_volume
    FROM SurveyData
    GROUP BY 1,2,3,4,5,6,7
)

-- Final SELECT: Calculate metrics with 4-week rolling windows across the full history
SELECT
    FISCAL_MONTH_DESC,
    FISCAL_MONTH_ID,
    FISCAL_QUARTER_DESC,
    FISCAL_QUARTER_ID,
    FISCAL_WEEK_DESC,
    FISCAL_WEEK_ID,
    FISCAL_YEAR_ID,

    -- Weekly Metrics
    ROUND(((promoter_count - detractor_count)::FLOAT / NULLIF(nps_response_volume, 0)) * 100, 2) AS FISCAL_WEEK_NPS_SCORE,
    ROUND((repurchase_sum / NULLIF(ltr_volume, 0)), 2) AS REPURCHASE_SCORE,
    
    -- Rolling 4-Week NPS
    ROUND(((
        SUM(promoter_count) OVER (ORDER BY FISCAL_WEEK_ID ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
        SUM(detractor_count) OVER (ORDER BY FISCAL_WEEK_ID ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
    )::FLOAT /
        NULLIF(SUM(nps_response_volume) OVER (ORDER BY FISCAL_WEEK_ID ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0)
    ) * 100, 2) AS NPS_4_WEEK_ROLLING,
    
    -- Rolling 4-Week LTR
    ROUND((
        SUM(repurchase_sum) OVER (ORDER BY FISCAL_WEEK_ID ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) /
        NULLIF(SUM(ltr_volume) OVER (ORDER BY FISCAL_WEEK_ID ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 0)
    ), 2) AS LTR_4_WEEK_ROLLING,
    
    nps_response_volume AS NPS_RESPONSE_VOLUME,
    ltr_volume AS LTR_VOLUME
FROM
    WeeklyMetrics
ORDER BY
    FISCAL_WEEK_ID DESC;
