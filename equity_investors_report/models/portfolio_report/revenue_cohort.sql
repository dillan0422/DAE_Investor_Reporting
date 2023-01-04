/* Revenue information by cohort */
{{
    config(schema = 'da_stage'
           , materialized = 'table')
}}

SELECT CAST(current_date AS date) AS run_dt
       , revenue.mnth_end_dt
       , revenue.cycle_end_dt
       , revenue.vintage
       , revenue.stmt_num -- Is it neccesary? Not used in financial model
       , acct_smry.active_account_count
       , revenue.curr_stmt_interest_charged -- assessed_interest
       , revenue.collected_interest
       , revenue.interest_revenue_amt -- adjusted_interest
       , revenue.assessed_fee
       , revenue.fee_revenue_amt
       , revenue.open_fee_amt
       , revenue.interchange_revenue_amt
       , revenue.service_pmt_revenue_amt
FROM (SELECT LAST_DAY(a.cycle_end_dt) AS mnth_end_dt
             , a.cycle_end_dt
             , a.vintage
             , MAX(a.stmt_num) AS stmt_num -- Is it neccesary? Not used in financial model
             , SUM(a.stmt_charged_interest_amt) AS curr_stmt_interest_charged -- assessed_interest
             , SUM(b.stmt_paid_interest_amt) AS collected_interest
             , SUM(b.stmt_paid_interest_amt) - sum(b.stmt_tot_refund_interests_amt) AS interest_revenue_amt -- adjusted_interest
             , SUM(a.stmt_charged_fee_amt) AS assessed_fee
             , SUM(b.stmt_paid_fee_amt) - sum(b.stmt_tot_refund_fee_amt) AS fee_revenue_amt
             , SUM(CASE WHEN a.stmt_num=1 THEN a.open_fee_amt ELSE 0 END) AS open_fee_amt
             , SUM(a.stmt_tot_interchange_fee_amt + coalesce(a.service_pmt_commission_amt, 0)) AS interchange_revenue_amt
             , SUM(COALESCE(a.service_pmt_commission_amt, 0)) AS service_pmt_revenue_amt
    
      FROM ua.wen_stmt_acct_table_v3 AS a
      LEFT OUTER JOIN ua.wen_stmt_acct_table_v3 b
      ON a.external_acct_id = b.external_acct_id
      AND a.cycle_end_dt = b.cycle_start_dt - 1
      WHERE a.activation_dt IS NOT NULL
      AND a.acct_status_desc <> 'Inactive'
      GROUP BY 1,2,3
     ) AS revenue
LEFT JOIN (SELECT cycle_end_dt
                  , LAST_DAY(cycle_end_dt) AS mnth_end_dt
                  , vintage
                  , SUM(active_acct_cnt) AS active_account_count
           FROM {{ ref ('vintage_sumry') }} 
           GROUP BY 1,2,3
          ) acct_smry
ON revenue.mnth_end_dt = acct_smry.mnth_end_dt
AND revenue.vintage = acct_smry.vintage
ORDER BY 1,2,3,4