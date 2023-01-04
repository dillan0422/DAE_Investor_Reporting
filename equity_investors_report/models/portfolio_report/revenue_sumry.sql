/* DBT Configuration */
{{
    config(schema='da_stage',
        materialized='table')
}}

SELECT CAST(current_date AS date) AS run_dt
        , acct_smry.mnth_end_dt
        , acct_smry.cycle_end_dt
        , num_acct AS new_cards_issued
        , SUM(num_acct) OVER(ORDER BY acct_smry.mnth_end_dt ROWS UNBOUNDED PRECEDING ) AS cum_cards_issued
        , activated_account_count
        , activated_account_count - LAG(activated_account_count,1) OVER(ORDER BY acct_smry.mnth_end_dt ) AS new_activation
        , open_account_count
        , active_account_count
        , revenue.curr_stmt_interest_charged
        , revenue.interchange_revenue_amt
        , revenue.open_fee_amt
        , revenue.assessed_fee
        , revenue.service_pmt_commission_amt
        , revenue.collected_interest
        , revenue.interest_revenue_amt
        , revenue.fee_revenue_amt
        , total_exposure
FROM (SELECT cycle_end_dt
            , LAST_DAY(cycle_end_dt) AS mnth_end_dt
            , SUM(actived_acct_cnt) AS activated_account_count
            , SUM(open_acct_cnt) AS open_account_count
            , SUM(active_acct_cnt) AS active_account_count
            , SUM(actived_credit_limit) AS total_exposure
      FROM {{ ref ('vintage_sumry') }} 
      GROUP BY 1,2) AS acct_smry
LEFT JOIN (SELECT LAST_DAY(a.cycle_end_dt) AS mnth_end_dt,a.cycle_end_dt
                , SUM(CASE WHEN a.stmt_num=1 THEN a.open_fee_amt ELSE 0 END) AS open_fee_amt
                , SUM(a.stmt_charged_fee_amt) AS assessed_fee
                , SUM(b.stmt_paid_interest_amt) - SUM(b.stmt_tot_refund_interests_amt) AS interest_revenue_amt
                , SUM(b.stmt_paid_fee_amt) - SUM(b.stmt_tot_refund_fee_amt) AS fee_revenue_amt
                , SUM(a.stmt_tot_interchange_fee_amt) AS interchange_revenue_amt
                , SUM(COALESCE(a.service_pmt_commission_amt,0)) AS service_pmt_commission_amt
                , SUM(a.stmt_charged_interest_amt) AS curr_stmt_interest_charged
                , SUM(b.stmt_paid_interest_amt) AS collected_interest

           FROM ua.wen_stmt_acct_table_v3 AS a
           LEFT OUTER JOIN ua.wen_stmt_acct_table_v3 b
           ON a.external_acct_id = b.external_acct_id
           AND a.cycle_end_dt = b.cycle_start_dt - 1
           WHERE a.activation_dt IS NOT NULL
           AND a.acct_status_desc <> 'Inactive'
           GROUP BY 1,2) AS revenue
ON acct_smry.mnth_end_dt = revenue.mnth_end_dt
LEFT JOIN (SELECT CASE WHEN acct_since_date<='2019-12-31' THEN '2020-01-31' ELSE LAST_DAY(acct_since_date) END AS mnth_end_dt
                   , COUNT(*) AS num_acct
           FROM stori_ccm_ro.account_profile_cur
           WHERE acct_since_date >= date'2019-09-01'
           GROUP BY 1
           ORDER BY 1
          ) AS s1
ON acct_smry.mnth_end_dt = s1.mnth_end_dt
ORDER BY 1