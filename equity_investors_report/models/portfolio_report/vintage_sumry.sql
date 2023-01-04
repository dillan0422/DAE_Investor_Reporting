/* Summary by vintage */
{{
    config(schema = 'da_stage'
           , materialized = 'table')
}}

SELECT vtg.*
       , alb.all_accounts
       , alb.all_prncp_balance
        
FROM (SELECT
        CAST(current_date AS date) AS run_dt
        , EXTRACT(year FROM cycle_end_dt)*100+extract(month FROM cycle_end_dt) AS stmt_mth                
        , cycle_end_dt
        , vintage
        , MAX(stmt_num) AS stmt_num -- Is it necessary?

        , SUM(1) AS actived_acct_cnt
        , SUM(CASE WHEN acct_close_status='Attrition' THEN 1 ELSE 0 END) AS attrition_cnt
        , SUM(CASE WHEN acct_close_status='Charge_off' THEN 1 ELSE 0 END) AS co_cnt
        , SUM(CASE WHEN acct_close_status='Other_closed' THEN 1 ELSE 0 END) AS other_close_cnt
        , SUM(CASE WHEN acct_close_status='Open' THEN 1 ELSE 0 END) AS open_acct_cnt

        , SUM(CASE WHEN acct_active_flag = 'Y' THEN 1 ELSE 0 END) AS active_acct_cnt
        , ROUND(COALESCE(CAST(active_acct_cnt AS float)/ NULLIF(CAST(open_acct_cnt AS float), 0), 0), 4) AS active_rate

        /* Credit line */
        , SUM(curr_credit_limit) AS actived_credit_limit
        , SUM(CASE WHEN acct_close_status = 'Open' THEN curr_credit_limit ELSE 0 END) AS open_credit_limit
        , ROUND(AVG(CASE WHEN acct_close_status = 'Open' THEN apr_amt ELSE 0 END),2) AS avg_open_acct_apr
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN curr_credit_limit ELSE 0 END) AS active_credit_limit
        , ROUND(1.000000*active_credit_limit/active_acct_cnt, 2) AS avg_active_credit_line

        /* Card usage */
        , SUM(CASE WHEN acct_active_flag = 'Y' AND COALESCE(indra_txn_amt,0)>0 THEN 1 ELSE 0 END) AS transactor_acct_cnt
        , ROUND(1.000000 * COALESCE( transactor_acct_cnt/ NULLIF(open_acct_cnt, 0), 0 ), 4 ) AS transactor_acct_pct
        , SUM(CASE WHEN acct_active_flag = 'Y' AND indra_adb_amt>0 THEN 1 ELSE 0 END) AS revolver_acct_cnt
        , ROUND(1.000000 * COALESCE( revolver_acct_cnt / NULLIF(open_acct_cnt, 0), 0), 4) AS revolver_acct_pct
        , SUM(CASE WHEN acct_active_flag = 'Y' AND interest_bearing_adb>0 AND indra_txn_amt>0 THEN 1 ELSE 0 END) AS transactor_revolver_acct_cnt
        , ROUND(1.000000*transactor_revolver_acct_cnt/open_acct_cnt, 4) AS transactor_revolver_acct_pct

        /* Card Usage - Volume */
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN curr_ledger_bal_amt ELSE 0 END) AS tot_end_os_bal_amt
        , ROUND(1.000000*tot_end_os_bal_amt/active_acct_cnt, 2) AS acct_end_os_bal_amt
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'N' THEN curr_ledger_bal_amt ELSE 0 END) AS nondq90_end_os_bal_amt
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'Y' THEN curr_ledger_bal_amt ELSE 0 END) AS dq90_end_os_bal_amt


        --Purchase Information----
        , SUM(total_txn_amt) AS tot_txn_amt
        , ROUND(1.000000*tot_txn_amt/active_acct_cnt, 2) AS acct_txn_amt
        , ROUND(1.000000*tot_txn_amt/active_credit_limit, 4) AS txn_cl_util_pct

        , SUM(total_txn_cnt) AS tot_txn_cnt
        , ROUND(1.000000*tot_txn_cnt/active_acct_cnt, 2) AS acct_txn_cnt
        , ROUND(1.000000*tot_txn_amt/tot_txn_cnt, 2) AS txn_size_amt

        , SUM(CASE WHEN acct_active_flag = 'Y' THEN indra_adb_amt ELSE 0 END) AS tot_adb_amt
        , ROUND(1.000000*tot_adb_amt/active_acct_cnt, 2) AS acct_adb_amt
        , ROUND(1.000000*tot_adb_amt/active_credit_limit, 4) AS adb_cl_util_pct
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN interest_bearing_adb ELSE 0 END) AS interest_bearing_adb_amt
                  --WHEN acct_active_flag = 'Y' AND cycle_end_dt BETWEEN '2021-11-01' AND '2022-01-31'THEN interest_bearing_adb_wen 
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN all_adb ELSE 0 END) AS all_adb
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'N' THEN interest_bearing_adb ELSE 0 END) AS nondq90_interest_bearing_adb_amt
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'N' THEN all_adb ELSE 0 END) AS nondq_90all_adb
        , (CASE WHEN nondq_90all_adb <> 0 THEN ROUND(1.000000 * nondq90_interest_bearing_adb_amt / nondq_90all_adb, 2) ELSE 0 END ) AS revolver_bal_pct

        /* Principal account balance */
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN curr_tot_prncp_amt ELSE 0 END) AS tot_prncp_amt
        , SUM(open_fee_acct_flag) AS incremental_open_fee_acct_cnt
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'N' THEN curr_tot_prncp_amt ELSE 0 END) AS nondq90_prncp_amt
        , SUM(CASE WHEN acct_active_flag = 'Y' AND dq_90_flag = 'Y' THEN curr_tot_prncp_amt ELSE 0 END) AS dq90_prncp_amt

        , SUM(co_balance) AS co_bal


        --Principal Payment----
        , SUM(CASE WHEN acct_active_flag = 'Y' THEN stmt_cycle_prncp_payment ELSE 0 END) AS stmt_cycle_prncp_pmt
        , SUM(all_payment_amt) AS total_pmt

        , ROUND(COALESCE(tot_txn_amt / NULLIF(active_credit_limit, 0), 0), 2) AS purchase_utilization
        , ROUND(COALESCE(active_credit_limit / NULLIF(active_acct_cnt, 0), 0), 2) AS vntg_act_cline
        , ROUND(COALESCE(nondq90_interest_bearing_adb_amt / NULLIF(nondq_90all_adb, 0), 0), 2) AS interest_bearing

        ,SUM(CASE WHEN acct_active_flag = 'Y' THEN all_adb ELSE 0 END) AS all_adb_amt 
        ,CASE WHEN all_adb_amt = 0 THEN 0 ELSE 1.0000*tot_adb_amt/all_adb_amt END AS revolving_rate
        FROM (
                SELECT a.external_acct_id, a.cycle_end_dt, a.vintage, a.stmt_num, a.activation_dt,a.acct_status_desc,a.apr_amt
                /* If account IS closed per stori requests, we put it AS other closed category */
                , CASE WHEN a.charge_off_ind = 1 THEN 'Charge_off'
                        WHEN acct_status_desc = 'Closed' AND acct_close_reason IN ('Paid Frozen Account Closing','Stori request') THEN 'Other_closed'
                /* If the frozen account IS paid off prior to the 180 charge off, we consider account IS closed */
                        WHEN acct_status_desc = 'Closed' AND acct_close_reason='Customer request' THEN 'Attrition'
                        ELSE 'Open'
                        END AS acct_close_status
                , CASE WHEN (a.stmt_tot_purchase_amt+a.stmt_tot_cash_withdraw_amt+COALESCE(a.service_pmt_amt, 0)>0 or a.curr_ledger_bal_amt>0)
                        AND delinq_days_cnt < 180 AND acct_close_status = 'Open' THEN 'Y' ELSE 'N' END AS acct_active_flag
                , CASE WHEN delinq_days_cnt > 90 AND delinq_days_cnt < 180 AND acct_close_status='Open' THEN 'Y' ELSE 'N' END AS dq_90_flag
                , CASE WHEN co.co_dt = a.cycle_end_dt AND co.external_acct_id IS NOT NULL THEN 'Y' ELSE 'N' END AS co_flag
                , CASE WHEN co.co_dt = a.cycle_end_dt THEN co.charge_off_amt ELSE 0 END AS co_balance
                , a.curr_credit_limit_amt AS curr_credit_limit
                , COALESCE(a.stmt_tot_purchase_amt + a.stmt_tot_cash_withdraw_amt + COALESCE(a.service_pmt_amt,0),0) AS indra_txn_amt
                , a.stmt_tot_purchase_amt + a.stmt_tot_cash_withdraw_amt + COALESCE(a.service_pmt_amt,0) AS total_txn_amt
                , a.stmt_tot_purchase_cnt + a.stmt_tot_cash_withdraw_cnt + COALESCE(sp.service_pmt_cnt, 0) AS total_txn_cnt

                , CASE WHEN COALESCE(a.stmt_charged_interest_amt, 0) > 0 AND a.stmt_num > 1 THEN 'Y' ELSE 'N' END AS revolving_ind
                , CASE WHEN a.stmt_num=1 THEN 0 
                        WHEN a.delinq_days_cnt>60 THEN COALESCE(a.revolver_avg_daily_bal_amt,a.all_avg_daily_bal_amt)            
                        ELSE a.revolver_avg_daily_bal_amt
                        END AS interest_bearing_adb 

                , a.curr_ledger_bal_amt
                , a.curr_tot_prncp_amt
                , CASE WHEN a.open_fee_amt>0 AND a.stmt_num=1 THEN 1 ELSE 0 END AS open_fee_acct_flag

                , a.all_avg_daily_bal_amt AS all_adb
                , a.revolver_avg_daily_bal_amt AS indra_adb_amt

                , a.stmt_rev_purchase_amt + a.stmt_posted_non_of_pmt_amt + a.stmt_posted_other_credit_amt AS all_payment_amt
                , GREATEST(0, a.stmt_paid_interest_amt - a.stmt_tot_refund_interests_amt) AS interest_paid_adj
                , GREATEST(0, a.stmt_paid_fee_amt - a.stmt_tot_refund_fee_amt) AS fee_paid_adj
                , GREATEST(0, a.stmt_paid_vat_amt - a.stmt_tot_vat_refund_fee_amt - a.stmt_tot_vat_refund_interests_amt) AS vat_paid_adj
                , all_payment_amt - interest_paid_adj - fee_paid_adj - vat_paid_adj AS stmt_cycle_prncp_payment
                FROM ua.wen_stmt_acct_table_v3 AS a 

                LEFT JOIN {{ ref ('chargeoff_dtls') }} AS co
                ON co.external_acct_id = a.external_acct_id
                AND co.co_dt <= a.cycle_end_dt

                LEFT JOIN (
                        SELECT a.external_acct_id 
                        , b.cycle_end_date
                        , SUM(1) AS service_pmt_cnt 
                        FROM mini_program_npi_class_1.superapp_transaction_details AS a 
                        INNER JOIN ua.statement_cycle_list AS b 
                        ON a.arcus_txn_dt BETWEEN b.cycle_start_date AND b.cycle_end_date
                        GROUP BY 1,2 
                ) AS sp
                ON a.external_acct_id = sp.external_acct_id
                AND a.cycle_end_dt = sp.cycle_end_date

                WHERE a.activation_dt IS NOT NULL
                AND a.acct_status_desc <> 'Inactive'
                AND a.stmt_num>0

        ) AS base
        GROUP BY 2,3,4

     ) AS vtg 
     
INNER JOIN {{ ref ('all_balances') }} AS alb
ON vtg.cycle_end_dt = alb.cycle_end_dt AND vtg.vintage = alb.vintage
ORDER BY 2,3,4