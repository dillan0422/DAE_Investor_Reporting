/* DBT Configuration */
{{
    config(schema = 'da_stage',
        materialized = 'table')
}}

SELECT 'Overall' AS portfolio
       , EXTRACT(YEAR FROM	cycle_end_dt ) * 100 + EXTRACT(MONTH FROM cycle_end_dt ) AS stmt_mth
       , delinq_status
       , SUM(1) AS actived_acct_cnt
       
       /* Credit line */
	   , SUM(curr_credit_limit) AS actived_credit_limit	
	   
       /* Card Usage - Volume */
	   , SUM(curr_ledger_bal_amt ) AS tot_end_os_bal_amt

	   , SUM(total_txn_amt) AS tot_txn_amt
	   , SUM(total_txn_cnt) AS tot_txn_cnt
	
	   , SUM(all_adb) AS tot_adb_amt_activated
	   , SUM(CASE WHEN revolving_ind = 'Y' AND delinq_days_cnt<90 THEN all_adb ELSE 0 END) AS revolving_adb
	   /* Principal account balance */
	   , SUM(curr_tot_prncp_amt) AS tot_prncp_amt
	
	   /*Charge off amt*/
	   , sum(COALESCE(charge_off_amt,0)) AS charge_off_amt_cum
	
FROM (SELECT a.external_acct_id
             , a.cycle_end_dt
             , a.stmt_num
		     , CASE WHEN co.external_acct_id IS NOT NULL THEN 'Charge_off'
			        WHEN acct_status_desc = 'Closed' AND acct_close_reason IN ('Paid Frozen Account Closing', 'Stori request') THEN 'Other_closed'
				    WHEN acct_status_desc = 'Closed' AND acct_close_reason = 'Customer request' THEN 'Attrition'
					ELSE 'Open'
				    END AS acct_close_status
			 , CASE WHEN (a.stmt_tot_purchase_amt + a.stmt_tot_cash_withdraw_amt + COALESCE(a.service_pmt_amt,0) > 0 OR a.curr_ledger_bal_amt > 0) AND delinq_days_cnt < 180 AND acct_close_status = 'Open' THEN 'Y'
					ELSE 'N'
				    END AS acct_active_flag
			 , a.delinq_status AS delinq_status
			 , a.delinq_days_cnt AS delinq_days_cnt
			 , a.curr_credit_limit_amt AS curr_credit_limit
			 , COALESCE(a.stmt_tot_purchase_amt + a.stmt_tot_cash_withdraw_amt + COALESCE(a.service_pmt_amt,0), 0) AS indra_txn_amt
             , a.stmt_tot_purchase_amt + a.stmt_tot_cash_withdraw_amt + COALESCE(a.service_pmt_amt,0) AS total_txn_amt
             , a.stmt_tot_purchase_cnt + a.stmt_tot_cash_withdraw_cnt + (CASE WHEN COALESCE(a.service_pmt_amt,0)>0 THEN 1 ELSE 0 END) AS total_txn_cnt
             , CASE WHEN COALESCE(a.stmt_charged_interest_amt,0)>0 AND a.stmt_num>1 THEN 'Y' ELSE 'N' END AS revolving_ind
             , a.curr_ledger_bal_amt
             , a.curr_tot_prncp_amt
             , a.all_avg_daily_bal_amt
             , CASE WHEN a.open_fee_amt > 0 AND stmt_num = 1 THEN 1
					ELSE 0
					END AS open_fee_acct_flag
             , a.all_avg_daily_bal_amt AS all_adb
             , CASE WHEN a.charge_off_ind = 1 THEN a.charge_off_amt ELSE 0 END AS charge_off_amt
	 FROM ua.wen_stmt_acct_table_v3 AS a
     LEFT JOIN {{ ref ('chargeoff_dtls') }} AS co 
     ON co.external_acct_id = a.external_acct_id AND co.co_dt <= a.cycle_end_dt
	 WHERE a.activation_dt IS NOT NULL AND a.acct_status_desc <> 'Inactive' AND a.stmt_num>0
	 ) AS base
GROUP BY 1,2,3