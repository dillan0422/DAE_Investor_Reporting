WITH incremental_co AS (
SELECT external_acct_id 
       , MIN(cycle_end_dt) AS co_cycle_end_dt 
FROM ua.wen_stmt_acct_table_v3
WHERE charge_off_ind = 1
GROUP BY 1  
),

dq_all_acct_info AS (
SELECT drv.vintage 
       , drv.cycle_end_dt 
       , drv.stmt_num 
       , SUM(drv.curr_posted_bal_amt) AS os_bal 
       , SUM(CASE WHEN drv.delinq_status IN ('DQ 1-30') THEN drv.curr_tot_prncp_amt ELSE 0 END) AS dq1_30_prncp_bal
       , SUM(CASE WHEN drv.delinq_status IN ('DQ 31-60') THEN drv.curr_tot_prncp_amt ELSE 0 END) AS dq31_60_prncp_bal
       , SUM(CASE WHEN drv.delinq_status IN ('DQ 61-90') THEN drv.curr_tot_prncp_amt ELSE 0 END) AS dq61_90_prncp_bal
       , SUM(CASE WHEN drv.delinq_status IN ('DQ 91-120','DQ 121-150','DQ 151-180') THEN drv.curr_tot_prncp_amt ELSE 0 END) AS dq91_180_prncp_bal
       , SUM(CASE WHEN co.external_acct_id IS NOT NULL THEN 1 ELSE 0 END) AS co_cnt
       , SUM(CASE WHEN co.external_acct_id IS NOT NULL THEN drv.curr_tot_prncp_amt ELSE 0 END) AS co_amt 
FROM ua.wen_stmt_acct_table_v3 drv 
LEFT OUTER JOIN incremental_co co 
ON drv.external_acct_id = co.external_acct_id
AND drv.cycle_end_dt = co.co_cycle_end_dt
LEFT OUTER JOIN 
    (SELECT external_acct_id
    FROM fraud.application_fraud_label 
    WHERE fraud_type <> 'Payment default'
    GROUP BY 1 
    ) AS frd 
ON drv.external_acct_id = frd.external_acct_id
WHERE drv.acct_status_desc NOT IN ('Closed','Inactive')
AND drv.cycle_end_dt>=drv.activation_dt
AND frd.external_acct_id IS NULL  
GROUP BY 1,2,3
)

SELECT cycle_end_dt
       , stmt_num 
       , vintage
       , SUM(os_bal) AS os_bal 
       , SUM(dq1_30_prncp_bal) AS dq1_30_prncp_bal 
       , SUM(dq31_60_prncp_bal) AS dq31_60_prncp_bal 
       , SUM(dq61_90_prncp_bal) AS dq61_90_prncp_bal 
       , SUM(dq91_180_prncp_bal) AS dq91_180_prncp_bal 
       , SUM(co_cnt) AS co_cnt
       , SUM(co_amt) AS co_amt 
FROM dq_all_acct_info 
GROUP BY 1,2,3
ORDER BY cycle_end_dt;