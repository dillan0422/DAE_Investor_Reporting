/* Charge-off date and amount for each charged-off account */
{{
    config(schema = 'da_stage',
        materialized = 'table')
}}

SELECT co.external_acct_id
       , co.co_dt
       , b.charge_off_amt
       , b.curr_tot_prncp_amt 
FROM (SELECT external_acct_id
             , MIN(cycle_end_dt) AS co_dt
        FROM ua.wen_stmt_acct_table_v3
        WHERE delinq_days_cnt >180
        AND acct_status_desc <> 'Inactive'
        GROUP BY 1
     ) AS co
INNER JOIN ua.wen_stmt_acct_table_v3 AS b
ON co.external_acct_id = b.external_acct_id 
AND co.co_dt = b.cycle_end_dt 