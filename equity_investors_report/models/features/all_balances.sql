/* Number of accounts and total principal balance by vintage for each cycle end date */
{{
    config(schema = 'da_stage',
        materialized = 'table')
}}

SELECT
    cycle_end_dt
	, vintage
	, count(DISTINCT acct_id) AS all_accounts 
	, sum(curr_tot_prncp_amt) AS all_prncp_balance 
FROM 
    ua.wen_stmt_acct_table_v3 
WHERE
    vintage IS NOT NULL 
    AND acct_status_desc <> 'Inactive'
GROUP BY 1, 2 