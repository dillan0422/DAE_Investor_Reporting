SELECT 
        a.cycle_end_dt
        , a.vintage
        , SUM(CASE WHEN a.acct_status_desc = 'Active' THEN 1 ELSE 0 END) AS active_acct_cnt 
        , SUM(a.stmt_charged_interest_amt) AS assessed_interest 
        , SUM(b.stmt_paid_interest_amt) AS collected_interest 
        , SUM(b.stmt_paid_interest_amt) - SUM(b.stmt_tot_refund_interests_amt) AS adj_interest_revenue_amt
        
        , SUM(a.stmt_charged_fee_amt) AS assessed_fee 
        , SUM(b.stmt_paid_fee_amt) - SUM(b.stmt_tot_refund_fee_amt) AS fee_revenue_amt 
        
        , SUM(CASE WHEN a.stmt_num=1 THEN a.open_fee_amt ELSE 0 END) AS open_fee_rev_amt 
        , SUM(a.stmt_tot_interchange_fee_amt) AS interchange_revenue_amt
        , SUM(COALESCE(a.service_pmt_commission_amt,0)) AS service_pmt_revenue_amt
        , COALESCE(adj_interest_revenue_amt,0)+COALESCE(fee_revenue_amt,0)+COALESCE(open_fee_rev_amt,0)+COALESCE(interchange_revenue_amt,0)+COALESCE(service_pmt_revenue_amt,0) as total_revenue

FROM ua.wen_stmt_acct_table_v3 a
LEFT OUTER JOIN ua.wen_stmt_acct_table_v3 b 
ON a.external_acct_id = b.external_acct_id AND a.cycle_end_dt = b.cycle_start_dt - 1
WHERE a.activation_dt IS NOT NULL
AND a.acct_status_desc <> 'Inactive'
GROUP BY 1,2
ORDER BY 1;