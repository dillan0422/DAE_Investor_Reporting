SELECT a.cycle_end_dt
       /* Cards */
       , a.actived_acct_cnt 
       , a.attrition_cnt 
       , a.co_cnt 
       , a.other_close_cnt 
       , a.open_acct_cnt 
       /* Credit Line */    
       , a.actived_credit_limit 
       , a.open_credit_limit
       , a.active_credit_limit
       /* Card Usage - Active Ratio */    
       , a.active_acct_cnt 
       , a.transactor_acct_cnt 
       , a.revolver_acct_cnt 
       /* Card Usage - Volume */       
       , a.tot_end_os_bal_amt 
       , a.tot_txn_amt 
       , a.tot_txn_cnt 
       , a.tot_adb_amt
       /* AVG DAU & MAU */
       , b.avg_dau
       , c.mau
       , b.num_of_dau
       , c.active_acct_cnt AS num_of_mau   
FROM (SELECT cycle_end_dt
		       /* Cards */
		       , SUM(actived_acct_cnt) AS actived_acct_cnt 
		       , SUM(attrition_cnt) AS attrition_cnt 
		       , SUM(co_cnt) AS co_cnt 
		       , SUM(other_close_cnt) AS other_close_cnt 
		       , SUM(open_acct_cnt) AS open_acct_cnt 
		       /* Credit Line */    
		       , SUM(actived_credit_limit) AS actived_credit_limit 
		       , SUM(open_credit_limit) AS open_credit_limit
		       , SUM(active_credit_limit) AS active_credit_limit
		       /* Card Usage - Active Ratio */    
		       , SUM(active_acct_cnt) AS active_acct_cnt 
		       , SUM(transactor_acct_cnt) AS transactor_acct_cnt 
		       , SUM(revolver_acct_cnt) AS revolver_acct_cnt 
		       /* Card Usage - Volume */       
		       , SUM(tot_end_os_bal_amt) AS tot_end_os_bal_amt 
		       , SUM(tot_txn_amt) AS tot_txn_amt 
		       , SUM(tot_txn_cnt) AS tot_txn_cnt 
		       , SUM(tot_adb_amt) AS tot_adb_amt 
	  FROM da_stage.vintage_sumry -- Select from portfolio_report schema
	  GROUP BY 1) AS a
LEFT JOIN (SELECT last_day(snap_dt) AS snap_dt
	              , SUM(dau)/SUM(1) AS avg_dau
	              , SUM(active_acct_cnt)/SUM(1) AS num_of_dau
           FROM stori_ccm_pl.da_cce_dau
           GROUP BY 1) AS b
ON LAST_DAY(a.cycle_end_dt) = b.snap_dt
LEFT JOIN stori_ccm_pl.da_cce_mau AS c
ON LAST_DAY(a.cycle_end_dt) = c.acct_yr_mth
ORDER BY cycle_end_dt;