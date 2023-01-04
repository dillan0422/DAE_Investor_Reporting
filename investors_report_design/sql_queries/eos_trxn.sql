WITH eos_trxn_summary AS (
SELECT dt.cycle_end_date
	,SUM(1) AS tot_txn_cnt
	,SUM(invoice_amt) AS tot_txn_amt 
	
	,SUM(CASE WHEN invoice_type IN (1013,1019) THEN 1 ELSE 0 END) AS online_txn_cnt
	,SUM(CASE WHEN invoice_type IN (1013,1019) THEN invoice_amt ELSE 0 END) AS online_txn_amt
	
	,SUM(CASE WHEN activity_code IN (5411) THEN 1 ELSE 0 END) AS grocery_txn_cnt
	,SUM(CASE WHEN activity_code IN (5541) THEN 1 ELSE 0 END) AS gas_txn_cnt
	,SUM(CASE WHEN activity_code IN (5814) THEN 1 ELSE 0 END) AS fast_food_txn_cnt
	,SUM(CASE WHEN activity_code IN (5812) THEN 1 ELSE 0 END) AS restaurant_txn_cnt
	,SUM(CASE WHEN activity_code IN (4814,4900,4899,5964,5399) THEN 1 ELSE 0 END) AS utility_txn_cnt
	--,SUM(CASE WHEN activity_code IN (5964,5399) THEN 1 ELSE 0 END) AS digital_pmt_txn_cnt
	,SUM(CASE WHEN activity_code IN (5499) THEN 1 ELSE 0 END) AS convenience_txn_cnt
	,SUM(CASE WHEN activity_code IN (5311) THEN 1 ELSE 0 END) AS retail_txn_cnt
	,SUM(CASE WHEN activity_code IN (5912) THEN 1 ELSE 0 END) AS pharm_txn_cnt
	
	,SUM(CASE WHEN activity_code IN (5411) THEN invoice_amt ELSE 0 END) AS grocery_txn_amt
	,SUM(CASE WHEN activity_code IN (5541) THEN invoice_amt ELSE 0 END) AS gas_txn_amt
	,SUM(CASE WHEN activity_code IN (5814) THEN invoice_amt ELSE 0 END) AS fast_food_txn_amt
	,SUM(CASE WHEN activity_code IN (5812) THEN invoice_amt ELSE 0 END) AS restaurant_pmt_txn_amt
	,SUM(CASE WHEN activity_code IN (4814,4900,4899,5964,5399) THEN invoice_amt ELSE 0 END) AS utility_txn_amt
	--,SUM(CASE WHEN activity_code IN (5964,5399) THEN invoice_amt ELSE 0 END) AS digital_pmt_txn_amt
	,SUM(CASE WHEN activity_code IN (5499) THEN invoice_amt ELSE 0 END) AS convenience_pmt_txn_amt
	,SUM(CASE WHEN activity_code IN (5311) THEN invoice_amt ELSE 0 END) AS retail_pmt_txn_amt
	,SUM(CASE WHEN activity_code IN (5912) THEN invoice_amt ELSE 0 END) AS pharm_pmt_txn_amt
		
FROM 
(SELECT	external_acct_id
  		,invoice_date
  		,invoice_ref_num
  		,invoice_type
  		,ABS(invoice_amt) AS invoice_amt
  		,auth_num
  		,txn_processed_date
		,accounting_date
  		,txn_seq_num
		,activity_code
		,ROW_NUMBER() OVER(PARTITION BY external_acct_id,txn_processed_date,txn_seq_num) AS rn
FROM stori_ccm_npi_class_1.cc_card_daily_operations_detail_cur
WHERE invoice_type IN (5,1013,1019)
AND correction_flag=0
AND cancelled_movement_flag = 0
) txn 
INNER JOIN ua.statement_cycle_list dt 
ON txn.invoice_date BETWEEN dt.cycle_start_date AND dt.cycle_end_date 
AND txn.rn = 1
GROUP BY 1 
)

SELECT cycle_end_date
       , ROUND(1.000000*online_txn_cnt/tot_txn_cnt, 6) AS online_txn_pct
       , ROUND(1.000000*grocery_txn_cnt/tot_txn_cnt, 6) AS grocery_txn_pct
       , ROUND(1.000000*gas_txn_cnt/tot_txn_cnt, 6) AS gas_txn_pct
       , ROUND(1.000000*fast_food_txn_cnt/tot_txn_cnt, 6) AS fast_food_txn_pct   
       , ROUND(1.000000*restaurant_txn_cnt/tot_txn_cnt, 6) AS restaurant_txn_pct   
       , ROUND(1.000000*utility_txn_cnt/tot_txn_cnt, 6) AS utility_txn_pct   
       , ROUND(1.000000*convenience_txn_cnt/tot_txn_cnt, 6) AS convenience_txn_pct   
       , ROUND(1.000000*retail_txn_cnt/tot_txn_cnt, 6) AS retail_txn_pct   
       , ROUND(1.000000*pharm_txn_cnt/tot_txn_cnt, 6) AS pharm_txn_pct
       
       , ROUND(1.000000*online_txn_amt/tot_txn_amt, 6) AS online_txn_amt
       , ROUND(1.000000*grocery_txn_amt/tot_txn_amt, 6) AS grocery_txn_amt
       , ROUND(1.000000*gas_txn_amt/tot_txn_amt, 6) AS gas_txn_amt
       , ROUND(1.000000*fast_food_txn_amt/tot_txn_amt, 6) AS fast_food_txn_amt   
       , ROUND(1.000000*restaurant_pmt_txn_amt/tot_txn_amt, 6) AS restaurant_pmt_txn_amt   
       , ROUND(1.000000*utility_txn_amt/tot_txn_amt, 6) AS utility_txn_amt
       , ROUND(1.000000*convenience_pmt_txn_amt/tot_txn_amt, 6) AS convenience_pmt_txn_amt   
       , ROUND(1.000000*retail_pmt_txn_amt/tot_txn_amt, 6) AS retail_pmt_txn_amt  
       , ROUND(1.000000*pharm_pmt_txn_amt/tot_txn_amt, 6) AS pharm_pmt_txn_amt      
FROM eos_trxn_summary
ORDER BY cycle_end_date;