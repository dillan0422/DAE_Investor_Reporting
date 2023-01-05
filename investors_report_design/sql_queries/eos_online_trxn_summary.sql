WITH online_txrxn AS (SELECT cycle_end_dt 
       , SUM(CASE WHEN online_purchase_cnt>0 THEN 1 ELSE 0 END) AS online_acct_cnt 
       , SUM(online_purchase_cnt) AS online_purchase_cnt
       , SUM(online_purchase_amt) AS online_purchase_amt  
       , SUM(purchase_cnt) AS purchase_cnt 
       , SUM(purchase_amt) AS purchase_amt
FROM (SELECT b.cycle_end_date AS cycle_end_dt
             , external_acct_id
             , SUM(CASE WHEN a.invoice_type IN (5) THEN 1 ELSE 0 END) AS pos_purchase_cnt 
             , SUM(CASE WHEN a.invoice_type IN (5) THEN a.invoice_amt ELSE 0 END) AS pos_purchase_amt
             , SUM(CASE WHEN a.invoice_type <> 5 THEN 1 ELSE 0 END) AS online_purchase_cnt
             , SUM(CASE WHEN a.invoice_type <> 5 THEN a.invoice_amt ELSE 0 END) AS online_purchase_amt
             , SUM(1) AS purchase_cnt
             , SUM(a.invoice_amt) AS purchase_amt
      FROM (SELECT external_acct_id
                   , invoice_type
                   , txn_processed_date
                   , abs(invoice_amt) AS invoice_amt
            FROM stori_ccm_npi_class_2.cc_card_daily_operations_detail_cur
            WHERE correction_flag = 0 
            AND cancelled_movement_flag = 0 AND (invoice_type IN (5,1013,1019) OR invoice_type BETWEEN 1598 AND 1807)
           ) AS a 
      INNER JOIN ua.statement_cycle_list b 
      ON a.txn_processed_date BETWEEN b.cycle_start_date AND b.cycle_end_date
      GROUP BY 1,2)
GROUP BY 1)

SELECT cycle_end_dt
       , ROUND(1.000000*online_purchase_cnt/purchase_cnt, 6) AS online_cnt_pct
       , ROUND(1.000000*online_purchase_amt/purchase_amt, 6) AS online_amt_pct
       , online_acct_cnt
FROM online_txrxn
ORDER BY cycle_end_dt;