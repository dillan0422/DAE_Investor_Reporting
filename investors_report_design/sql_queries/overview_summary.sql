WITH delivery AS (
SELECT apc.external_acct_id, min(delivered_dt) AS delivered_dt
FROM 
    (SELECT 
        a.tracking_id
        ,a.tracking_number
        ,a.status_code
        ,a.courier
        ,COALESCE(a.card_ref_id,b.card_ref_id,c.card_ref_id,d.card_ref_id,e.card_ref_id,f.card_ref_id,g.card_ref_id) AS card_ref_id
        ,TO_DATE(substring(a.tracking_id, 1, 6), 'YYMMDD') delivered_dt
    FROM stori_tpd_npi_class_2.card_delivery_tracking_hist_cur a
    LEFT JOIN stori_tpd_npi_class_2.dhl_stori_outbound_cur b ON a.tracking_id=b.tracking_id
    LEFT JOIN stori_tpd_npi_class_2.h2h_stori_outbound_cur c ON a.tracking_id=c.tracking_id
    LEFT JOIN stori_tpd_npi_class_2.ninety_nine_mins_stori_outbound_cur d ON a.tracking_id=d.tracking_id
    LEFT JOIN stori_tpd_npi_class_2.h2h_stori_outbound_cur e ON a.tracking_id=e.tracking_id
    LEFT JOIN stori_tpd_npi_class_2.h2h_stori_manual_tracking_cur f ON a.tracking_id=f.tracking_id
    LEFT JOIN stori_tpd_npi_class_2.dhl_delivery_tmp_cur g ON a.tracking_id=g.tracking_id
    ) AS del
INNER JOIN stori_ccm_npi_class_2.cc_card_profile_cur AS cprf
ON cprf.ref_id = del.card_ref_id
INNER JOIN stori_ccm_npi_class_2.account_profile_cur apc 
ON cprf.acct_uuid = apc.acct_uuid
WHERE apc.external_acct_id IS NOT NULL and cprf.status IN (2,3)
GROUP BY 1)

SELECT drv.end_month_date
    , SUM(1) AS issued_card_cnt
    , SUM(CASE WHEN dlv.external_acct_id IS NOT NULL THEN 1 ELSE 0 END) AS delivery_cnt  
    , SUM(CASE WHEN drv.activation_dt<=drv.end_month_date THEN 1 ELSE 0 END) AS activated_acct_cnt 
FROM stori_ccm_pl.account_eom_ledger_info_v1 drv
left outer join delivery dlv 
ON drv.external_acct_id = dlv.external_acct_id
and dlv.delivered_dt<=drv.end_month_date
WHERE drv.booked_dt<=drv.end_month_date AND end_month_date > '2019-12-31'
GROUP BY 1
ORDER BY end_month_date;