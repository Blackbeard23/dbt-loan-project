SELECT 
	lo.*,
	CASE 
		WHEN ild.disbursement_id IS NULL THEN 'offer not disbursed'
		WHEN irm.disbursement_id IS NULL THEN 'defaulted'
		WHEN 3 = ANY(irm.missed_con_month)
			OR irm.percentage_paid < 50 THEN 'defaulted'
		WHEN irm.percentage_paid = 100 THEN 'fully paid'
		ELSE 'active'
	END AS loan_status,
    irm.avg_delay_days
FROM {{ref('loan_offers_raw')}} lo
LEFT JOIN {{ref('int_loan_disbursement')}} ild 
ON lo.offer_id = ild.offer_id
LEFT JOIN {{ref('int_repayment_metrics')}} irm 
ON ild.disbursement_id = irm.disbursement_id