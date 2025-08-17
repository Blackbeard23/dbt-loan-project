SELECT
	disbursement_id,
	sum(actual_payment) total_payment,
	sum(expected_payment) total_payback_loan,
	round(sum(actual_payment) / NULLIF(sum(expected_payment),0)* 100,1) percentage_paid,
	array_agg(DISTINCT consecutive_missed_month) missed_con_month,
    round(avg(delay_in_days)) avg_delay_days,
    count(actual_payment) FILTER (WHERE actual_payment IN (NULL, 0)) missed_payment_count
FROM {{ref('int_repayment_con_missed_month')}}
GROUP BY 1
ORDER BY 1