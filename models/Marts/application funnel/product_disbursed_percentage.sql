WITH total_counts AS (
	SELECT count(*) AS total_disbursement FROM {{ref('int_loan_disbursement')}}
)
SELECT 
	product_name,
	round((count(*)::NUMERIC /
	t.total_disbursement) * 100, 2) AS disbursed_percentage
FROM {{ref('int_loan_disbursement')}} dp
CROSS JOIN total_counts t
GROUP BY 1, t.total_disbursement
ORDER BY 2 DESC