SELECT round(
	(count(*) FILTER (WHERE status = 'approved')::NUMERIC / count(*)) * 100
	 , 2
) AS approved_percentage 
FROM {{ ref('loan_applications_raw') }}