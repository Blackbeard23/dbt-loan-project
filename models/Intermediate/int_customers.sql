WITH customer_tranformed AS (
	SELECT customer_id,
			full_name,
			(EXTRACT(YEAR FROM current_date) - EXTRACT(YEAR FROM signup_date)) + age AS current_age,
			employment_status,
			credit_score,
			CASE
				WHEN credit_score <= 499 THEN 'Very Poor'
				WHEN credit_score <= 579 THEN 'Poor'
				WHEN credit_score <= 669 THEN 'Fair'
				WHEN credit_score <= 739 THEN 'Good'
				WHEN credit_score <= 799 THEN 'Very Good'
				WHEN credit_score <= 850 THEN 'Excellent'
			END AS credit_score_category,
			monthly_income,
			CASE
				WHEN monthly_income < 50000 THEN 'Low Income'
				WHEN monthly_income <= 99999 THEN 'Lower-Middle'
				WHEN monthly_income <= 199999 THEN 'Middle Income'
				WHEN monthly_income <= 499999 THEN 'Upper Middle'
				WHEN monthly_income >= 500000 THEN 'High Income'
			END AS income_category,
			signup_date
	FROM {{ref('customers_raw')}}
)
SELECT *,
		CASE
			WHEN current_age <= 24 THEN 'Youth/Teen'
			WHEN current_age <= 44 THEN 'Young Adult'
			WHEN current_age <= 64 THEN 'Middle-Aged Adult'
			ELSE 'Senior/Edlderly' END AS age_group
FROM customer_tranformed