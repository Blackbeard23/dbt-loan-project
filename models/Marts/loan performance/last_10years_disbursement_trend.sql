WITH year_range AS (
	SELECT generate_series(
		(date_part('year', current_date) - 10)::int,
		(date_part('year', current_date))::int
	) AS year
)
SELECT year,
		count(id.disbursement_date) as num_of_disbursement
FROM year_range yr
LEFT JOIN {{ ref('int_loan_disbursement') }} id
ON date_part('year', disbursement_date) = yr.year
GROUP BY year
ORDER BY 1