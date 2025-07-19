WITH repayment_status AS (
	SELECT disbursement_id,
			date_trunc('month', due_date) AS month,
			CASE
				WHEN actual_payment IS NOT NULL AND actual_payment <> 0 THEN 0
				ELSE 1
			END AS is_missed
	FROM {{ref('repayment_raw')}}
),
	missed_streak AS (
		SELECT disbursement_id,
				month,
				is_missed,
				sum(is_missed) OVER (PARTITION BY disbursement_id ORDER BY month) -
				ROW_NUMBER() OVER (PARTITION BY disbursement_id ORDER BY month) AS streak_group
		FROM repayment_status
),
	consecutive_counts AS (
		SELECT disbursement_id,
				MONTH,
				is_missed,
				ROW_NUMBER() OVER (PARTITION BY disbursement_id, streak_group ORDER BY month) AS miss_count
		FROM missed_streak
		WHERE is_missed = 1
)
SELECT r.*,
		COALESCE (cc.miss_count, 0) AS consecutive_missed_month
FROM {{ref('repayment_raw')}} r
LEFT JOIN consecutive_counts cc
ON r.disbursement_id = cc.disbursement_id
AND date_trunc('month', due_date) = cc.MONTH
ORDER BY r.repayment_id, r.disbursement_id, due_date