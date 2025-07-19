with disbursement_transform as (
    SELECT ld.disbursement_id,
		lo.offer_id,
		lo.application_id,
		ld.customer_id,
        la.application_date,
        lo.offer_date,
        ld.disbursement_date,
        la.loan_amount_requested,
		lo.loan_amount_offered,
        disbursed_amount,
		ld.fees_charged,        
		lo.term_months AS offer_term_month,
		lo.interest_rate AS offer_interest_rate,
		lp.product_id,
		lp.product_name,
		lp.interest_rate AS product_interest_rate,
		lp.loan_type,
		lp.max_term_months AS product_max_term_months
    FROM {{ ref('loan_disbursements_raw') }} ld
    LEFT JOIN {{ ref('loan_offers_raw') }} lo
    ON ld.offer_id = lo.offer_id
    LEFT JOIN {{ ref('loan_product_raw') }} lp
    ON lo.product_id = lp.product_id
    LEFT JOIN {{ ref('loan_applications_raw') }} la
    ON la.application_id = lo.application_id
    AND la.status = 'approved'
)
SELECT *,
        date_part('days', age(disbursement_date, application_date)) AS days_to_disbursement
FROM disbursement_transform