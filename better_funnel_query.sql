SELECT count(session_starting_timestamp) as session_starting_timestamp,
	count(pdp_timestamp) as pdp_timestamp,
	count(in_cart_timestamp) as in_cart_timestamp,
	count(checkout_timestamp) as checkout_timestamp,
	count(purchased_timestamp) as purchased_timestamp
FROM(
		SELECT *
		FROM(
				SELECT rank() over (
						partition by uuid,
						session_id
						order by revision desc
					) as revision_rnk,
					uuid,
					session_id,
					session_duration,
					session_starting_timestamp,
					in_cart_timestamp,
					checkout_timestamp,
					purchased_timestamp,
					purchased_value,
					sign(
						sum(in_cart_indicator) OVER (partition by uuid, session_id)
					) AS in_cart_indicator,
					sign(
						sum(purchased_indicator) OVER (partition by uuid, session_id)
					) AS purchased_indicator
				FROM metrical_lake_db_prod.prod_visit_reports_json
				WHERE survey_id = 'replace_id'
					AND date_con 
					AND transaction_id = 1
			)
		WHERE revision_rnk = 1
	) AS v
	LEFT JOIN (
		SELECT *
		FROM(
				SELECT rank() over (
						partition by uuid,
						session_id
						order by interaction_timestamp asc
					) as time_rnk,
					uuid,
					session_id,
					interaction_timestamp AS pdp_timestamp
				FROM metrical_lake_db_prod.prod_interactions_json
				WHERE survey_id = 'replace_id'
					AND date_con
					AND usermeta_itemid is not null
			)
		WHERE time_rnk = 1
	) AS i ON v.uuid = i.uuid
	AND v.session_id = i.session_id
