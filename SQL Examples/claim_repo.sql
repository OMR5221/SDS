 /*
Reviewing query used to find events that fall within a timeperiod of the claim which could provide eligiblity:
 */

WITH significant_event AS (
	SELECT pm.person_id, e.source, e.eligible, e.event_id, e.effective_on,
	  ce.covered_entity_id, ce.date_written_window
	FROM ehr.person_map pm
	JOIN ehr.event e ON e.id = pm.id
		AND e.table_oid = pm.table_oid
		AND e.hospital_id = pm.hospital_id
	JOIN freedom.covered_entity ce
		ON ce.covered_entity_system_id = e.hospital_id
		AND ce.covered_entity_system_id = pm.hospital_id
	WHERE pm.person_id in (SELECT person_id
							FROM ehr.fast_person_data_cache
							WHERE (first_name=ehr.magic_first_name_cleaner(coalesce('TEST',''))
								OR first_name=regexp_replace('TEST',' .*','')
								--OR ct.flags LIKE '%match_last_name_dob%' -- in which case any first name is okay
							  )
							  AND (last_name='TEST'
								--OR (p.last_name IS NULL AND (ct.contract_id IN (173,193) OR ct.flags LIKE '%match_without_last_name%'))
							  )
							  AND date_of_birth=to_date('24-MAR-89','dd-mon-rr')
							   
							)
		AND ce.covered_entity_id = 2445
		AND e.eligible IN (0,1)
		AND e.covered_entity_id = 2445
		AND trunc(e.effective_on) <= trunc(to_date('28-SEP-15','dd-mon-rr')) - INTERVAL '30' days
		AND e.eligible_for > 0
		AND (
			trunc(to_date('28-SEP-15','dd-mon-rr')) <= e.effective_on + numtodsinterval(e.eligible_for, 'day')
			OR e.eligible = 0
		)

		-- Ensure the events being considered are inside the date written window:
		AND (
			-- if the incoming Rx date is empty (and already whitelisted above), don't enforce it
			to_date('01-SEP-15','dd-mon-rr') IS NULL
			OR (
				-- the event occurred before, or the same day, as the
				-- prescription_date and within written_window days after.
				trunc(e.effective_on) <= to_date('01-SEP-15','dd-mon-rr')
				AND (
					-- not using the window, so don't enforce it
					0 = 0
					-- Overriding is used by Sentrex when we try to make
					-- a claim potentionally eligible via COC\RVS.
					OR to_date('01-SEP-15','dd-mon-rr') <= trunc(e.effective_on)
						+ numtodsinterval(
							COALESCE(null, ce.date_written_window),
							'day'
						)
				)
			)
		)
), extended_window_event AS (
	-- Events falling inside the extended RVS window
	SELECT person_id, se.source, se.eligible, se.event_id, se.effective_on,
	  MAX(effective_on) OVER (PARTITION BY se.source, se.covered_entity_id) AS max_effective_on
	FROM significant_event se
	WHERE
		-- Not whitelisted
		to_date('01-SEP-15','dd-mon-rr') IS NOT NULL

		-- Using date written window
		AND 0 != 0

		-- Using extended window for COC/RVS
		AND null IS NOT NULL
), regular_window_event AS (
	-- Events falling inside date written window (or window not in use)
	SELECT person_id, se.source, se.eligible, se.event_id, se.effective_on,
	  MAX(effective_on) OVER (PARTITION BY se.source, se.covered_entity_id) AS max_effective_on
	FROM significant_event se
	WHERE
		-- Not whitelisted
		to_date('01-SEP-15','dd-mon-rr') IS NOT NULL

		-- The event occurred before, or the same day, as the
		-- prescription_date and within written_window days after.
		AND trunc(se.effective_on) <= to_date('01-SEP-15','dd-mon-rr')
		AND (
			-- Not using the window, so don't enforce it.
			0 = 0

			-- Using regular window without overriding.
			OR to_date('01-SEP-15','dd-mon-rr') <= trunc(se.effective_on)
				+ numtodsinterval(se.date_written_window, 'day')
		)
), whitelist_event AS (
	SELECT person_id, se.source, se.eligible, se.event_id, se.effective_on,
	  MAX(effective_on) OVER (PARTITION BY se.source, se.covered_entity_id) AS max_effective_on
	FROM significant_event se
	WHERE
		-- If incoming RX date is null and whitelisted the window does not apply
		to_date('01-SEP-15','dd-mon-rr') IS NULL
)
SELECT
	person_id, eligible, event_id,
	COALESCE(extended_window_eligible, 0), extended_window_event_id,
	COALESCE(regular_window_eligible, 0), regular_window_event_id,
	COALESCE(whitelist_eligible, 0), whitelist_event_id
FROM (
	-- Union of all window events - preserves original behavior of
	-- eligible and event_id and flattens eligible and event_id for
	-- each case into a single result that can then be used by the
	-- claims function to figure out which method is making the person
	-- eligible.
	SELECT person_id,
		MAX(e.eligible) AS eligible,
		MAX(e.event_id) AS event_id,
		MAX(e.ewe_eligible) AS extended_window_eligible,
		MAX(e.ewe_event_id) AS extended_window_event_id,
		MAX(e.rwe_eligible) AS regular_window_eligible,
		MAX(e.rwe_event_id) AS regular_window_event_id,
		MAX(e.wle_eligible) AS whitelist_eligible,
		MAX(e.wle_event_id) AS whitelist_event_id
	FROM (
		SELECT person_id, ewe.source, MAX(ewe.effective_on) AS effective_on,
			MAX(ewe.eligible) AS eligible, MAX(ewe.event_id) AS event_id,
			MAX(ewe.eligible) AS ewe_eligible, MAX(ewe.event_id) AS ewe_event_id,
			NULL AS rwe_eligible, NULL AS rwe_event_id,
			NULL AS wle_eligible, NULL AS wle_event_id
		FROM extended_window_event ewe
		WHERE ewe.effective_on = ewe.max_effective_on
		GROUP BY person_id, ewe.source
		HAVING SUM(CASE WHEN ewe.eligible = 0 THEN 1 ELSE 0 END) = 0

		UNION ALL

		SELECT person_id, rwe.source, MAX(rwe.effective_on) AS effective_on,
			MAX(rwe.eligible) AS eligible, MAX(rwe.event_id) AS event_id,
			NULL AS ewe_eligible, NULL AS ewe_event_id,
			MAX(rwe.eligible) AS rwe_eligible, MAX(rwe.event_id) AS rwe_event_id,
			NULL AS wle_eligible, NULL AS wle_event_id
		FROM regular_window_event rwe
		WHERE rwe.effective_on = rwe.max_effective_on
		GROUP BY person_id, rwe.source
		HAVING SUM(CASE WHEN rwe.eligible = 0 THEN 1 ELSE 0 END) = 0

		UNION ALL

		SELECT person_id, wle.source, MAX(wle.effective_on) AS effective_on,
			MAX(wle.eligible) AS eligible, MAX(wle.event_id) AS event_id,
			NULL AS ewe_eligible, NULL AS ewe_event_id,
			NULL AS rwe_eligible, NULL AS rwe_event_id,
			MAX(wle.eligible) AS wle_eligible, MAX(wle.event_id) AS wle_event_id
		FROM whitelist_event wle
		WHERE wle.effective_on = wle.max_effective_on
		GROUP BY person_id, wle.source
		HAVING SUM(CASE WHEN wle.eligible = 0 THEN 1 ELSE 0 END) = 0
	) e
	GROUP BY person_id, e.source
	ORDER BY MAX(e.effective_on) DESC
)-- WHERE rownum = 1