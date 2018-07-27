/*
Ticket# ###### - Cleanup Clara Maass Dispensations

Based on ticket# ######
*/

spool ./charge_reprocessing.txt;

set serveroutput on;

DECLARE
	i NUMBER := #;
	v_user_id NUMBER;
	v_username VARCHAR#(##) := '?';
	v_feed_id NUMBER := ###;
	v_ticket_number NUMBER := ######;
	v_dd_id NUMBER;
	v_hospital_id NUMBER := ####;
	v_pharmacy_id NUMBER;
	v_act_pool_id NUMBER;
	v_mrn_pool_id NUMBER;
	map_count NUMBER := #;
	need_to_hash NUMBER := #;
	
	v_charges_person_id NUMBER;
	v_charges_person_exists VARCHAR#(#);
        v_charges_person_count NUMBER := #;
	v_foo NUMBER;
	v_map_count NUMBER := #;
	v_code_event_id NUMBER;
	v_hashed_count NUMBER := #;
	v_dd_count NUMBER := #;
	v_rev_code_count NUMBER := #;
	v_cpt_code_count NUMBER := #;
	v_hcpc_code_count NUMBER := #;
	v_j_code_count NUMBER := #;
  
	TYPE v_record IS RECORD(
		account_number VARCHAR#(###), mrn VARCHAR#(###), 
		last_name VARCHAR#(###), first_name VARCHAR#(###), middle_initial VARCHAR#(###),
		inpatient_outpatient_status VARCHAR#(###), cdm_code VARCHAR#(###), 
		description VARCHAR#(###), 
		charge_amount VARCHAR#(###), rev_code VARCHAR#(###), cpt_code VARCHAR#(###), 
		hcpc_code VARCHAR#(###), j_code VARCHAR#(###), quantity VARCHAR#(###), 
		service_date VARCHAR#(###), service_time VARCHAR#(###), post_date VARCHAR#(###),
		journal VARCHAR#(###), feed_id NUMBER, imported_from VARCHAR#(###),
		charges_import_id NUMBER, 
		time_of_service TIMESTAMP, 
		is_outpatient NUMBER
    );
	TYPE v_recs IS TABLE OF v_record INDEX BY PLS_INTEGER;
	v_records v_recs;

BEGIN

    SELECT user_id INTO v_user_id 
    FROM freedom.users 
    WHERE username = v_username;


    SELECT DISTINCT 
	feed_id INTO v_feed_id
    FROM adt.feed
    WHERE covered_entity_system_id = v_hospital_id
    ;


    SELECT DISTINCT
	pharmacy_id INTO v_pharmacy_id
    FROM sentinel.site
    WHERE hospital_id = v_hospital_id;

    SELECT DISTINCT 
	m.pool_id INTO v_act_pool_id
    FROM ehr.map m
    JOIN ehr.pool ep
    on m.pool_id = ep.pool_id
    WHERE m.hospital_id = v_hospital_id
    AND ep.pool_type_id = #
    ;


    SELECT DISTINCT 
	m.pool_id INTO v_mrn_pool_id
    FROM ehr.map m
    JOIN ehr.pool ep
    on m.pool_id = ep.pool_id
    WHERE m.hospital_id = v_hospital_id
    AND ep.pool_type_id = #
    ;

    SELECT 
	ci.account_number, ci.mrn, ci.last_name, ci.first_name, ci.middle_initial, 
	ci.inpatient_outpatient_status,
	ci.cdm_code, ci.description, ci.charge_amount, ci.rev_code, ci.cpt_code, ci.hcpc_code, ci.j_code, 
	ci.quantity, ci.service_date, ci.service_time, ci.post_date, ci.journal, 
	ci.feed_id, ci.imported_from, ci.charges_import_id,
	to_timestamp(ci.service_date || '?') time_of_service, 
	CASE WHEN ci.inpatient_outpatient_status IN ('?') THEN # ELSE # END is_outpatient
	BULK COLLECT INTO v_records
    FROM SOURCE.charges_import ci
    LEFT JOIN ehr.drug_dispensation dd 
	    ON dd.src_id = ci.charges_import_id 
	    AND dd.src_table_oid = freedom.tableoid('?') 
	    AND dd.hospital_id = v_hospital_id
    where ci.feed_id = v_feed_id and ci.imported_on between to_timestamp('?')
    and regexp_like(cdm_code, '?') and dd.drug_dispensation_id is null
    ;

		
	FOR i IN # .. v_records.COUNT
	LOOP

	    dbms_output.put_line('?' || v_records(i).account_number);
		
		-- Check to see if source.charges_person_record exists
	    v_charges_person_exists := '?';
	    v_charges_person_id     := NULL;
		
		BEGIN
			SELECT '?', cp.charges_person_id
				INTO v_charges_person_exists, v_charges_person_id
			FROM source.charges_person cp
			WHERE cp.feed_id = v_feed_id
				AND 
				(
					(
						cp.account_number IS NULL AND 
						v_records(i).account_number IS NULL
					) 
					OR 
					(
						v_records(i).account_number IS NOT NULL AND 
						account_number IS NOT NULL AND 
						v_records(i).account_number = account_number
					)
				)
				AND 
				(
					(
						cp.mrn IS NULL AND 
						v_records(i).mrn IS NULL
					) 
					OR 
					(
						v_records(i).mrn IS NOT NULL AND 
						mrn IS NOT NULL AND 
						v_records(i).mrn = mrn
					)
				);
        EXCEPTION
        WHEN NO_DATA_FOUND THEN
			v_charges_person_exists := '?';
			v_charges_person_id     := NULL;
        END;

		-- Handle if found charges_person record
        IF v_charges_person_exists = '?' THEN
			BEGIN
				-- Update the charges_person_id value in source
				UPDATE source.charges_import
				SET charges_person_id = v_charges_person_id
				WHERE feed_id         = v_feed_id
				AND charges_import_id = v_records(i).charges_import_id;
				
			EXCEPTION
			WHEN OTHERS THEN
				DBMS_OUTPUT.PUT_LINE('?' || 
				v_records(i).charges_import_id || '?');
			END;

        ELSE 
			-- charges_person record doesn't exist (create new charges_person record)
			v_charges_person_count := v_charges_person_count + #;
		
			SELECT source.charges_person_id_seq.NEXTVAL INTO v_charges_person_id FROM dual;

            -- source.charges_person_view contains a trigger that automatically performs 
			-- inserts into source.charges_person, ehr.map and ehr.hash.
			BEGIN
				INSERT INTO source.charges_person_view (
					charges_person_id, feed_id, hospital_id, 
					account_number, account_number_pool, account_number_hash, 
					mrn, mrn_pool, mrn_hash
				)
				VALUES (
					v_charges_person_id, v_feed_id, v_hospital_id, 
					v_records(i).account_number, v_act_pool_id, 
					ehr.multihash_functions.multihash(v_act_pool_id, v_records(i).account_number),
					v_records(i).mrn, v_mrn_pool_id, 
					ehr.multihash_functions.multihash(v_mrn_pool_id, v_records(i).mrn)
				);
			
				-- Call demand_person
				v_foo := ehr.demand_person_from_id_tbloid_t (
					v_charges_person_id, freedom.tableoid('?')
				);

				-- Update the charges_person_id value in source
				UPDATE source.charges_import
				SET charges_person_id = v_charges_person_id
				WHERE feed_id         = v_feed_id
				AND charges_import_id = v_records(i).charges_import_id;

			EXCEPTION
				WHEN OTHERS THEN
				DBMS_OUTPUT.PUT_LINE(
					 '?' || 
					v_charges_person_id || '?' || v_records(i).charges_import_id || 
					'?');
			END;
		END IF;
		
		dbms_output.put_line('?' || v_charges_person_id);
	

	    
		insert into source.charge_without_pyxis(charge_id, table_oid, feed_id, item_id, charge_date, found_dispensations_quantity, original_charge_quantity)
			values(v_records(i).charges_import_id, freedom.tableoid('?'), v_feed_id, v_records(i).cdm_code, trunc(v_records(i).time_of_service), #, v_records(i).quantity);
	
			
		-- drug_dispensation
		INSERT INTO ehr.drug_dispensation (
			id, table_oid, src_id, src_table_oid, hospital_id, quantity, description, cdm_description,
			covered_entity_id, pharmacy_id, dispensed_on, is_outpatient, use_mrn_for_in_out_calcs, location_id
		)
		VALUES(
			v_charges_person_id, freedom.tableoid('?'), 
			v_records(i).charges_import_id, freedom.tableoid('?'), 
			v_hospital_id, v_records(i).quantity, v_records(i).cdm_code, v_records(i).description,
			NULL, v_pharmacy_id, v_records(i).time_of_service, v_records(i).is_outpatient,
			#, NULL)
		returning drug_dispensation_id INTO v_dd_id;
		
		v_dd_count := v_dd_count + #;
		
		-- code events
		IF v_records(i).rev_code IS NOT NULL THEN
			BEGIN
				SELECT ce.code_event_id into v_code_event_id
				FROM ehr.code_event ce
				WHERE ce.hospital_id = v_hospital_id
					AND ce.id = v_charges_person_id
					AND ce.table_oid = freedom.tableoid('?')
					AND ce.src_id = v_records(i).charges_import_id
					AND ce.src_table_oid = freedom.tableoid('?')
					AND ce.code = v_records(i).rev_code
					AND ce.type = '?';
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
				v_rev_code_count := v_rev_code_count + #;
				INSERT INTO ehr.code_event (
					id, table_oid, src_id, src_table_oid, code, type, covered_entity_id, 
					effective_on, hospital_id, location_id
				)
				VALUES (
					v_charges_person_id, freedom.tableoid('?'), 
					v_records(i).charges_import_id, freedom.tableoid('?'), 
					v_records(i).rev_code, '?', NULL, v_records(i).time_of_service, v_hospital_id, NULL
				);
			END;
        END IF;
        
        IF v_records(i).cpt_code IS NOT NULL THEN
			BEGIN
				SELECT ce.code_event_id into v_code_event_id
				FROM ehr.code_event ce
				WHERE ce.hospital_id = v_hospital_id
					AND ce.id = v_charges_person_id
					AND ce.table_oid = freedom.tableoid('?')
					AND ce.src_id = v_records(i).charges_import_id
					AND ce.src_table_oid = freedom.tableoid('?')
					AND ce.code = v_records(i).cpt_code
					AND ce.type = '?';
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
				v_cpt_code_count := v_cpt_code_count + #;
				INSERT INTO ehr.code_event (
					id, table_oid, src_id, src_table_oid, code, type, covered_entity_id, 
					effective_on, hospital_id, location_id
				)
				VALUES (
					v_charges_person_id, freedom.tableoid('?'), 
					v_records(i).charges_import_id, freedom.tableoid('?'), 
					v_records(i).cpt_code, '?', NULL, v_records(i).time_of_service, v_hospital_id, NULL
				);
			END;
		END IF;

		IF v_records(i).hcpc_code IS NOT NULL THEN
			BEGIN
				SELECT ce.code_event_id into v_code_event_id
				FROM ehr.code_event ce
				WHERE ce.hospital_id = v_hospital_id
					AND ce.id = v_charges_person_id
					AND ce.table_oid = freedom.tableoid('?')
					AND ce.src_id = v_records(i).charges_import_id
					AND ce.src_table_oid = freedom.tableoid('?')
					AND ce.code = v_records(i).hcpc_code
					AND ce.type = '?';
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
				v_hcpc_code_count := v_hcpc_code_count + #;
				INSERT INTO ehr.code_event (
					id, table_oid, src_id, src_table_oid, code, type, covered_entity_id, 
					effective_on, hospital_id, location_id
				)
				VALUES (
					v_charges_person_id, freedom.tableoid('?'), 
					v_records(i).charges_import_id, freedom.tableoid('?'), 
					v_records(i).hcpc_code, '?', NULL, v_records(i).time_of_service, v_hospital_id, NULL
				);
			END;
		END IF;
		
		IF v_records(i).j_code IS NOT NULL THEN
			BEGIN
				SELECT ce.code_event_id into v_code_event_id
				FROM ehr.code_event ce
				WHERE ce.hospital_id = v_hospital_id
					AND ce.id = v_charges_person_id
					AND ce.table_oid = freedom.tableoid('?')
					AND ce.src_id = v_records(i).charges_import_id
					AND ce.src_table_oid = freedom.tableoid('?')
					AND ce.code = v_records(i).j_code
					AND ce.type = '?';
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
				v_j_code_count := v_j_code_count + #;
				INSERT INTO ehr.code_event (
					id, table_oid, src_id, src_table_oid, code, type, covered_entity_id, 
					effective_on, hospital_id, location_id
				)
				VALUES (
					v_charges_person_id, freedom.tableoid('?'), 
					v_records(i).charges_import_id, freedom.tableoid('?'), 
					v_records(i).j_code, '?', null, v_records(i).time_of_service, v_hospital_id, null
				);	
			END;
		END IF;

		-- Finally, the ce id queue
		INSERT INTO ehr.ce_id_queue (
			ID,
			TABLE_OID,
			SRC_ID,
			SRC_TABLE_OID,
			FEED_ID,
			ACCOUNT_NUMBER,
			LOCATION_FIELD,
			USE_MODIFIED_UCRN_CLAIM,
			USE_ACCOUNT_NUMBER,
			USES_UCRN,
			PRIOR_TO,
			UPDATE_CODE_EVENT,
			UPDATE_EVENT,
			UPDATE_DRUG_DISPENSATION,
			UPDATE_DEMOGRAPHICS,
			POOL_ID,
			COVERED_ENTITY_SYSTEM_ID,
			UPDATE_VISIT_PAYER,
			UPDATE_CLAIM,
			UPDATE_CLAIM_LINE,
			PHARMACY_ID_EXPRESSION,
			MRN,
			MRN_POOL_ID,
			USE_MRN_FALLBACK
		)
		VALUES (
			v_charges_person_id, 
			freedom.tableoid('?'),
			v_records(i).charges_import_id,
			freedom.tableoid('?'),
			v_feed_id,
			v_records(i).account_number,
			NULL,
			#, -- use_modified_ucrn_claim
			#, -- use_account_number
			#,
			v_records(i).time_of_service,
			#,
			#,
			#,
			#,
			v_act_pool_id,
			v_hospital_id,
			#,
			#,
			#,
			NULL, 
			NULL, 
			v_mrn_pool_id, 
			#
		);
		
		--Add record change log 
		INSERT INTO  freedom.record_changes_log (id, table_oid, ticket_number, note, user_id)
			VALUES(v_dd_id, freedom.tableoid_e('?'), v_ticket_number, 
			'?', v_user_id);
 	
		-- Check to see if it is time to commit
		
		IF MOD(i,###) = # THEN
			COMMIT;
		END IF;
		
	END LOOP;

	commit;
	
	DBMS_OUTPUT.PUT_LINE(
		 '?' || v_charges_person_count || 
		'?'
	);
	DBMS_OUTPUT.PUT_LINE(
		 '?' || v_hashed_count || 
		'?'
	);
	
	DBMS_OUTPUT.PUT_LINE(
		 '?' || v_dd_count || 
		'?'
	);
	
	DBMS_OUTPUT.PUT_LINE(
		 '?' ||
		v_cpt_code_count || '?' ||
		v_hcpc_code_count || '?' || 
		v_j_code_count || '?' ||
		'?'
	);
	DBMS_OUTPUT.PUT_LINE(
		 '?');
 
END;
/


