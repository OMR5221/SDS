timing start fix_maps;

DECLARE
    
    v_hospital_id NUMBER := ##;
    v_acct_pool_id NUMBER := ##;
    v_mrn_pool_id NUMBER := ##;
    v_ucrn_pool_id NUMBER := ###;
    v_feed_id NUMBER := ##;


PROCEDURE fix_maps AS

    -- PB records:
    TYPE r_ec_rec IS RECORD (id NUMBER, table_oid NUMBER, hospital_id NUMBER, bad_claim_ref_num VARCHAR#(###), good_claim_ref_num VARCHAR#(###), mrn varchar#(###));
    TYPE t_ec_recs IS TABLE OF r_ec_rec INDEX BY BINARY_INTEGER;
    v_ec_recs t_ec_recs;

    -- SOURCE UCRN records:
    TYPE r_ul_rec IS RECORD (ucrn_list_id NUMBER, bad_claim_ref_num VARCHAR#(###), good_claim_ref_num VARCHAR#(###),medical_record_number VARCHAR#(###));
    TYPE t_ul_recs IS TABLE OF r_ul_rec INDEX BY BINARY_INTEGER;
    v_ul_recs t_ul_recs;

    -- NON PB RECORDS:
    TYPE r_npb_rec IS RECORD (id NUMBER, table_oid NUMBER, hospital_id NUMBER, claim_ref_num VARCHAR#(###));
    TYPE t_npb_recs IS TABLE OF r_npb_rec INDEX BY BINARY_INTEGER;
    v_npb_recs t_npb_recs;

BEGIN

    -- Get Acct Number Pool Id:
    SELECT DISTINCT 
	m.pool_id
    INTO v_acct_pool_id
    FROM ehr.map m
    JOIN ehr.pool ep
	on ep.pool_id = m.pool_id
    where m.hospital_id = ##
    and ep.pool_type_id = #;


    -- get MRN Pool Id:
    SELECT DISTINCT 
	m.pool_id
    INTO v_mrn_pool_id
    FROM ehr.map m
    JOIN ehr.pool ep
	on ep.pool_id = m.pool_id
    where m.hospital_id = ##
    and ep.pool_type_id = #;


    -- get UCRN Pool Id:
    SELECT DISTINCT 
	m.pool_id
    INTO v_ucrn_pool_id
    FROM ehr.map m
    JOIN ehr.pool ep
	on ep.pool_id = m.pool_id
    where m.hospital_id = ##
    and ep.pool_type_id = #;


    -- Get feed id:
    select distinct 
	feed_id 
    INTO v_feed_id
    from adt.feed 
    where covered_entity_system_id = ##;


    -- Processing instances with multuple claim ref nums:
    SELECT DISTINCT 
	id, 
	table_oid, 
	hospital_id,
	bad_claim_ref_num,
	account_number,
	mrn
    BULK COLLECT INTO v_ec_recs
    FROM APPSUPPORT.TSP###_#_final_mcf
    ;
    

    -- Fix PB Issue maps:
    IF v_ec_recs.COUNT > # THEN

	-- Want to check if we have a new ucrn value to lookup to:
	for i in v_ec_recs.FIRST .. v_ec_recs.LAST
	LOOP

	    -- UPDATE Maps:

	    -- update source.edi_###_claim TABLE:
	    UPDATE source.edi_###_claim ec
		SET ec.account_number = v_ec_recs(i).good_claim_ref_num
	    WHERE ec.feed_id = v_feed_id
	    AND ec.account_number = v_ec_recs(i).bad_claim_ref_num
	    AND ec.medical_record_number = v_ec_recs(i).mrn
	    AND ec.edi_###_claim_id = v_ec_recs(i).id
	    ;

	    COMMIT;
	    

	    -- Update ehr EDI ### to UCRN map:
	    UPDATE ehr.map m
		SET m.value = v_ec_recs(i).good_claim_ref_num
	    WHERE m.hospital_id = v_ec_recs(i).hospital_id
	    AND m.id = v_ec_recs(i).id
	    AND m.table_oid = v_ec_recs(i).table_oid
	    AND m.pool_id = v_ucrn_pool_id;

	    COMMIT;


	    -- Delete  prior edi ### account number map
	    -- keep MRN map and edi-ucrn maps
	    DELETE FROM ehr.map m
	    WHERE m.hospital_id = v_ec_recs(i).hospital_id 
	    AND m.table_oid = v_ec_recs(i).table_oid
	    AND m.id = v_ec_recs(i).id
	    AND m.pool_id = v_acct_pool_id
	    ;

	    COMMIT;

	    
	    -- Rehash: Will recreate hashes for both mrn and edi###-ucrn maps:
	    ehr.autohash(v_ec_recs(i).id, v_ec_recs(i).table_oid);

	    COMMIT;

	END LOOP;
    
    END IF;

END;



PROCEDURE load_ce_id_queue AS

    v_user_id number;
    v_ticket_number number := ########;

    TYPE r_record IS RECORD (log_id INTEGER, log_table_oid INTEGER, hospital_id INTEGER, id INTEGER, table_oid INTEGER, src_id INTEGER, src_table_oid INTEGER, covered_entity_id INTEGER, location_id INTEGER, time_of_service TIMESTAMP, acct_num_pool_id INTEGER, acct_num VARCHAR#(##), feed_id INTEGER, update_code_event INTEGER, update_event INTEGER, update_drug_dispensation INTEGER,update_demographics INTEGER, update_visit_payer INTEGER, update_claim INTEGER, update_claim_line INTEGER);
    TYPE t_records IS TABLE OF r_record INDEX BY pls_integer;
    v_records t_records;


    --Settings
    v_uses_account_number integer := #; --checked
    v_uses_ucrn integer := #; --checked
    v_uses_modified_ucrn_claim integer := #; --checked
    v_covered_entity_system_id INTEGER := ##;

BEGIN


    -- Get user_id
    SELECT DISTINCT 
	u.user_id
    INTO v_user_id
    FROM freedom.users u
    WHERE u.username = ?;



    -- bulk collecting records
    SELECT *
    BULK COLLECT INTO v_records
    FROM 
    (
	SELECT 
	    e.event_id as log_id, 
	    freedom.tableoid('?') as log_table_oid,
	    v_hospital_id, 
	    e.id, 
	    e.table_oid, 
	    null as src_id, 
	    null as src_table_oid, 
	    e.covered_entity_id,
	    e.location_id,
	    e.effective_on as time_of_service, 
	    v_acct_pool_id,
	    m.value,
	    v_feed_id AS feed_id,
	    # AS update_code_event,
	    # AS update_event,
	    # AS update_drug_dipsensation,
	    # AS update_demographics,
	    # AS update_visit_payer,
	    # AS update_claim,
	    # AS update_claim_line
	from SOURCE.EDI_###_CLAIM c 
	join ehr.map m 
	    on c.edi_###_claim_id = m.id 
	    and m.table_oid = freedom.tableoid('?')
	    and m.hospital_id = v_hospital_id
	join ehr.pool ep
	    on m.pool_id = ep.pool_id
	    and ep.pool_type_id = # -- EDI### - UCRN LOOKUPS
	join ehr.event e
	    on e.hospital_id = v_hospital_id
	    and e.id = c.EDI_###_CLAIM_ID 
	    and e.TABLE_OID = freedom.tableoid('?')
	where c.feed_id = v_feed_id
	AND (c.edi_###_claim_id) in
	(
	    SELECT DISTINCT
		id
	    FROM APPSUPPORT.TSP###_#_final_mcf
	)
	AND NOT EXISTS 
	(
	    SELECT # 
	    FROM ehr.ce_id_queue q 
	    WHERE q.id = e.id 
	    and q.table_oid = e.table_oid 
	    and covered_entity_system_id = v_hospital_id 
	    and feed_id = v_feed_id
	)
    );



    IF v_records.count > # THEN
     
	-- loop through records

	FOR i in v_records.FIRST .. v_records.LAST 
	LOOP
	
	    -- Inserting back into the ce_id_queue

	    INSERT INTO ehr.ce_id_queue 
	    (
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
		PHARMACY_ID_EXPRESSION
	    )
	    values 
	    (
		v_records(i).id,
		v_records(i).table_oid,
		v_records(i).src_id,
		v_records(i).src_table_oid,
		v_feed_id,
		v_records(i).acct_num,
		null,
		v_uses_modified_ucrn_claim,
		v_uses_account_number,
		v_uses_ucrn,
		v_records(i).time_of_service,
		v_records(i).update_code_event,
		v_records(i).update_event,
		v_records(i).update_drug_dispensation,
		v_records(i).update_demographics,
		v_records(i).acct_num_pool_id,
		v_records(i).hospital_id,
		v_records(i).update_visit_payer,
		v_records(i).update_claim,
		v_records(i).update_claim_line,
		null
	    );
	       
	     --inserting into log

	    INSERT INTO freedom.record_changes_log (id, table_oid, ticket_number, note, user_id)
	    VALUES (v_records(i).log_id, v_records(i).log_table_oid, v_ticket_number,
	    '?' || v_records(i).location_id, v_user_id);

	    IF mod(i,###) = ## THEN
		COMMIT;
	    END IF;
   
	END LOOP;

	COMMIT;
    END IF;
END;



-- Main:
BEGIN

    fix_maps();
    load_ce_id_queue();

END;
/



timing stop;
