timing start loc_edit_retro;

DECLARE

v_feed_id number := ####;
v_site_id NUMBER := ###;
v_hospital_id NUMBER := ####;
v_update_count number := #;
v_ticket_number NUMBER := ########;
v_split_time TIMESTAMP := to_timestamp('?');
v_username varchar#(##) := ?;
v_location_mapping_type_id NUMBER := #;
v_is_in_four_walls NUMBER := #;
v_is_non_sentinel NUMBER := #;
v_new_timeframe_id number;
v_curr_timeframe_id number;
v_curr_start_time timestamp;
v_curr_end_time timestamp;
v_elig_ce_id NUMBER := ####;


TYPE r_loc IS RECORD (location_mapping_timeframe_id NUMBER, location_mapping_range_id NUMBER, location_mapping_id number, new_ce_id NUMBER);
TYPE t_locs IS TABLE OF r_loc;
v_locs t_locs;


PROCEDURE reprocess_dispensations AS
  TYPE rec_disp IS RECORD (dd_id number);
  TYPE tt_disp IS TABLE OF rec_disp;
  v_disp tt_disp;
  v_user varchar#(###);

BEGIN

    SELECT DISTINCT 
	drug_dispensation_id
    BULK COLLECT INTO v_disp
    FROM
    (
	SELECT DISTINCT
	    dd.drug_dispensation_id
	FROM ehr.drug_dispensation dd
	left join ehr.location l
	    on l.COVERED_ENTITY_SYSTEM_ID = dd.hospital_id
	    and l.location_id = dd.location_id
	left join ehr.location_code lc
	    on lc.COVERED_ENTITY_SYSTEM_ID = dd.hospital_id
	    and lc.location_id = dd.location_id
	LEFT JOIN freedom.covered_entity ce
	    on ce.covered_entity_id = dd.covered_entity_id
	WHERE dd.hospital_id = v_hospital_id
	and TO_DATE(TRUNC(dd.dispensed_on), '?') >= v_split_time
	AND
	(
	    l.location_code IN
	    (   
		?
	    )
	    OR
	    lc.location_code IN
	    (
		?
	    )
	)
	AND (ce.is_covered = # or ce.is_covered IS NULL)
    ) md
    ;

    -- show_status('?');

  v_update_count := #;
  FOR i in v_disp.FIRST .. v_disp.LAST
  LOOP 
    
    UPDATE ehr.drug_dispensation
	SET location_id = NULL
    WHERE hospital_id = v_hospital_id
    and drug_dispensation_id = v_disp(i).dd_id;

    COMMIT;

    v_update_count := v_update_count + sql%rowcount;

  END LOOP;
  commit;
  
--show_status('?');

-- show_status('?');

  ehr.loc_backfill_pkg.process_feed(
              v_feed_id, -- feed_id
              v_hospital_id, -- hospital_id/covered_entity_system_id
              null, -- location_field (found in source.pyxis_feed and the charge portion of the parser, usually null)
              #, -- use modified ucrn claim (found in the charges parser usually #)
              #, -- use account number (found in source.pyxis_feed, and charges parser usually #)
              #, -- uses ucrn  (found in charges parser usually #)
              #, -- use mrn fallback (found in source.pyxis_feed, and charges parser usually #)
              #, -- use_acct_num_to_look_up_ce (default is #, this is not widely used, but you will find it when its set in the charges parser)
              null, -- pharmacy_id expression  (used in the ce_id_queue, but almost always null)
              v_ticket_number, -- ticket number
              v_username, -- user_id
              ####, -- --commit interval default ####
              #, -- kick off ce_id_queue default # (this will run the ce_id_queue once the records have been loaded there, # means you will have to run it manually)
              #, -- fix sentinel eligibility default # (this will compare the ehr eligibility to sentinel and update sentinel where necessary, # only updates the ehr)
              #, -- only process null locations default # (only updates the records in your selection set with a null location_id, # updates ALL locations in your selection set)
              #, -- override_skipping_sentrex default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records. Set to # to override this and process Sentrex-related events. Used in conjunction with only_process_sentinel)
              #, -- only_process_sentinel default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records, only ehr.drug_dispensation records. Set to # to override this behavior and process events. Used in conjunction with override_skipping_sentrex)
              #, -- override_processing_adt default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records (including ADT). If there is a reason to update ADT events, set flag to #)
              TRUNC(v_split_time), -- earliest adt, or min date to process (you can go back further, but you will have to use the parameters that were used to pull in the data using UCRN, instead of ADT, and adjust your dates accordingly)
	    sysdate
	); -- or max date to process  
              
  
END;

BEGIN
    
    --  get the current timeframe id and the end_time of the time frame record to be split
    select location_mapping_timeframe_id, start_time, end_time
    INTO v_curr_timeframe_id, v_curr_start_time, v_curr_end_time
    from adt.location_mapping_timeframe 
    where feed_id = v_feed_id
    and (start_time is null or start_time < v_split_time)
    and (end_time is null or end_time > v_split_time);

    -- update timeframe
    UPDATE adt.location_mapping_timeframe
    SET end_time = v_split_time
    WHERE location_mapping_timeframe_id = v_curr_timeframe_id;   

    -- create new timeframe:
    INSERT INTO ADT.location_mapping_timeframe (feed_id, start_time, end_time)
    VALUES (v_feed_id, v_split_time, v_curr_end_time)
    RETURNING location_mapping_timeframe_id into v_new_timeframe_id; 


    -- Create the range record with the new timeframe id with the same values as the old time frame id from location_mapping_timeframe.

    INSERT INTO adt.location_mapping_range (location_mapping_id,location_mapping_timeframe_id,feed_id, covered_entity_id, pharmacy_id, location_mapping_type_id,hardcoded_patient_status, is_in_four_walls, is_non_sentinel_type)
    SELECT location_mapping_id, v_new_timeframe_id, feed_id, covered_entity_id, pharmacy_id,
	location_mapping_type_id, hardcoded_patient_status, is_in_four_walls, is_non_sentinel_type
    FROM adt.location_mapping_range
    WHERE feed_id = v_feed_id
    AND location_mapping_timeframe_id = v_curr_timeframe_id;

     
    COMMIT;


    -- Update non eleiglbe location maps for this location to be eligible:
    SELECT DISTINCT
	l.location_mapping_timeframe_id,
	l.location_mapping_range_id,
	l.location_mapping_id,
	v_elig_ce_id
    bulk collect into v_locs
    FROM
    (
	select DISTINCT
	    lmr.LOCATION_MAPPING_TIMEFRAME_ID,
	    lmr.LOCATION_MAPPING_RANGE_ID,
	    lmr.LOCATION_MAPPING_ID,
	    lmt.start_time,
	    lmt.end_time,
	    ce.covered_entity_id curr_ce_id
	FROM adt.location_mapping_timeframe lmt
	JOIN adt.location_mapping_range lmr
	  on lmt.feed_id = lmr.feed_id
	  and lmt.LOCATION_MAPPING_TIMEFRAME_ID = lmr.LOCATION_MAPPING_TIMEFRAME_ID
	JOIN adt.location_mapping lm
	  on lm.feed_id = lmt.feed_id
	  and lm.location_mapping_id = lmr.location_mapping_id
	LEFT JOIN freedom.covered_entity ce
	    ON ce.covered_entity_id = lmr.covered_entity_id
	WHERE lm.feed_id = v_feed_id
	AND upper(trim(both from lm.mapping_value)) IN
	(
	    ?
	)
    ) l
    WHERE 
    (
	-- Get all location timeframes to update:
	start_time >= v_split_time
    )
    ;



    IF v_locs.COUNT > # THEN

	FOR i IN v_locs.FIRST .. v_locs.LAST
	LOOP

	    UPDATE adt.location_mapping_range 
		SET covered_entity_id = v_locs(i).new_ce_id
	    WHERE feed_id = v_feed_id
	    and location_mapping_id = v_locs(i).location_mapping_id
	    and location_mapping_range_id = v_locs(i).location_mapping_range_id
	    and location_mapping_timeframe_id = v_locs(i).location_mapping_timeframe_id
	    ;

	    COMMIT;
	END LOOP;

    END IF;


    -- process dispensations
    reprocess_dispensations;

    commit;
END;
/

timing stop;
