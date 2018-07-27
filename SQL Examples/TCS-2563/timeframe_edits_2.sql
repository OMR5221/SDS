timing start edit_retro;

DECLARE

    v_feed_id number := ###;
    v_site_id NUMBER := ###;
    v_hospital_id NUMBER := ####;
    v_update_count number := #;
    v_ticket_number NUMBER := #######;
    v_split_time TIMESTAMP :=  to_timestamp('?');
    v_username varchar#(##) := ?;
    v_location_mapping_type_id NUMBER := #;
    v_is_in_four_walls NUMBER := #;
    v_is_non_sentinel NUMBER := #;

PROCEDURE split_time_frame(p_feed_id number, p_split_time timestamp)
AS
  v_curr_timeframe_id number;
  v_curr_start_time timestamp;
  v_curr_end_time timestamp;
  v_new_timeframe_id number;

BEGIN
  --  get the current timeframe id and the end_time of the time frame record to be split

  select location_mapping_timeframe_id, start_time, end_time
  INTO v_curr_timeframe_id, v_curr_start_time, v_curr_end_time
  from adt.location_mapping_timeframe 
  where feed_id = p_feed_id
  and (start_time is null or start_time < p_split_time)
  and (end_time is null or end_time > p_split_time);

  -- update timeframe with the v_split_time as end_time.      
  
  UPDATE adt.location_mapping_timeframe
  SET end_time = p_split_time
  WHERE location_mapping_timeframe_id = v_curr_timeframe_id;   
   

  -- create new timeframe for v_split_time as start_time 
  
  INSERT INTO ADT.location_mapping_timeframe (feed_id, start_time, end_time)
  VALUES (p_feed_id, p_split_time, v_curr_end_time)
  RETURNING location_mapping_timeframe_id into v_new_timeframe_id; 
  
  
  -- Create the range record with the new timeframe id with the same values as the old time frame id from location_mapping_timeframe.
     INSERT INTO adt.location_mapping_range (location_mapping_id,location_mapping_timeframe_id,feed_id, covered_entity_id,
          pharmacy_id, location_mapping_type_id,hardcoded_patient_status, is_in_four_walls, is_non_sentinel_type)
     SELECT location_mapping_id, v_new_timeframe_id, feed_id, covered_entity_id, pharmacy_id, location_mapping_type_id,
          hardcoded_patient_status, is_in_four_walls, is_non_sentinel_type
     FROM adt.location_mapping_range
     WHERE feed_id = p_feed_id
     AND location_mapping_timeframe_id = v_curr_timeframe_id;
 
  -- commit;

END;

PROCEDURE update_covered_entity_id(p_feed_id number) AS
  v_###b_ce_id number := #####;
BEGIN

  FOR cur IN 
  (
    select DISTINCT
	lmt.location_mapping_timeframe_id, 
	lm.location_mapping_id,
	lm.mapping_value,
	case when lmr.feed_id is null then # else # end has_lmr
      from adt.location_mapping_timeframe lmt
      JOIN adt.location_mapping lm
	on lm.feed_id = lmt.feed_id
	and lm.mapping_value IN
	(
	    select distinct location_code
	    FROM appsupport.TCS####_locations
	)
      left join adt.location_mapping_range lmr
	    ON lmr.feed_id = lmt.feed_id
	    AND lmr.LOCATION_MAPPING_TIMEFRAME_ID = lmt.LOCATION_MAPPING_TIMEFRAME_ID
	    AND lmr.location_mapping_id = lm.location_mapping_id
      left join freedom.covered_entity ce 
	    on ce.covered_entity_id = lmr.covered_entity_id
      where lmt.feed_id = p_feed_id --###
      and (ce.is_covered = # OR ce.covered_entity_id IS NULL)
      and trunc(lmt.start_time) >= to_date('?')
      order by lm.mapping_Value, lmt.location_mapping_timeframe_id
  )
  LOOP

    -- Update timefrange if it exists:
    IF cur.has_lmr = # THEN
	update adt.location_mapping_range
	set covered_entity_id = v_###b_ce_id
	where feed_id = p_feed_id
	and location_mapping_timeframe_id = cur.location_mapping_timeframe_id
	and location_mapping_id = cur.location_mapping_id;
    ELSE
	-- Add the new range with the necessary timerame and mapping ids:
	INSERT INTO adt.location_mapping_range 
	(
	    location_mapping_id, 
	    location_mapping_timeframe_id, 
	    feed_id, 
	    covered_entity_id, 
	    location_mapping_type_id, 
	    is_in_four_walls,
	    is_non_sentinel_type
	)
	SELECT DISTINCT 
	cur.location_mapping_id, 
	cur.location_mapping_timeframe_id,
	v_feed_id,
	v_###b_ce_id,
	v_location_mapping_type_id, 
	v_is_in_four_walls,
	v_is_non_sentinel
	FROM dual
	;	
    END IF;

  END LOOP;
 
END;


PROCEDURE reprocess_dispensations AS

  TYPE rec_disp IS RECORD (dd_id number);
  TYPE tt_disp IS TABLE OF rec_disp;
  v_disp tt_disp;
  v_user varchar#(###);

BEGIN

  SELECT DISTINCT drug_dispensation_id
  BULK COLLECT INTO v_disp
  FROM appsupport.tcs####_disps
  ;

    -- show_status('?');

  v_update_count := #;
  FOR i in v_disp.FIRST .. v_disp.LAST
  LOOP 
    
    UPDATE ehr.drug_dispensation
    SET location_id = NULL
    WHERE drug_dispensation_id = v_disp(i).dd_id;

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
              '?', -- pharmacy_id expression  (used in the ce_id_queue, but almost always null)
              v_ticket_number, -- ticket number
              v_username, -- user_id
              ####, -- commit interval default ####
              #, -- kick off ce_id_queue default # (this will run the ce_id_queue once the records have been loaded there, # means you will have to run it manually)
              #, -- fix sentinel eligibility default # (this will compare the ehr eligibility to sentinel and update sentinel where necessary, # only updates the ehr)
              #, -- only process null locations default # (only updates the records in your selection set with a null location_id, # updates ALL locations in your selection set)
              #, -- override_skipping_sentrex default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records. Set to # to override this and process Sentrex-related events. Used in conjunction with only_process_sentinel)
              #, -- only_process_sentinel default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records, only ehr.drug_dispensation records. Set to # to override this behavior and process events. Used in conjunction with override_skipping_sentrex)
              #, -- override_processing_adt default # (Until issues in ####### are resolved we do not want to reprocess ehr.event records (including ADT). If there is a reason to update ADT events, set flag to #)
              to_date('?'), -- earliest adt, or min date to process (you can go back further, but you will have to use the parameters that were used to pull in the data using UCRN, instead of ADT, and adjust your dates accordingly)
              to_date('?')); -- or max date to process  
              
  
END;

BEGIN
    -- create new timeframe records
    split_time_frame(v_feed_id, to_timestamp('?'));
    
    commit;  

    update_covered_entity_id(v_feed_id);

    commit;

    -- process dispensations
    reprocess_dispensations;
  
    commit;

    -- process pdap
    -- sentinel.pull_drugs_and_patients(v_site_id);
END;
/

timing stop;
