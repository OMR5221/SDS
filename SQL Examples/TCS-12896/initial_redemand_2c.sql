timing start initial_redemand_#c;

DECLARE
    
    v_hospital_id NUMBER := ##;
    v_acct_pool_id NUMBER := ##;
    v_mrn_pool_id NUMBER := ##;
    v_ucrn_pool_id NUMBER := ###;
    v_feed_id NUMBER := ##;

PROCEDURE initial_redemand AS

    v_hospital_id NUMBER := ##;

    TYPE r_map IS RECORD (id NUMBER, table_oid NUMBER, hospital_id NUMBER, has_except NUMBER, has_person NUMBER);
    TYPE t_maps IS TABLE OF r_map INDEX BY BINARY_INTEGER;
    v_maps t_maps;

    v_new_person_id NUMBER;

BEGIN
  
    -- Delete map_exceptions/person_maps associated to records and redemand:
    -- I think this needs to be run for all prior id/tableoids for both pd and non pb sources:
	-- a. Need to correct both first
	-- b. Remove ids for records not being processed due to more than # unique claim ref num?
    SELECT DISTINCT
	m.id,
        m.table_oid,
        m.hospital_id,
        CASE WHEN me.id IS NULL THEN # ELSE # END has_except,
        CASE WHEN pm.id IS NULL THEN # ELSE # END has_person
    BULK COLLECT INTO v_maps
    FROM 
    (	
	-- PB ISuue: # correct claim ref number found:
	SELECT 
	    id, 
	    hospital_id,
	    table_oid
	FROM appsupport.TSP###_#_ec_recs 
	WHERE num_good_claim_refs <= #
	UNION
	SELECT 
	    id, 
	    hospital_id,
	    table_oid
	FROM APPSUPPORT.TSP###_#_final_mcf
	UNION
	-- Non PB Issue: 
	SELECT
	    id, 
	    hospital_id,
	    table_oid
	FROM appsupport.TSP###_#_npb_recs
    ) m
    LEFT JOIN ehr.map_exception me
        ON me.hospital_id = m.hospital_id
        AND me.table_oid = m.table_oid
        AND me.id = m.id
    LEFT JOIN ehr.person_map pm
        ON pm.hospital_id = m.hospital_id
        AND pm.table_oid = m.table_oid
        AND pm.id = m.id
    ;


    
    -- Process maps for persons:
    IF v_maps.COUNT() > # THEN

        FOR i in v_maps.FIRST .. v_maps.LAST
        LOOP

            -- Delete map excepts by has lookup:

            IF v_maps(i).has_except = # THEN

                DELETE FROM ehr.map_exception me
                WHERE me.hospital_id = v_maps(i).hospital_id
                AND me.table_oid = v_maps(i).table_oid
                AND me.id = v_maps(i).id
                ;

                COMMIT;
	    
	    END IF;
	
	    IF v_maps(i).has_person = # THEN

		-- delete the prior person_maps created from bad ucrn references:
		DELETE FROM ehr.person_map pm
		WHERE pm.hospital_id = v_maps(i).hospital_id 
		AND pm.table_oid = v_maps(i).table_oid
		AND pm.id = v_maps(i).id
		;

		COMMIT;

	    END IF;

 
	    -- ce_id_queue already generates a new hash:
	    -- ehr.autohash(v_recs(i).id, v_recs(i).table_oid);
            -- COMMIT;


            -- Redemand for person map
            v_new_person_id := ehr.demand_person_from_id_tableoid(v_maps(i).id, v_maps(i).table_oid);
            COMMIT;
             
        END LOOP;

   END IF;

END;


-- Main:
BEGIN
    
    initial_redemand();

END;
/



timing stop;
