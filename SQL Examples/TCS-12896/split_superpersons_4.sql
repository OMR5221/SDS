timing start correct_superpersons_#;

DECLARE

PROCEDURE split_superpersons AS

    v_hospital_id NUMBER := ##;

    TYPE r_sp IS RECORD (person_id NUMBER, id NUMBER, table_oid NUMBER, hospital_id NUMBER, is_demo NUMBER);
    TYPE t_sp IS TABLE OF r_sp INDEX BY BINARY_INTEGER;
    sp t_sp;

    v_new_person_id NUMBER;

BEGIN


    SELECT DISTINCT 
	person_id, 
	id, 
	table_oid, 
	hospital_id,
	is_demo
    BULK COLLECT INTO sp
    FROM  APPSUPPORT.TSP###_#_superperson_recs
    ;
    

    for i in sp.FIRST .. sp.LAST
    LOOP

	-- delete the person maps
	DELETE FROM ehr.person_map pm
	WHERE pm.hospital_id = sp(i).hospital_id 
	AND pm.person_id = sp(i).person_id	
	AND pm.table_oid = sp(i).table_oid
	AND pm.id = sp(i).id
	;

	COMMIT;

	-- Rehash:
	ehr.autohash(sp(i).id, sp(i).table_oid);
	
	COMMIT;

    END LOOP;

    -- REDEMAND DEMOS FIRST:    
    for i in sp.FIRST .. sp.LAST
    LOOP

	IF sp(i).is_demo = # THEN

	    -- Redemand for person map
	    v_new_person_id := ehr.demand_person_from_id_tableoid(sp(i).id, sp(i).table_oid);
	    
	    COMMIT;

	END IF;

    END LOOP;


    -- REDEMAND NON-DEMOS SECOND:    
    for i in sp.FIRST .. sp.LAST
    LOOP

	IF sp(i).is_demo = # THEN
	    -- Redemand for person map:
	    v_new_person_id := ehr.demand_person_from_id_tableoid(sp(i).id, sp(i).table_oid);
	    COMMIT;
	END IF;

    END LOOP;

END;


PROCEDURE redemand_sp_excepts AS

    v_hospital_id NUMBER := ##;

    TYPE r_sp IS RECORD (id NUMBER, table_oid NUMBER, hospital_id NUMBER, is_demo NUMBER);
    TYPE t_sp IS TABLE OF r_sp INDEX BY BINARY_INTEGER;
    sp t_sp;

    v_new_person_id NUMBER;

BEGIN


    SELECT DISTINCT 
	id, 
	table_oid, 
	hospital_id,
	is_demo
    BULK COLLECT INTO sp
    FROM  APPSUPPORT.TSP###_#_superperson_excepts
    ;
    

    -- DELETE and rehash map excepts:
    for i in sp.FIRST .. sp.LAST
    LOOP

	DELETE FROM ehr.map_exception me
	WHERE me.hospital_id = sp(i).hospital_id 
	AND me.table_oid = sp(i).table_oid
	AND me.id = sp(i).id
	;

	COMMIT;

	-- Rehash:
	ehr.autohash(sp(i).id, sp(i).table_oid);
	
	COMMIT;

    END LOOP;

    -- REDEMAND 
    for i in sp.FIRST .. sp.LAST
    LOOP

	IF sp(i).is_demo = # THEN

	    -- Redemand for person map
	    v_new_person_id := ehr.demand_person_from_id_tableoid(sp(i).id, sp(i).table_oid);
	    
	    COMMIT;

	END IF;

    END LOOP;


    -- REDEMAND NON-DEMOS SECOND:    
    for i in sp.FIRST .. sp.LAST
    LOOP

	IF sp(i).is_demo = # THEN
	    -- Redemand for person map:
	    v_new_person_id := ehr.demand_person_from_id_tableoid(sp(i).id, sp(i).table_oid);
	    COMMIT;
	END IF;

    END LOOP;

END;

-- Main Thread:
BEGIN

    -- attempt split of identified superpersons:
    split_superpersons();

    redemand_sp_excepts();

END;
/



timing stop;
