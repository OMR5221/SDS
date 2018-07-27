timing start redeamnd_missing_persons_#;

DECLARE

PROCEDURE redemand_no_pm AS

    v_person_id NUMBER;
    v_count_me NUMBER;
    v_num_persons_found NUMBER;
    v_lowest_found_person_id NUMBER;
    v_new_person_id NUMBER;
    TYPE r_rec IS RECORD
    (
	id NUMBER,
	TABLE_OID number,
	is_demo NUMBER
    );
    TYPE t_recs IS TABLE OF r_rec INDEX BY BINARY_INTEGER;
    v_recs t_recs;

    v_hospital_id NUMBER := ####;

BEGIN


    -- Fix Demographic Exceptions and Missing Person Maps:
    SELECT DISTINCT
	id,
	table_oid,
	is_demo
    BULK COLLECT INTO v_recs
    FROM
    (
	select 
	    h.id,
	    h.table_oid,
	    case when d.id is null then # else # end is_demo
	FROM ehr.hash h
	LEFT JOIN ehr.demographics d
	    ON d.hospital_id = h.hospital_id
	    AND d.table_oid = h.table_oid
	    and d.id = h.id
	where h.hospital_id = v_hospital_id
	and NOT EXISTS 
	(
	    SELECT pm.person_id
	    FROM ehr.person_map pm
	    WHERE pm.hospital_id = h.hospital_id
	    AND pm.table_oid = h.table_oid
	    and pm.id = h.id
	)
	and NOT EXISTS 
	(
	    SELECT me.id
	    FROM ehr.map_exception me
	    WHERE me.hospital_id = h.hospital_id
	    AND me.table_oid = h.table_oid
	    and me.id = h.id
	)
    )
    ORDER BY is_demo DESC, table_oid
    ;


    -- Delete map exceptions and rehash:
    FOR i in v_recs.FIRST .. v_recs.LAST
    LOOP

	ehr.autohash(v_recs(i).id, v_recs(i).table_oid);
	COMMIT;
	      	    
    end loop;
    COMMIT;

    -- Person Redemand for demographics:
    FOR i in v_recs.FIRST .. v_recs.LAST
    LOOP

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_recs(i).id, v_recs(i).table_oid);
	COMMIT;
	      	    
    end loop;
    COMMIT;

END;


-- Main Thread:
BEGIN

    redemand_no_pm();

END;
/



timing stop;
