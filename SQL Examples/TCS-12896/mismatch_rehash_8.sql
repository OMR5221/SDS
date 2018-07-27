timing start mismatch_rehash_#;

SET SERVEROUTPUT ON SIZE UNLIMITED

DECLARE

PROCEDURE mismatch_rehash AS

    type r_mismatch is record
    (
	person_id_list VARCHAR#(###)
    );
    type t_mismatched is table of r_mismatch index by pls_integer;
    v_mismatched t_mismatched;
    

    TYPE r_rec IS RECORD
    (
	id NUMBER,
	TABLE_OID number
    );
    TYPE t_recs IS TABLE OF r_rec INDEX BY BINARY_INTEGER;
    v_bad_maps t_recs;

    v_new_person_id NUMBER;

BEGIN


    SELECT DISTINCT person_id_list
    BULK COLLECT INTO v_mismatched
    FROM 
    (
	SELECT person_id_list
	FROM appsupport.TSP###_mismatch_log
	UNION
	SELECT person_id_list
	FROM appsupport.TSP###_pl_classify
	WHERE can_merge = #
    );

    -- Loop through mismatches:
    FOR i in v_mismatched.FIRST .. v_mismatched.LAST
    LOOP

    
	-- get bad maps to be rehashed:
	SELECT DISTINCT 
            pm.id,
            pm.table_oid
	BULK COLLECT INTO v_bad_maps
        FROM ehr.person_map pm
	where (pm.person_id) in
        (
	    SELECT DISTINCT TO_NUMBER(p_id) as person_id
	    FROM
	    (
		SELECT trim(REGEXP_SUBSTR(v_mismatched(i).person_id_list,  '?', #, levels.column_value)) p_id
		FROM dual,
	    table(cast(multiset(select level from dual connect by level <= length (regexp_replace(v_mismatched(i).person_id_list, '?')) + #) as sys.OdciNumberList)) levels
	    -- CONNECT BY REGEXP_SUBSTR(person_id_list,  '?', #, level) IS NOT NULL
	  )
        );


	IF v_bad_maps.COUNT > # THEN

	    FOR i in v_bad_maps.FIRST .. v_bad_maps.LAST
	    LOOP

		ehr.autohash(v_bad_maps(i).id, v_bad_maps(i).table_oid);
		commit;
		
		delete from ehr.person_map pm
		where pm.table_oid = v_bad_maps(i).table_oid
		and pm.id = v_bad_maps(i).id;
		commit;
		
	    END LOOP;

	    FOR j in v_bad_maps.FIRST .. v_bad_maps.LAST
	    LOOP

		v_new_person_id := ehr.demand_person_from_id_tableoid(v_bad_maps(j).id, v_bad_maps(j).table_oid);
		commit;

	    END LOOP;

	END IF;


    END LOOP;

END;



-- Main Thread:
BEGIN

    mismatch_rehash();

END;
/



timing stop;
