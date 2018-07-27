
    -- Fix dispensations without a hash

    SELECT *
    BULK COLLECT INTO v_recs
    FROM
    (
	select 
	id,
	table_oid
	from
	(
	    -- Dispensation exceptions
	    SELECT distinct
	    dd.id,
	    dd.table_oid
	    from ehr.drug_dispensation dd
	    LEFT JOIN EHR.HASH h 
		on h.id = dd.id 
		and h.table_oid = dd.table_oid 
		and h.hospital_id = dd.hospital_id
	    where
	    dd.hospital_id = v_hospital_id
	    AND h.id IS NULL
	)
	order by table_oid, id
    )
    ;

    FOR i in v_recs.FIRST .. v_recs.LAST 
    LOOP

	ehr.autohash(v_recs(i).id, v_recs(i).table_oid);

	COMMIT;

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_recs(i).id, v_recs(i).table_oid);
	COMMIT;
	      	    
    end loop;




    -- FIX Dispensation Exceptions:

    SELECT *
    BULK COLLECT INTO v_recs
    FROM
    (
	select 
	id,
	table_oid
	from
	(
	    -- Dispensation exceptions
	    SELECT distinct
	    dd.id,
	    dd.table_oid
	    from ehr.drug_dispensation dd
	    join ehr.map_exception me on me.id = dd.id and me.table_oid = dd.table_oid and me.hospital_id = dd.hospital_id
	    where
	    dd.hospital_id = v_hospital_id
	    -- AND PM.person_id IS NULL
	)
	order by table_oid, id
    )
    ;

    FOR i in v_recs.FIRST .. v_recs.LAST LOOP

	DELETE FROM ehr.map_exception me
	WHERE me.hospital_id = v_hospital_id 
	AND me.id = v_recs(i).id
	and me.table_oid = v_recs(i).table_oid;

	COMMIT;

	ehr.autohash(v_recs(i).id, v_recs(i).table_oid);

	COMMIT;

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_recs(i).id, v_recs(i).table_oid);
	COMMIT;
	      	    
    end loop;


    v_recs.delete;
    COMMIT;


    -- FIX Dispensation without a Perons:
    SELECT *
    BULK COLLECT INTO v_recs
    FROM
    (
	select 
	id,
	table_oid
	from
	(
	    -- Dispensation exceptions
	    SELECT distinct
	    dd.id,
	    dd.table_oid
	    from ehr.drug_dispensation dd
	    left join ehr.person_map pm 
		    on pm.id = dd.id 
		    and pm.table_oid = dd.table_oid 
		    and pm.hospital_id = dd.hospital_id
	    where
	    dd.hospital_id = v_hospital_id
	    AND PM.person_id IS NULL
	)
	order by table_oid, id
    )
    ;



    FOR i in v_recs.FIRST .. v_recs.LAST LOOP

	BEGIN

	ehr.autohash(v_recs(i).id, v_recs(i).table_oid);

	COMMIT;

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_recs(i).id, v_recs(i).table_oid);
	
	COMMIT;
	
	EXCEPTION WHEN OTHERS THEN

	    CONTINUE;
	END;

    end loop;



    -- FIX Dispensation without a EXCEPTION OR PERSON:
    SELECT *
    BULK COLLECT INTO v_recs
    FROM
    (
	select 
	id,
	table_oid
	from
	(
	    -- Dispensation exceptions
	    SELECT distinct
		dd.id,
		dd.table_oid
	    from ehr.drug_dispensation dd
	    LEFT JOIN ehr.map_exception me
	      on me.hospital_id = dd.hospital_id
	      and me.table_oid = dd.table_oid
	      and me.id = dd.id
	    LEFT JOIN ehr.person_map pm
	      on pm.hospital_id = dd.hospital_id
	      and pm.table_oid = dd.table_oid
	      and pm.id = dd.id
	    WHERE dd.hospital_id = v_hospital_id
	    and me.id IS NULL
	    and pm.id IS NULL
	)
	order by table_oid, id
    )
    ;



    FOR i in v_recs.FIRST .. v_recs.LAST LOOP

	BEGIN

	ehr.autohash(v_recs(i).id, v_recs(i).table_oid);

	COMMIT;

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_recs(i).id, v_recs(i).table_oid);
	
	COMMIT;
	
	EXCEPTION WHEN OTHERS THEN

	    CONTINUE;
	END;

    end loop;
