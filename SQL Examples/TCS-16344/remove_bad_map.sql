DECLARE
 
   n                NUMBER;
   v_person_id      INTEGER       := #########;
   v_hospital_id    NUMBER        := ####;
   
  
   --vars for bad maps
   TYPE bad_maps IS RECORD (id NUMBER,
			    table_oid NUMBER, 
			    value VARCHAR#(###),
			    pool_id NUMBER,
			    hospital_id NUMBER);
   TYPE t_bad_maps IS TABLE OF bad_maps INDEX BY pls_integer;
   v_bad_maps t_bad_maps;

   --vars for person maps
   TYPE id_tableoids IS RECORD (id NUMBER,
				table_oid NUMBER,
				hospital_id NUMBER,
				has_pm NUMBER,
				has_me NUMBER,
				is_demo NUMBER);
   TYPE t_id_tableoid IS TABLE OF id_tableoids INDEX BY pls_integer;
   v_id_tableoid t_id_tableoid;
 
BEGIN   

    --grab the bad maps to be deleted:
    SELECT DISTINCT
	  id
	 ,table_oid
	 ,acct_num
	 ,acct_num_pool_id
	 ,hospital_id  
    BULK COLLECT INTO v_bad_maps       
    FROM
    (
      select distinct 
	  pm.id,
	  pm.table_oid,
	  pm.hospital_id,
	  pm.created_on,
	  m#.pool_id acct_num_pool_id,
	  m#.value acct_num,
	  m#.pool_id mrn_pool_id,
	  m#.value mrn, 
	  MIN(PM.CREATED_ON) OVER (PARTITION BY m#.value) min_created_on
      from ehr.person p
      join ehr.person_map pm on pm.person_id = p.person_id
      join ehr.map m# 
	on m#.id = pm.id 
	and m#.table_oid = pm.table_oid 
	and m#.hospital_id = v_hospital_id
	and m#.pool_id in 
	(
	    select distinct 
		ep#.pool_id 
	    from ehr.pool ep# 
	    where ep#.pool_id = m#.pool_id 
	    and ep#.pool_type_id = #
	)
      join ehr.map m# 
	on m#.id = pm.id 
	and m#.table_oid = pm.table_oid 
	and m#.hospital_id = v_hospital_id
	and m#.pool_id in 
	(
	    select distinct 
		ep#.pool_id 
	    from ehr.pool ep# 
	    where ep#.pool_id = m#.pool_id 
	    and ep#.pool_type_id = #
	)
      WHERE pm.person_id IN (v_person_id)
      and m#.value IN
      (
	select distinct 
	  m#.value acct_num
	from ehr.person p
	join ehr.person_map pm 
	    on pm.person_id = p.person_id
	join ehr.map m# 
	    on m#.id = pm.id 
	    and m#.table_oid = pm.table_oid 
	    and m#.hospital_id = v_hospital_id
	    and m#.pool_id in 
	    (
		select distinct 
		    ep#.pool_id 
		from ehr.pool ep# 
		where ep#.pool_id = m#.pool_id 
		and ep#.pool_type_id = #
	    )
	join ehr.map m# 
	    on m#.id = pm.id 
	    and m#.table_oid = pm.table_oid 
	    and m#.hospital_id = v_hospital_id
	    and m#.pool_id in 
	    (
		select distinct 
		    ep#.pool_id 
		from ehr.pool ep# 
		where ep#.pool_id = m#.pool_id 
		and ep#.pool_type_id = #
	    )
	WHERE pm.person_id IN (v_person_id)
	GROUP BY
	m#.value
	HAVING count(distinct m#.value) > #
      )
      GROUP BY 
	  m#.pool_id,
	  m#.value,
	  m#.pool_id,
	  m#.value,
	  pm.id,
	  pm.table_oid,
	  pm.hospital_id,
	  pm.created_on
    )
    WHERE created_on <> min_created_on
    ;
    
 
   -- Found  bad maps - beginning loop to delete:
  
   --loop through the bad maps and delete
   IF v_bad_maps.COUNT > # THEN
 
     FOR i IN v_bad_maps.FIRST .. v_bad_maps.LAST LOOP
 
       DELETE FROM ehr.map
       WHERE id = v_bad_maps(i).id
       AND table_oid = v_bad_maps(i).table_oid
       AND pool_id = v_bad_maps(i).pool_id
       AND value = v_bad_maps(i).value;
 
       COMMIT;   
 
     END LOOP;
 
   END IF;
 
   --find the person maps via hash lookup:
    SELECT DISTINCT 
	id
        ,table_oid
	,hospital_id
	,has_pm
	,has_me
	,is_demo
    BULK COLLECT INTO v_id_tableoid
    from
    (
      SELECT 
	h.id, h.table_oid, h.hospital_id,
	CASE WHEN pm.person_id IS NULL THEN # ELSE # END has_pm,
	case when me.id is null then # else # end has_me,
	case when d.id is null then # else # end is_demo
      FROM ehr.hash h
      left join ehr.map_exception me on me.id = h.id and me.table_oid = h.table_oid and me.hospital_id = h.hospital_id
      left JOIN ehr.person_map pm
	on h.hospital_id = pm.hospital_id
	and pm.table_oid = h.table_oid
	and pm.id = h.id
      left JOIN ehr.demographics d
	on h.hospital_id = d.hospital_id
	and h.table_oid = d.table_oid
	and h.id = d.id
      WHERE (h.method, h.hash) IN
      (
	select distinct
	  h.method,
	  h.hash
	from ehr.person_map pm 
	join ehr.hash h
	  on h.hospital_id = pm.hospital_id
	  and h.table_oid = pm.table_oid
	  and pm.id = h.id
	where pm.person_id = v_person_id
      )
    )
    -- Ensure charges processed last:
    ORDER BY is_demo DESC;
 
     
     --loop through and delete the person maps so we can redemand
    IF v_id_tableoid.COUNT > # THEN
   
	FOR j IN v_id_tableoid.FIRST .. v_id_tableoid.LAST 
	LOOP

	    -- DELETE person maps:
	    IF v_id_tableoid(j).has_pm = # THEN
		DELETE FROM ehr.person_map
		WHERE id = v_id_tableoid(j).id
		AND table_oid = v_id_tableoid(j).table_oid
		AND hospital_id = v_id_tableoid(j).hospital_id;
                    
		COMMIT;
    	    END IF;

	    -- Delete map exceptions:
	    IF v_id_tableoid(j).has_me = # THEN
		DELETE FROM ehr.map_exception
		WHERE id = v_id_tableoid(j).id
		AND table_oid = v_id_tableoid(j).table_oid
		AND hospital_id = v_id_tableoid(j).hospital_id;
                    
		COMMIT;
    	    END IF;

           ehr.autohash(v_id_tableoid(j).ID, v_id_tableoid(j).table_oid);
           COMMIT;

	END LOOP;
 
       --redemand patients
       FOR k in v_id_tableoid.FIRST .. v_id_tableoid.LAST 
       LOOP         
           n := ehr.demand_person_from_id_tableoid(v_id_tableoid(k).ID, v_id_tableoid(k).table_oid);
	   COMMIT;
       END LOOP;
 
    END IF;
 
    COMMIT;
 
END;
/
