timing start except_person_resolve_#;

DECLARE

PROCEDURE load_ptdb_issue_tables AS

    type r_mpl is record
    (
	person_id_list VARCHAR#(###)
    );
    type t_mpl is table of r_mpl index by pls_integer;
    v_mpl t_mpl;

    type r_demo is record
    (
	first_name VARCHAR#(###),
	last_name VARCHAR#(###),
	date_of_birth DATE
    );
    type t_demos is table of r_demo index by pls_integer;
    v_demos t_demos;
    v_base_person_record r_demo;

    v_person_list varchar#(####);
    v_person_list_count integer;

    v_merge_person_flag integer;
    v_persons_already_merged integer;

    v_hospital_id NUMBER := ####;

    v_new_person_id NUMBER;
    v_rm_exception NUMBER;
    v_num_persons NUMBER;
    v_final_num_persons NUMBER;
    f_person_list VARCHAR#(####);
    v_num_dobs NUMBER;
    v_superperson_found NUMBER;
    v_superperson_list VARCHAR#(###);
    v_already_merged NUMBER;


    type r_superperson is record
    (
	person_id NUMBER,
	is_superperson NUMBER
    );
    type t_superpersons is table of r_superperson index by pls_integer;
    v_superpersons t_superpersons;

    v_already_processed NUMBER;

BEGIN

    SELECT DISTINCT person_id_list
    BULK COLLECT INTO v_mpl
    FROM APPSUPPORT.TCS_#####_pl_classify
    WHERE can_merge = #
    AND has_sp = #
    AND num_persons > #
    ;

    IF v_mpl.COUNT > # THEN 
    
	FOR i in v_mpl.FIRST .. v_mpl.LAST 
	LOOP

	    select distinct /*+ parallel */
		COALESCE(d.first_name, p.first_name) first_name,
		COALESCE(d.last_name, p.last_name) last_name,
		COALESCE(d.date_of_birth, p.date_of_birth) date_of_birth
	    BULK COLLECT INTO v_demos
	    FROM 
	    (
		SELECT
		    mp.person_id,
		    mp.id,
		    mp.table_oid,
		    mp.hospital_id
		FROM APPSUPPORT.TCS_#####_mp_prelim mp
		WHERE mp.person_id IN
		(
		    SELECT 
			fpl.person_id
		    FROM
		    (
			SELECT DISTINCT
			    person_id_list,
			    person_id
			FROM APPSUPPORT.TCS_#####_hlj_merge
		    ) fpl
		    WHERE v_mpl(i).person_id_list = fpl.person_id_list
		)
	    ) p#
	    JOIN ehr.demographics d
		ON d.hospital_id = p#.hospital_id
		AND d.table_oid = p#.table_oid
		AND d.id = p#.id
	    JOIN ehr.person p
		on p.person_id = p#.person_id
	    ;

	--Start with default settings:

	v_person_list := ?;
	v_person_list_count := #;
	v_merge_person_flag := #;
	v_rm_exception := #;


	if v_demos.COUNT > # THEN

	    for j in v_demos.first .. v_demos.last
	    loop

	      v_person_list_count := v_person_list_count + #;

	      --Determine if persons match each other based on first name/last name/dob
	      if v_person_list_count = # then

		v_base_person_record := v_demos(j);

	      else
		-- Match:
		if 
		(
		    ( 
		      REGEXP_REPLACE(trim(both from v_demos(j).first_name), '?' , null) 
		      = REGEXP_REPLACE(trim(both from v_base_person_record.first_name), '?' , null)
		      OR 
		      REGEXP_REPLACE(trim(both from v_demos(j).last_name), '?' , null) 
		      = REGEXP_REPLACE(trim(both from v_base_person_record.last_name), '?' , null)
		    )
		    AND
		    (
		      v_demos(j).date_of_birth = v_base_person_record.date_of_birth
		    )
		) 
		OR
		(
		    ( 
		      REGEXP_REPLACE(trim(both from v_demos(j).first_name), '?' , null) 
		      = REGEXP_REPLACE(trim(both from v_base_person_record.first_name), '?' , null)
		      AND
		      REGEXP_REPLACE(trim(both from v_demos(j).last_name), '?' , null) 
		      = REGEXP_REPLACE(trim(both from v_base_person_record.last_name), '?' , null)
		    )
		    OR
		    (
		      v_demos(j).date_of_birth = v_base_person_record.date_of_birth
		    )
		)
		then
		  
		  CONTINUE;

		-- Mismatch:
		else

		  v_merge_person_flag := #;
		 
		  -- EXIT;

		end if;
	      end if;
	    end loop;


	    IF v_merge_person_flag = # then

		INSERT INTO appsupport.TCS_#####_merge_log (person_id_list)
		values(v_mpl(i).person_id_list);

		COMMIT;
	
	    ELSE
	    

		INSERT INTO appsupport.TCS_#####_mismatch_log (person_id_list)
		values(v_mpl(i).person_id_list);

		COMMIT;

	    END IF;

	ELSE
	
	    CONTINUE;

	END IF;

    end loop;

    end if;

    COMMIT;

END;



PROCEDURE merge_dupe_persons AS

    v_note VARCHAR#(##) := '?';
    v_user_id integer;
    v_approving_user_id integer;

BEGIN


    select user_id into v_user_id from freedom.users where username = ?;
    select user_id into v_approving_user_id from freedom.users where username = ?;

    FOR cur IN
    (
	select distinct PERSON_ID_LIST
	from appsupport.TCS_#####_merge_log
    )
    LOOP

	insert into ehr.manual_person_merge (person_id_list, note, created_by, created_on, approved_by, approved_on)
	values (cur.person_id_list, v_note, v_user_id, systimestamp, v_approving_user_id, systimestamp);
	
	COMMIT;

	-- Merge persons found to be duplicates:
	ehr.manual_merge_person(cur.person_id_list);

	commit;	

    END LOOP;

    commit;


END;


PROCEDURE resolve_merge_recs AS

    v_new_person_id NUMBER;
    v_count_me NUMBER;
    v_num_persons_found NUMBER;
    v_lowest_found_person_id NUMBER;

    TYPE r_me IS RECORD
    (
	id NUMBER,
	TABLE_OID number
    );
    TYPE t_me IS TABLE OF r_me INDEX BY BINARY_INTEGER;
    v_me t_me;

BEGIN

    -- Resolve exceptions related byhash lookup across hospitals:
    SELECT DISTINCT
	mpe.id,
	mpe.table_oid
    BULK COLLECT INTO v_me
    FROM APPSUPPORT.TCS_#####_mp_excepts mpe
    ;


    -- Process excepts associated to merges:
    FOR i in v_me.FIRST .. v_me.LAST 
    LOOP

	DELETE FROM ehr.map_exception me
	WHERE me.table_oid = v_me(i).table_oid
	AND me.id = v_me(i).id;
	COMMIT;

	ehr.autohash(v_me(i).id, v_me(i).table_oid);
	COMMIT;

    END LOOP;

    FOR i in v_me.FIRST .. v_me.LAST 
    LOOP

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_me(i).id, v_me(i).table_oid);
	COMMIT;

    END LOOP;
    
    commit;

    v_me.delete;
    COMMIT;

    -- Resolve non-exceptions related by hash lookup across hospitals:
    SELECT DISTINCT
	mpne.id,
	mpne.table_oid
    BULK COLLECT INTO v_me
    FROM APPSUPPORT.TCS_#####_mp_nonexcepts mpne
    ;


    -- Process non-excepts associated to merges:
    FOR i in v_me.FIRST .. v_me.LAST 
    LOOP

	ehr.autohash(v_me(i).id, v_me(i).table_oid);
	COMMIT;

    END LOOP;

    FOR i in v_me.FIRST .. v_me.LAST 
    LOOP

	v_new_person_id := ehr.demand_person_from_id_tableoid(v_me(i).id, v_me(i).table_oid);
	COMMIT;

    END LOOP;
    
    commit;

END;



PROCEDURE redemand_hosp AS

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
    )
    ORDER BY is_demo DESC, table_oid
    ;


    -- Delete map exceptions and rehash:
    FOR i in v_recs.FIRST .. v_recs.LAST
    LOOP
	
	DELETE FROM ehr.map_exception me
	WHERE me.id = v_recs(i).id
	and me.table_oid = v_recs(i).table_oid;

	COMMIT;

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



PROCEDURE del_temp_tables AS
    v_owner VARCHAR#(##) := ?;
BEGIN 

  for apptables in
  (
    select DISTINCT UT.TABLE_NAME
    from all_tables UT
    where UPPER(owner) = v_owner
    and table_name LIKE '?'
    and UPPER(table_name) NOT IN
    (
	'?',
	'?'
    )
  )
  LOOP
  
    EXECUTE IMMEDIATE '?' || apptables.table_name;
    COMMIT;

  END LOOP;
  
END;


-- Main Thread:
BEGIN
    -- Verify persons to be merged:
    load_ptdb_issue_tables();

    -- Merge duplicates identified:
    merge_dupe_persons();

    -- Attempt to resolve map excepts and missing person maps across all hospitals associated to merged persons:
    resolve_merge_recs();

    -- Attempt to resolve map excepts and missing person maps for KDMC only
    redemand_hosp();

    -- Remove all temp tables used except final logs:
    del_temp_tables();

END;
/



timing stop;
