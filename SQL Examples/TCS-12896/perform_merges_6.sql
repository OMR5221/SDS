SET SERVEROUTPUT OFF;

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
	date_of_birth DATE,
	ssn VARCHAR#(##)
    );
    type t_demos is table of r_demo index by pls_integer;
    v_demos t_demos;
    v_base_person_record r_demo;

    v_person_list varchar#(####);
    v_person_list_count integer;

    v_merge_person_flag integer;
    v_persons_already_merged integer;

    v_hospital_id NUMBER := ##;

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
    FROM APPSUPPORT.TSP###_#_final_pl_split
    ;

    IF v_mpl.COUNT > # THEN 
    
	FOR i in v_mpl.FIRST .. v_mpl.LAST 
	LOOP

	    select distinct /*+ parallel */
		COALESCE(d.first_name, p.first_name) first_name,
		COALESCE(d.last_name, p.last_name) last_name,
		COALESCE(d.date_of_birth, p.date_of_birth) date_of_birth,
		COALESCE(d.ssn, p.ssn) ssn
	    BULK COLLECT INTO v_demos
	    FROM 
	    (
		SELECT
		    mp.person_id,
		    mp.id,
		    mp.table_oid,
		    mp.hospital_id
		FROM APPSUPPORT.TSP###_#_mp_prelim mp
		WHERE mp.person_id IN
		(
		    SELECT PL.COLUMN_VALUE
		    from table(freedom.explode(v_mpl(i).person_id_list)) pl
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

		INSERT INTO appsupport.TSP###_#_merge_log (person_id_list)
		values(v_mpl(i).person_id_list);

		COMMIT;
	
	    ELSE
	    

		INSERT INTO appsupport.TSP###_#_mismatch_log (person_id_list)
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
	from APPSUPPORT.TSP###_#_pl_classify
	WHERE num_persons > #
	AND can_merge = #
	AND has_sp = #
    )
    LOOP

	insert into ehr.manual_person_merge (person_id_list, note, created_by, created_on, approved_by, approved_on)
	values (cur.person_id_list, v_note, v_user_id, systimestamp, v_approving_user_id, systimestamp);

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
    FROM APPSUPPORT.TSP###_#_mp_excepts mpe
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
    FROM APPSUPPORT.TSP###_#_mp_nonexcepts mpne
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



PROCEDURE rehash_redemand AS

    v_person_id NUMBER;
    v_count_me NUMBER;
    v_num_persons_found NUMBER;
    v_lowest_found_person_id NUMBER;
    v_new_person_id NUMBER;
    TYPE r_rec IS RECORD
    (
	id NUMBER,
	TABLE_OID number,
	has_me NUMBER,
	is_demo NUMBER
    );
    TYPE t_recs IS TABLE OF r_rec INDEX BY BINARY_INTEGER;
    v_recs t_recs;

BEGIN


    -- DEL Map Excepts and Rehash:
    SELECT
	id,
	table_oid,
	has_me,
	is_demo
    BULK COLLECT INTO v_recs
    FROM
    (
	select distinct
	    m.id,
	    m.table_oid,
	    case when me.id is null then # else # end has_me,
	    CASE WHEN m.table_oid IN
	    (
		SELECT DISTINCT 
		    d.table_oid
		FROM ehr.demographics d
		WHERE d.hospital_id IN (##)
	    ) THEN #
	    ELSE #
	    END is_demo
	FROM ehr.map m
	LEFT JOIN ehr.person_map pm
	  on pm.hospital_id = m.hospital_id
	  AND pm.table_oid = m.table_oid
	  and m.id = pm.id
	LEFT JOIN ehr.map_exception me
	  on me.hospital_id = m.hospital_id
	  AND me.table_oid = m.table_oid
	  and me.id = m.id
	where m.hospital_id IN (##)
	and pm.person_id IS NULL
    )
    ORDER BY is_demo DESC, has_me DESC, TABLE_OID, ID
    ;


    -- Delete map exceptions and rehash:
    FOR i in v_recs.FIRST .. v_recs.LAST
    LOOP
	
	IF v_recs(i).has_me = # THEN

	    DELETE FROM ehr.map_exception me
	    WHERE me.id = v_recs(i).id
	    and me.table_oid = v_recs(i).table_oid;

	    COMMIT;
    
	END IF;

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
    -- Merge duplicates identified:
    merge_dupe_persons();

    -- Attempt to resolve map excepts and missing person maps across all hospitals associated to merged persons:
    resolve_merge_recs();

    -- Attempt to resolve map excepts and missing person maps for KDMC only
    rehash_redemand();


END;
/
