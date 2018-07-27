SET SERVEROUTPUT OFF;

-- Reference of all map excepts for KDMC:

DROP TABLE APPSUPPORT.TCS#####_excepts;


CREATE TABLE APPSUPPORT.TCS#####_excepts AS
select distinct 
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.map_exception me
WHERE me.hospital_id IN (##)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_excepts to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;





-- Get all hashes associated map exceptions:
drop table appsupport.TCS#####_except_hashes;

CREATE TABLE appsupport.TCS#####_except_hashes AS
SELECT DISTINCT 
    h.method,
    h.hash
FROM ehr.hash h
WHERE (h.hospital_id, h.table_oid, h.id) IN
(
    SELECT DISTINCT
	hospital_id,
	table_oid,
	id
     FROM APPSUPPORT.TCS#####_excepts
)
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_except_hashes to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;

COMMIT;



-- Perform lookup by exception hashes to get all related id/tableoids which are also map exceptions:
drop table appsupport.TCS#####_hash_lookup;

CREATE TABLE appsupport.TCS#####_hash_lookup AS
SELECT DISTINCT 
    h#.method,
    h#.hash,
    h#.hospital_id,
    h#.table_oid,
    h#.id
FROM ehr.hash h#
WHERE (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS#####_except_hashes
)
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_hash_lookup to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;

COMMIT;


drop table appsupport.TCS#####_hl_excepts;

CREATE TABLE appsupport.TCS#####_hl_excepts AS
SELECT DISTINCT 
    hl.method,
    hl.hash,
    me.hospital_id,
    me.table_oid,
    me.id
FROM appsupport.TCS#####_hash_lookup hl
JOIN ehr.map_exception me
    ON me.hospital_id = hl.hospital_id
    AND me.table_oid = hl.table_oid
    and me.id = hl.id
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_hl_excepts to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;

COMMIT;




-- Perform lookup by exception hashes to get all related id/tableoids which are not map exceptions but are mssing a person_map:
drop table appsupport.TCS#####_hl_nonexcepts;

CREATE TABLE appsupport.TCS#####_hl_nonexcepts AS
SELECT DISTINCT 
    hl.method,
    hl.hash,
    hl.hospital_id,
    hl.table_oid,
    hl.id
FROM appsupport.TCS#####_hash_lookup hl
LEFT JOIN ehr.map_exception me
    ON me.hospital_id = hl.hospital_id
    AND me.table_oid = hl.table_oid
    and me.id = hl.id
LEFT JOIN ehr.person_map pm
    ON pm.hospital_id = hl.hospital_id
    AND pm.table_oid = hl.table_oid
    and pm.id = hl.id
WHERE (me.id IS NULL and pm.id IS NULL)
;


grant select, insert, update, delete on APPSUPPORT.TCS#####_hl_nonexcepts to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;





-- Perform lookup by exception hashes to get all related id/tableoids which have a person map:
DROP TABLE APPSUPPORT.TCS#####_hl_persons;
COMMIT;

CREATE TABLE APPSUPPORT.TCS#####_hl_persons AS
select distinct 
    h#.method,
    h#.hash,
    pm.person_id,
    pm.hospital_id,
    pm.table_oid,
    pm.id
FROM appsupport.TCS#####_hash_lookup h#
JOIN ehr.person_map pm
    ON pm.hospital_id = h#.hospital_id
    and pm.table_oid = h#.table_oid
    and pm.id = h#.id
;

grant select, insert, update, delete on APPSUPPORT.TCS#####_hl_persons to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;


-- Find all remaining superpersons for KDMC:
DROP TABLE APPSUPPORT.TCS#####_sp_log_#;


CREATE TABLE APPSUPPORT.TCS#####_sp_log_# AS
select distinct
    sp.person_id
from
( 
    select DISTINCT
	person_id
    from 
    (
	SELECT p.person_id
	FROM ehr.person_map pm
	-- APPSUPPORT.TCS#####_hl_persons ep
	JOIN ehr.person p 
	    ON p.person_id = pm.person_id
	JOIN ehr.demographics d
	    ON d.hospital_id = pm.hospital_id
	    and d.table_oid = pm.table_oid
	    and d.id = pm.id
	WHERE pm.hospital_id IN (##)
	AND
	(
	    (upper(trim(both from p.first_name)) <> upper(trim(both from d.first_name)) 
	    AND
	    upper(trim(both from p.last_name)) <> upper(trim(both from d.last_name)))
	    AND 
	    (p.date_of_birth <> d.date_of_birth)
	)
    )
) sp
;

grant select, insert, update, delete on APPSUPPORT.TCS#####_sp_log_# to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;

COMMIT;


drop table appsupport.TCS#####_person_list;

create table appsupport.TCS#####_person_list AS
SELECT DISTINCT
  method,
  hash,
  COUNT(DISTINCT person_id) num_person_ids,
  SUM(is_sp) sp_sum,
  freedom.implode(DISTINCT person_id) person_id_list
from
(
  select distinct
      ep.method,
      ep.hash,
      ep.person_id,
      CASE WHEN sp.person_id IS NULL THEN # ELSE # END is_sp
  FROM APPSUPPORT.TCS#####_hl_persons ep
  LEFT JOIN appsupport.TCS#####_sp_log_# sp
      ON sp.person_id = ep.person_id
)
GROUP BY method, hash
;

GRANT SELECT on appsupport.TCS#####_person_list to appread, freedom;
GRANT SELECT, INSERT, DELETE, UPDATE ON appsupport.TCS#####_person_list to appuser, freedom
;




-- Using person maps found associated to the map exxeptions by a hash lookup
-- #. Remove all of thoser that are superpersons
-- #. Create listing of person ids with the same hash to consider for merge

DROP TABLE APPSUPPORT.TCS#####_person_list_split;
COMMIT;



CREATE TABLE APPSUPPORT.TCS#####_person_list_split AS
SELECT DISTINCT
    pl.person_id_list,
    ep.person_id,
    pl.sp_sum
FROM APPSUPPORT.TCS#####_person_list pl
JOIN appsupport.TCS#####_hl_persons ep
    on ep.method = pl.method
    and ep.hash = pl.hash
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_person_list_split to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;


/*
    #. Get all demographic ids for the persons associated to map exceptions:
*/

DROP TABLE APPSUPPORT.TCS#####_pl_ids;
COMMIT;

CREATE TABLE APPSUPPORT.TCS#####_pl_ids AS
SELECT
    pl.person_id_list,
    pl.sp_sum,
    pm.person_id,
    pm.id,
    pm.table_oid,
    pm.hospital_id
FROM ehr.person_map pm
JOIN APPSUPPORT.TCS#####_person_list_split pl
    on pm.person_id = pl.person_id
WHERE pm.person_id IN
(
    SELECT DISTINCT 
	person_id
    FROM APPSUPPORT.TCS#####_hl_persons ep
)
AND pm.table_oid IN
(
    SELECT DISTINCT
	d.table_oid
    FROM ehr.demographics d
    WHERE d.hospital_id = pm.hospital_id
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_pl_ids to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;


/*
    #. Processes a person id with duplicate person_id_lists to create new person_id_lists which combine lists
*/

DROP TABLE APPSUPPORT.TCS#####_new_person_list;
commit;

CREATE TABLE APPSUPPORT.TCS#####_new_person_list AS
SELECT DISTINCT 
    npl.person_id_list,
    npl.person_count
FROM
(
  select distinct
    mpl.person_id,
    count(distinct pl#.person_id) person_count,
    freedom.implode(distinct pl#.person_id) person_id_list
  FROM
  (
    SELECT DISTINCT
      pl.person_id ,
      COUNT(DISTINCT Pl.Person_Id_List) NUM_PL 
    FROM APPSUPPORT.TCS#####_person_list_split pl
    where pl.person_id_list is not null
    GROUP BY
      pl.person_id
  ) mpl
  join APPSUPPORT.TCS#####_person_list_split pl#
    on mpl.person_id = pl#.person_id
  join APPSUPPORT.TCS#####_person_list_split pl#
    on pl#.person_id_list = pl#.person_id_list
  GROUP BY
    mpl.person_id
) npl
;

-- grant permissions on this table:
grant select, insert, update, delete on APPSUPPORT.TCS#####_new_person_list to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;



/*
    #. Split each new person_id_list to regain the separate person_id column
*/

DROP TABLE APPSUPPORT.TCS#####_npl_split;
commit;


CREATE TABLE APPSUPPORT.TCS#####_npl_split AS
SELECT DISTINCT
    npl.person_id_list,
    npl.person_count,
    trim(REGEXP_SUBSTR(npl.person_id_list,  '?', #, levels.column_value)) person_id
FROM APPSUPPORT.TCS#####_new_person_list npl,
table(cast(multiset(select level from dual connect by level <= length (regexp_replace(npl.person_id_list, '?')) + #) as sys.OdciNumberList)) levels
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_npl_split to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;




/*

    #.	Removes duplicates from the new_perosn_id_list table by running a DENSE_RANK to keep the longest 	
	person_id_list per person_id
    #. 	Performs lookup to the main person_id_list tables (TCS#####_person_list_split/TSP_person_list_classify)
	which comnatins all lists and swaps these person id lists for the new lists if they are present in the 	
	new person_id_list table (only contains person ids that had duplicate lists which is why a left join is
	being performed)
*/


DROP TABLE APPSUPPORT.TCS#####_select_person_list;
commit;


CREATE TABLE APPSUPPORT.TCS#####_select_person_list AS
SELECT DISTINCT
  OPL.PERSON_ID,
  freedom.implode(distinct npl#.person_id) person_id_list
FROM
(
  SELECT DISTINCT
      pl.person_id,
      pl.person_id_list
  FROM APPSUPPORT.TCS#####_person_list_split pl
) opl
JOIN  APPSUPPORT.TCS#####_npl_split npl#
  ON OPL.person_id_list = npl#.person_id_list
JOIN  APPSUPPORT.TCS#####_npl_split npl#
  ON NPL#.person_id = npl#.person_id
JOIN  APPSUPPORT.TCS#####_npl_split npl#
  ON NPL#.person_id_list = npl#.person_id_list
group by
  OPL.PERSON_ID
ORDER BY
  OPL.PERSON_ID
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_select_person_list to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;



/*
    #. Final table used to get only disitnct person_id_lists and is used for processing in the next script
*/

DROP TABLE APPSUPPORT.TCS#####_final_pl_split;
commit;


CREATE TABLE APPSUPPORT.TCS#####_final_pl_split AS
SELECT DISTINCT
  person_id_list,
  trim(REGEXP_SUBSTR(person_id_list,  '?', #, levels.column_value)) person_id
FROM APPSUPPORT.TCS#####_select_person_list,
table(cast(multiset(select level from dual connect by level <= length (regexp_replace(person_id_list, '?')) + #) as sys.OdciNumberList)) levels
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_final_pl_split to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;
COMMIT;





/*
    #. Preliminary determination of if person_id_list should be merged based on the count of hashes 
    associated to method # of the ehr.autohash() function (SSN-DOB)

    #. Those person_id_lists with only a single hash for method one are considered can_merge = #
*/

DROP TABLE APPSUPPORT.TCS#####_pl_classify;
COMMIT;


CREATE TABLE APPSUPPORT.TCS#####_pl_classify AS
SELECT DISTINCT 
    person_id_list,
    COUNT(DISTINCT person_id) num_persons,
    CASE WHEN SUM(is_sp) > # THEN # ELSE # END has_sp,
    CASE 
    WHEN COUNT(DISTINCT prelim_merge) = # THEN
	CASE WHEN SUM(prelim_merge) >= # THEN #
	ELSE #
	END
    WHEN COUNT(DISTINCT prelim_merge) > # THEN
	CASE 
	WHEN COUNT(DISTINCT d_ssn) <= # THEN #
	ELSE #
	END
    ELSE #     
    END can_merge
FROM
(
    SELECT DISTINCT
	person_id_list,
	person_id,
	d_ssn,
	CASE 
	    WHEN ((p_fname = d_fname OR p_lname = d_lname)
            AND ((p_dob = d_dob) OR (matching_ssn = #))) THEN #
	    WHEN ((p_fname = d_fname AND p_lname = d_lname)) THEN #
	    ELSE #
	END prelim_merge,
	CASE WHEN SUM(sp_sum) > # THEN # ELSE # END is_sp
    FROM
    (
	SELECT
	    person_id_list,
	    person_id,
	    sp_sum,
	    p_fname,
	    p_lname,
	    p_dob,
	    d_fname,
	    d_lname,
	    d_dob,
	    p_ssn
	    d_ssn,
	    CASE WHEN d_ssn = p_ssn THEN #
	    ELSE #
	    END matching_ssn
	FROM
	(
	  SELECT
	    fpl.person_id_list,
	    fpl.person_id,
	    sp_sum,
	    UPPER(REGEXP_REPLACE(trim(both from p.first_name), '?' , null)) p_fname,
	    UPPER(REGEXP_REPLACE(trim(both from p.last_name), '?' , null)) p_lname,
	    p.date_of_birth p_dob,
	    p.ssn p_ssn,
	    UPPER(REGEXP_REPLACE(trim(both from d.first_name), '?' , null)) d_fname,
	    UPPER(REGEXP_REPLACE(trim(both from d.last_name), '?' , null)) d_lname,
	    d.date_of_birth d_dob,
	    d.ssn d_ssn
	  FROM APPSUPPORT.TCS#####_final_pl_split fpl
	  JOIN ehr.person p
	      on p.person_id = fpl.person_id
	  JOIN APPSUPPORT.TCS#####_pl_ids pm
	      on pm.person_id = fpl.person_id
	  JOIN ehr.demographics d
	    ON d.hospital_id = pm.hospital_id
	    AND d.table_oid = pm.table_oid
	    AND d.id = pm.id
	)
	GROUP BY
	    person_id_list,
	    person_id,
	    sp_sum,
	    p_fname,
	    p_lname,
	    p_dob,
	    d_fname,
	    d_lname,
	    d_dob,
	    p_ssn,
	    d_ssn,
	    CASE WHEN d_ssn = p_ssn THEN #
	    ELSE #
	    END 
    )
    GROUP BY
	person_id_list,
	person_id,
	d_ssn,
	CASE 
	WHEN ((p_fname = d_fname OR p_lname = d_lname)
              AND ((p_dob = d_dob) OR (matching_ssn = #)))
          THEN #
	WHEN ((p_fname = d_fname AND p_lname = d_lname))
          THEN #
        ELSE #
	END
)
GROUP BY
person_id_list
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_pl_classify to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;



/*
    #. Get all id/tableoids associated to potential merges:
*/

DROP TABLE APPSUPPORT.TCS#####_mp_prelim;
commit;

CREATE TABLE APPSUPPORT.TCS#####_mp_prelim AS
SELECT DISTINCT
    pm.person_id,
    PM.ID,
    PM.TABLE_OID,
    pm.hospital_id
FROM APPSUPPORT.TCS#####_pl_ids pm
WHERE (pm.person_id) IN
(
    SELECT DISTINCT 
	pl.person_id
    FROM appsupport.TCS#####_pl_classify mp
    JOIN APPSUPPORT.TCS#####_final_pl_split pl
	on pl.person_id_list = mp.person_id_list
    where mp.person_id_list is not null
    and mp.can_merge = #
    and mp.has_sp = #
    and num_persons >= #
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_mp_prelim to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;


/*
    #. Get hashes associated to the potential merges id/tableoids
*/

DROP TABLE APPSUPPORT.TCS#####_mp_hashes;
commit;

CREATE TABLE APPSUPPORT.TCS#####_mp_hashes AS
SELECT DISTINCT
    pl.person_id,
    h#.method,
    h#.hash
FROM APPSUPPORT.TCS#####_mp_prelim pl
JOIN 
(
    SELECT 
	method, 
	hash,
	hospital_id,
	table_oid,
	id
    FROM ehr.hash
    WHERE (hospital_id, table_oid, id) IN
    (
	SELECT DISTINCT
	    hospital_id,
	    table_oid,
	    id
	FROM APPSUPPORT.TCS#####_mp_prelim
    )
) h#
    on h#.hospital_id = pl.hospital_id
    and h#.table_oid = pl.table_oid
    and h#.id = pl.id
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_mp_hashes to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;


/* 
    #. Perform a hash lookup to get all related  records for the merge id/tableoids
    that are associated to a map exception
*/


DROP TABLE APPSUPPORT.TCS#####_mp_excepts;


CREATE TABLE APPSUPPORT.TCS#####_mp_excepts AS
SELECT DISTINCT
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.hash h#
JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
where (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS#####_mp_hashes
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_mp_excepts to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;



/*
    #. Perform a hash lookup to get all related  records for the merge id/tableoids
    that are not associated to a map exception, but still do not have a person map
*/


DROP TABLE APPSUPPORT.TCS#####_mp_nonexcepts;


CREATE TABLE APPSUPPORT.TCS#####_mp_nonexcepts AS
SELECT DISTINCT
    h#.id,
    h#.table_oid,
    h#.hospital_id
FROM ehr.hash h#
LEFT JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
LEFT JOIN ehr.person_map pm
  ON pm.hospital_id = h#.hospital_id
  and pm.table_oid = h#.table_oid
  and pm.id = h#.id
where (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS#####_mp_hashes
)
and (me.id IS NULL AND pm.person_id IS NULL)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS#####_mp_nonexcepts to freedom, ehr, adt, sentinel, appuser, appsupport, release_ddl_user;

COMMIT;

