timing start load_person_classify_#;

-- Reference of all map excepts for KDMC:

DROP TABLE APPSUPPORT.TSP###_#_excepts;


CREATE TABLE APPSUPPORT.TSP###_#_excepts AS
select distinct 
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.map_exception me
WHERE me.hospital_id = ##
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;





-- Get all hashes associated map exceptions:
drop table appsupport.TSP###_#_except_hashes;

CREATE TABLE appsupport.TSP###_#_except_hashes AS
SELECT DISTINCT 
    h.method,
    h.hash
FROM appsupport.TSP###_#_excepts ex
JOIN ehr.hash h
    ON h.hospital_id = ##
    AND H.TABLE_OID = ex.TABLE_OID
    AND h.id = ex.id
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_except_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



-- Perform lookup by exception hashes to get all related id/tableoids which are also map exceptions:
drop table appsupport.TSP###_#_except_lookup;

CREATE TABLE appsupport.TSP###_#_except_lookup AS
SELECT DISTINCT 
    h#.method,
    h#.hash,
    me.hospital_id,
    me.table_oid,
    me.id
FROM
(
    SELECT
	method,
	hash
    FROM appsupport.TSP###_#_except_hashes
)h#
JOIN ehr.hash h#
    on h#.method = h#.method
    and h#.hash = h#.hash
JOIN ehr.map_exception me
    ON me.hospital_id = h#.hospital_id
    AND me.table_oid = h#.table_oid
    and me.id = h#.id
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_except_lookup to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




-- Perform lookup by exception hashes to get all related id/tableoids which are not map exceptions but are mssing a person_map:
drop table appsupport.TSP###_#_non_except_lookup;

CREATE TABLE appsupport.TSP###_#_non_except_lookup AS
SELECT DISTINCT 
    h#.method,
    h#.hash,
    h#.hospital_id,
    h#.table_oid,
    h#.id
FROM
(
    SELECT
	method,
	hash
    FROM appsupport.TSP###_#_except_hashes
)h#
JOIN ehr.hash h#
    on h#.method = h#.method
    and h#.hash = h#.hash
LEFT JOIN ehr.map_exception me
    ON me.hospital_id = h#.hospital_id
    AND me.table_oid = h#.table_oid
    and me.id = h#.id
LEFT JOIN ehr.person_map pm
    ON pm.hospital_id = h#.hospital_id
    AND pm.table_oid = h#.table_oid
    and pm.id = h#.id
WHERE me.id IS NULL and pm.id IS NULL
;


grant select, insert, update, delete on APPSUPPORT.TSP###_#_non_except_lookup to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;





-- Perform lookup by exception hashes to get all related id/tableoids which have a person map:
DROP TABLE APPSUPPORT.TSP###_#_except_persons;


CREATE TABLE APPSUPPORT.TSP###_#_except_persons AS
select distinct 
    h#.method,
    h#.hash,
    pm.person_id,
    pm.hospital_id,
    pm.table_oid,
    pm.id
FROM ehr.hash h#
JOIN ehr.person_map pm
    ON pm.hospital_id = h#.hospital_id
    and pm.table_oid = h#.table_oid
    and pm.id = h#.id
WHERE (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM appsupport.TSP###_#_except_lookup
)
;

grant select, insert, update, delete on APPSUPPORT.TSP###_#_except_persons to freedom, ehr, adt, sentinel, appuser, appsupport;



-- Find all remaining superpersons for KDMC:
DROP TABLE APPSUPPORT.TSP###_#_superperson_log;


CREATE TABLE APPSUPPORT.TSP###_#_superperson_log AS
select distinct
    sp.person_id
from
( 
    SELECT DISTINCT
	sp#.person_id
    FROM
    (
      with mrns as 
      (
	select m.value, m.id, m.table_oid , m.hospital_id
	from ehr.map m 
	join ehr.pool ep
	  on ep.pool_id = m.pool_id
	where m.hospital_id = ## --kdmc
	and ep.pool_type_id = # --mrn
	and m.table_oid = freedom.tableoid('?')
      )
      select distinct 
	count(distinct m.value) mrns, 
	 pm.person_id
      from mrns m 
      join ehr.person_map pm 
	on pm.id = m.id 
	and pm.table_oid = m.table_oid 
	and pm.hospital_id = m.hospital_id
      group by pm.person_id
      having count(distinct value) > #
    ) sp#
) sp
;

grant select, insert, update, delete on APPSUPPORT.TSP###_#_superperson_log to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


drop table appsupport.TSP###_#_person_list;

create table appsupport.TSP###_#_person_list AS
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
  FROM APPSUPPORT.TSP###_#_except_persons ep
  LEFT JOIN appsupport.TSP###_#_superperson_log sp
      ON sp.person_id = ep.person_id
)
GROUP BY method, hash
;

GRANT SELECT on appsupport.TSP###_#_person_list to appread, freedom;
GRANT SELECT, INSERT, DELETE, UPDATE ON appsupport.TSP###_#_person_list to appuser, freedom
;




-- Using person maps found associated to the map exxeptions by a hash lookup
-- #. Remove all of thoser that are superpersons
-- #. Create listing of person ids with the same hash to consider for merge

DROP TABLE APPSUPPORT.TSP###_#_person_list_split;
COMMIT;



CREATE TABLE APPSUPPORT.TSP###_#_person_list_split AS
SELECT DISTINCT
    pl.person_id_list,
    ep.person_id
FROM APPSUPPORT.TSP###_#_person_list pl
JOIN appsupport.TSP###_#_except_persons ep
    on ep.method = pl.method
    and ep.hash = pl.hash
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_person_list_split to freedom, ehr, adt, sentinel, appuser, appsupport;

/*
    #. Get all demographic ids for the persons associated to map exceptions:
*/

DROP TABLE APPSUPPORT.TSP###_#_pl_ids;
COMMIT;

CREATE TABLE APPSUPPORT.TSP###_#_pl_ids AS
SELECT
    pls.person_id_list,
    pm.person_id,
    pm.id,
    pm.table_oid,
    pm.hospital_id,
    pl.sp_sum
FROM ehr.person_map pm
JOIN APPSUPPORT.TSP###_#_person_list_split pls
    on pm.person_id = pls.person_id
JOIN APPSUPPORT.TSP###_#_person_list pl
    on pl.person_id_list  = pls.person_id_list
WHERE pm.person_id IN
(
    SELECT DISTINCT 
	person_id
    FROM APPSUPPORT.TSP###_#_except_persons ep
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
grant select, insert, update, delete on APPSUPPORT.TSP###_#_pl_ids to freedom, ehr, adt, sentinel, appuser, appsupport;



/*
    #. Preliminary determination of if person_id_list should be merged based on the count of hashes 
    associated to method # of the ehr.autohash() function (SSN-DOB)

    #. Those person_id_lists with only a single hash for method one are considered can_merge = #
*/

DROP TABLE APPSUPPORT.TSP###_#_pl_classify;
COMMIT;


CREATE TABLE APPSUPPORT.TSP###_#_pl_classify AS
SELECT DISTINCT
    p#.person_id_list,
    CASE WHEN sp_sum > # THEN # ELSE # END has_sp,
    CASE WHEN COUNT(DISTINCT h.hash) > # THEN # ELSE # END can_merge
FROM APPSUPPORT.TSP###_#_pl_ids p#
JOIN ehr.hash h
  ON h.hospital_id = p#.hospital_id
  AND h.table_oid = p#.table_oid
  AND h.id = p#.id
WHERE h.method = #
GROUP BY
    p#.person_id_list,
    CASE WHEN sp_sum > # THEN # ELSE # END 
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_pl_classify to freedom, ehr, adt, sentinel, appuser, appsupport;



/*
    #. Processes a person id with duplicate person_id_lists to create new person_id_lists which combine lists
*/

DROP TABLE APPSUPPORT.TSP###_#_new_person_list;
commit;

CREATE TABLE APPSUPPORT.TSP###_#_new_person_list AS
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
    FROM appsupport.TSP###_#_pl_classify mp
    JOIN APPSUPPORT.TSP###_#_person_list_split pl
        on pl.person_id_list = mp.person_id_list
    where mp.person_id_list is not null
    and mp.can_merge = #
    GROUP BY
      pl.person_id
    HAVING COUNT(DISTINCT Pl.Person_Id_List) > #
  ) mpl
  join APPSUPPORT.TSP###_#_person_list_split pl#
    on mpl.person_id = pl#.person_id
  join APPSUPPORT.TSP###_#_person_list_split pl#
    on pl#.person_id_list = pl#.person_id_list
  GROUP BY
    mpl.person_id
) npl
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_new_person_list to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;



/*
    #. Split each new person_id_list to regain the separate person_id column
*/

DROP TABLE APPSUPPORT.TSP###_#_npl_split;
commit;


CREATE TABLE APPSUPPORT.TSP###_#_npl_split AS
SELECT DISTINCT
    npl.person_id_list,
    npl.person_count,
    trim(REGEXP_SUBSTR(npl.person_id_list,  '?', #, levels.column_value)) person_id
FROM APPSUPPORT.TSP###_#_new_person_list npl,
table(cast(multiset(select level from dual connect by level <= length (regexp_replace(npl.person_id_list, '?')) + #) as sys.OdciNumberList)) levels
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_npl_split to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;




/*

    #.	Removes duplicates from the new_perosn_id_list table by running a DENSE_RANK to keep the longest 	
	person_id_list per person_id
    #. 	Performs lookup to the main person_id_list tables (TSP###_#_person_list_split/TSP_person_list_classify)
	which comnatins all lists and swaps these person id lists for the new lists if they are present in the 	
	new person_id_list table (only contains person ids that had duplicate lists which is why a left join is
	being performed)
*/


DROP TABLE APPSUPPORT.TSP###_#_select_person_list;
commit;


CREATE TABLE APPSUPPORT.TSP###_#_select_person_list AS
SELECT DISTINCT
  OPL.PERSON_ID,
  freedom.implode(distinct npl#.person_id) person_id_list
FROM
(
  SELECT DISTINCT
      pl.person_id,
      pl.person_id_list
  FROM APPSUPPORT.TSP###_#_person_list_split pl
) opl
JOIN  APPSUPPORT.TSP###_#_npl_split npl#
  ON OPL.person_id_list = npl#.person_id_list
JOIN  APPSUPPORT.TSP###_#_npl_split npl#
  ON NPL#.person_id = npl#.person_id
JOIN  APPSUPPORT.TSP###_#_npl_split npl#
  ON NPL#.person_id_list = npl#.person_id_list
group by
  OPL.PERSON_ID
ORDER BY
  OPL.PERSON_ID
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_select_person_list to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;



/*
    #. Final table used to get only disitnct person_id_lists and is used for processing in the next script
*/

DROP TABLE APPSUPPORT.TSP###_#_final_person_list;
commit;


CREATE TABLE APPSUPPORT.TSP###_#_final_person_list AS
SELECT DISTINCT
  person_id_list
FROM APPSUPPORT.TSP###_#_select_person_list
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_final_person_list to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;


/*
    #. Get all id/tableoids associated to potential merges:
*/

DROP TABLE APPSUPPORT.TSP###_#_mp_prelim;
commit;

CREATE TABLE APPSUPPORT.TSP###_#_mp_prelim AS
SELECT DISTINCT
    pm.person_id,
    PM.ID,
    PM.TABLE_OID,
    pm.hospital_id
FROM APPSUPPORT.TSP###_#_pl_ids pm
WHERE (pm.person_id) IN
(
    SELECT DISTINCT 
	pl.person_id
    FROM appsupport.TSP###_#_pl_classify mp
    JOIN APPSUPPORT.TSP###_#_person_list_split pl
	on pl.person_id_list = mp.person_id_list
    where mp.person_id_list is not null
    and mp.can_merge = #
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_mp_prelim to freedom, ehr, adt, sentinel, appuser, appsupport;


/*
    #. Get hashes associated to the potential merges id/tableoids
*/

DROP TABLE APPSUPPORT.TSP###_#_mp_hashes;
commit;

CREATE TABLE APPSUPPORT.TSP###_#_mp_hashes AS
SELECT DISTINCT
    pl.person_id,
    h#.method,
    h#.hash
FROM APPSUPPORT.TSP###_#_mp_prelim pl
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
	FROM APPSUPPORT.TSP###_#_mp_prelim
    )
) h#
    on h#.hospital_id = pl.hospital_id
    and h#.table_oid = pl.table_oid
    and h#.id = pl.id
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_mp_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;


/* 
    #. Perform a hash lookup to get all related  records for the merge id/tableoids
    that are associated to a map exception
*/


DROP TABLE APPSUPPORT.TSP###_#_mp_excepts;


CREATE TABLE APPSUPPORT.TSP###_#_mp_excepts AS
SELECT DISTINCT
    me.id,
    me.table_oid,
    me.hospital_id,
    h#.person_id
FROM APPSUPPORT.TSP###_#_mp_hashes h#
JOIN ehr.hash h#
  on h#.method = h#.method
  and h#.hash = h#.hash
JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_mp_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;




/* 
    #. Perform a hash lookup to get all related  records for the merge id/tableoids
    that are not associated to a map exception, but still do not have a person map
*/


DROP TABLE APPSUPPORT.TSP###_#_mp_nonexcepts;


CREATE TABLE APPSUPPORT.TSP###_#_mp_nonexcepts AS
SELECT DISTINCT
    h#.id,
    h#.table_oid,
    h#.hospital_id
FROM APPSUPPORT.TSP###_#_mp_hashes h#
JOIN ehr.hash h#
  on h#.method = h#.method
  and h#.hash = h#.hash
LEFT JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
LEFT JOIN ehr.person_map pm
  ON pm.hospital_id = h#.hospital_id
  and pm.table_oid = h#.table_oid
  and pm.id = h#.id
WHERE me.id IS NULL
AND pm.person_id IS NULL
AND h#.table_oid <> freedom.tableoid('?')
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_mp_nonexcepts to freedom, ehr, adt, sentinel, appuser, appsupport;


/*
    #. Final table to hold the person_id_lists of those that will defineltey be merged:
    #. Populated in the next script (resolve_merge_excepts_#.sql) procedure load_ptdb_tables()
*/

drop table appsupport.TSP###_#_merge_log;

create table appsupport.TSP###_#_merge_log
(
   "PERSON_ID_LIST" VARCHAR#(###)
);

GRANT SELECT on appsupport.TSP###_#_merge_log to appread, freedom;
GRANT SELECT, INSERT, DELETE, UPDATE ON appsupport.TSP###_#_merge_log to appuser, freedom
;

COMMIT;


/*
    #. Table to hold all person_id_lists that will not be merged
*/

drop table appsupport.TSP###_#_mismatch_log;

create table appsupport.TSP###_#_mismatch_log
(
   "PERSON_ID_LIST" VARCHAR#(###)
);

GRANT SELECT on appsupport.TSP###_#_mismatch_log to appread, freedom;
GRANT SELECT, INSERT, DELETE, UPDATE ON appsupport.TSP###_#_mismatch_log to appuser, freedom
;

COMMIT;


timing stop;
