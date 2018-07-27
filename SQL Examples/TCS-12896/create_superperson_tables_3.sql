timing start load_superpersons_#;

DROP TABLE APPSUPPORT.TSP###_#_mrn_superpersons;


CREATE TABLE APPSUPPORT.TSP###_#_mrn_superpersons AS
select distinct
    bp.person_id
from 
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
) bp
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_mrn_superpersons to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE APPSUPPORT.TSP###_#_superperson_recs;


CREATE TABLE APPSUPPORT.TSP###_#_superperson_recs AS
SELECT DISTINCT 
    pm.person_id, 
    pm.id, 
    pm.table_oid, 
    pm.hospital_id,
    CASE WHEN d.id IS NULL THEN #
    ELSE #
    END is_demo
FROM ehr.person p
JOIN ehr.person_map pm
    on pm.hospital_id = ##
    and pm.person_id = p.person_id
left join ehr.demographics d
    on pm.hospital_id = d.hospital_id
    and pm.id = d.id
    and pm.table_oid = d.table_oid
WHERE (p.person_id) IN
(
    SELECT DISTINCT
	person_id
    FROM APPSUPPORT.TSP###_#_mrn_superpersons
)
/*
WHERE 
(
    PM.TABLE_OID IN
    (
	SELECT FREEDOM.TABLEOID('?')
	from DUAL
    )
    OR 
    (((D.FIRST_NAME <> P.FIRST_NAME) OR (D.last_NAME <> P.last_NAME)) and p.date_of_birth <> d.date_of_birth)
)  
*/
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_superperson_recs to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;




DROP TABLE APPSUPPORT.TSP###_#_superperson_hashes;


CREATE TABLE APPSUPPORT.TSP###_#_superperson_hashes AS
select distinct
    h.method,
    h.hash
from ehr.hash h 
WHERE (h.id, h.table_oid, h.hospital_id) IN
(
    SELECT DISTINCT 
	id,
	table_oid,
	hospital_id
    FROM APPSUPPORT.TSP###_#_superperson_recs
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_superperson_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;



DROP TABLE APPSUPPORT.TSP###_#_sp_hash_lookup;


CREATE TABLE APPSUPPORT.TSP###_#_sp_hash_lookup AS
select distinct
    h.*
from ehr.hash h 
WHERE (h.method, h.hash) IN
(
    SELECT DISTINCT 
	method,
	hash
    FROM APPSUPPORT.TSP###_#_superperson_hashes
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_sp_hash_lookup to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;


DROP TABLE APPSUPPORT.TSP###_#_superperson_excepts;


CREATE TABLE APPSUPPORT.TSP###_#_superperson_excepts AS
select distinct
    me.*,
    CASE WHEN d.id IS NULL THEN #
    ELSE #
    END is_demo
FROM ehr.map_exception me
left join ehr.demographics d
  on me.hospital_id = d.hospital_id
  and me.id = d.id
  and me.table_oid = d.table_oid
WHERE (me.id, me.table_oid, me.hospital_id) IN
(
    SELECT DISTINCT
	id,
	table_oid,
	hospital_id
    FROM APPSUPPORT.TSP###_#_sp_hash_lookup
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_superperson_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;

/*
DROP TABLE APPSUPPORT.TSP###_#_superperson_persons;


CREATE TABLE APPSUPPORT.TSP###_#_superperson_persons AS
select distinct
    pm.person_id,
    pm.id,
    pm.table_oid,
    pm.hospital_id
FROM ehr.person_map pm
WHERE (pm.id, pm.table_oid, pm.hospital_id) IN
(
    SELECT DISTINCT
	id,
	table_oid,
	hospital_id
    FROM APPSUPPORT.TSP###_#_sp_hash_lookup
)
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_#_superperson_persons to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;
*/


timing stop;
