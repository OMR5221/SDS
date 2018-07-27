timing start load_MISMATCHES_#;
/*
-- Reference of all map excepts for KDMC:

DROP TABLE APPSUPPORT.TSP###_remain_excepts;


CREATE TABLE APPSUPPORT.TSP###_remain_excepts AS
select distinct 
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.map_exception me
WHERE me.hospital_id = ##
;

-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_remain_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;



/*
-- Get all hashes associated map exceptions:
drop table appsupport.TSP###_mm_persons;

CREATE TABLE appsupport.TSP###_mm_persons AS
SELECT DISTINCT 
    person_id_list,
    TO_NUMBER(p_id) person_id
FROM
(
    SELECT person_id_list, trim(REGEXP_SUBSTR(person_id_list,  '?', #, levels.column_value)) p_id
    FROM
    (   
	SELECT person_id_list
	FROM appsupport.TSP###_mismatch_log
	UNION
	SELECT person_id_list
	FROM appsupport.TSP###_pl_classify
	WHERE can_merge = #
    ),
table(cast(multiset(select level from dual connect by level <= length (regexp_replace(person_id_list, '?')) + #) as sys.OdciNumberList)) levels
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_mm_persons to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;
*/



/*
-- Get all hashes associated map exceptions:
drop table appsupport.TSP###_kdmc_mm_ids;

CREATE TABLE appsupport.TSP###_kdmc_mm_ids AS
SELECT DISTINCT 
    pm.id,
    pm.table_oid
FROM ehr.person_map pm
where pm.hospital_id = ##
and (pm.person_id) in
(
    SELECT DISTINCT
	person_id
    FROM APPSUPPORT.TSP###_mm_persons
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_kdmc_mm_ids to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


-- Get all hashes associated map exceptions:
drop table appsupport.TSP###_kdmc_mm_hashes;

CREATE TABLE appsupport.TSP###_kdmc_mm_hashes AS
SELECT DISTINCT 
    h.method,
    h.hash
FROM ehr.hash h
where h.hospital_id = ##
and (h.table_oid, h.id) in
(
    SELECT DISTINCT
	table_oid,
	id
    FROM APPSUPPORT.TSP###_remain_excepts
);


-- grant permissions on this table
grant select, insert, update, delete on appsupport.TSP###_kdmc_mm_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;





-- Get all hashes associated map exceptions:
drop table appsupport.TSP###_mm_hash_lookup;

CREATE TABLE appsupport.TSP###_mm_hash_lookup AS
SELECT DISTINCT 
    h.hospital_id,
    h.table_oid,
    h.id,
    h.method,
    h.hash
FROM ehr.hash h
where (h.method, h.hash) in
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TSP###_kdmc_mm_hashes
)
-- No charge data:
and h.table_oid NOT IN
(
  -- CHARGE TABLES
  select distinct
    table_oid
  from  sentinel.pdap_dispensation_tables 
  union
  select freedom.tableoid('?')
  from dual
  union
  select freedom.tableoid('?')
  from dual
)
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_mm_hash_lookup to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



drop table appsupport.TSP###_mm_all_persons;

CREATE TABLE appsupport.TSP###_mm_all_persons AS
SELECT DISTINCT 
    pm.hospital_id,
    pm.person_id,
    pm.table_oid,
    pm.id
FROM ehr.person_map pm
where (pm.hospital_id, pm.table_oid, pm.id) in
(
    SELECT DISTINCT
	hospital_id,
	table_oid,
	id
    FROM APPSUPPORT.TSP###_mm_hash_lookup
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_mm_all_persons to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




drop table appsupport.TSP###_mm_all_maps;
commit;

CREATE TABLE appsupport.TSP###_mm_all_maps AS
SELECT DISTINCT 
    m.hospital_id,
    m.table_oid,
    m.id,
    ep.pool_type_id,
    --m.pool_id,
    m.value
FROM ehr.map m
JOIN ehr.pool ep
    on ep.pool_id = m.pool_id
where (m.hospital_id, m.table_oid, m.id) in
(
    SELECT
	hospital_id,
	table_oid,
	id
    FROM APPSUPPORT.TSP###_mm_hash_lookup
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_mm_all_maps to freedom, ehr, adt, sentinel, appuser, appsupport;





drop table appsupport.TSP###_bad_client_data;

CREATE TABLE appsupport.TSP###_bad_client_data AS
select distinct
    hl.method,
    hl.hash,
    m.pool_type_id,
    m.value,
    pm.person_id,
    p.first_name p_fname,
    p.last_name p_lname,
    p.date_of_birth p_dob,
    d.hospital_id,
    d.table_oid,
    d.first_name d_fname,
    d.last_name d_lname,
    d.date_of_birth d_dob
FROM appsupport.TSP###_mm_hash_lookup hl
join appsupport.TSP###_mm_all_maps m
  on m.hospital_id = hl.hospital_id
  and m.table_oid = hl.table_oid
  and m.id = hl.id
join appsupport.TSP###_mm_all_persons pm
  on pm.hospital_id = hl.hospital_id
  and pm.table_oid = hl.table_oid
  and pm.id = hl.id
JOIN ehr.person p
    on p.person_id = pm.person_id
JOIN ehr.demographics d
  on d.hospital_id = hl.hospital_id
  and d.table_oid = hl.table_oid
  and d.id = hl.id
WHERE m.pool_type_id = #
--GROUP BY
--hl.method,
--hl.hash,
--m.POOL_TYPE_ID,
--m.VALUE
----HAVING ( (COUNT(DISTINCT d.first_name) > # or COUNT(DISTINCT d.last_name) > #) AND COUNT(DISTINCT d.date_of_birth) > # )
;


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_bad_client_data to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;
*/


drop table appsupport.TSP###_missing_client_data;

CREATE TABLE appsupport.TSP###_missing_client_data AS
SELECT distinct
(ec.patient_first_name || '?' || ec.patient_last_name) patient_name,
(ec.subscriber_first_name || '?' || ec.subscriber_last_name) subscriber_name,
ec.account_number,
ec.medical_record_number mrn,
-- ec.edi_###_claim_id,
case when KC.ACCOUNT_NUMBER IS NULL THEN # ELSE # END has_new_val,
case when ul.claim_reference_number IS NULL THEN # ELSE # END has_ucrn
FROM source.edi_###_claim ec
LEFT JOIN appsupport.kdmc_edi_###_claim kc
  on EC.ACCOUNT_NUMBER = substr(KC.ACCOUNT_NUMBER,#,LENGTH(ec.account_number))
LEFT JOIN source.ucrn_list ul
    on ul.feed_id = ##
    and EC.ACCOUNT_NUMBER = ul.claim_reference_number
WHERE ec.feed_id = ##
and UPPER(regexp_replace(ec.imported_from,'?'
-- and kc.account_number IS NULL
AND (ec.edi_###_claim_id)  IN
(
    select distinct
	id
    from APPSUPPORT.TSP###_mm_all_maps
    where table_oid = freedom.tableoid('?')
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TSP###_missing_client_data to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


timing stop;
