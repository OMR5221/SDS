SET SERVEROUTPUT OFF;

timing start log_DISP_TABLES;

-- Remaining superpersons:

DROP TABLE APPSUPPORT.TCS_#####_demo_tables;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_demo_tables AS
select distinct
  table_oid
from  ehr.demographics d
where d.hospital_id = ####
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_demo_tables to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE APPSUPPORT.TCS_#####_disps;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_disps AS
SELECT DISTINCT
    dd.id,
    dd.table_oid,
    dd.drug_dispensation_id,
    DD.CREATED_ON
FROM ehr.drug_dispensation dd
WHERE dd.hospital_id = ####
and NOT EXISTS 
(
  SELECT pm.person_id
  FROM ehr.person_map pm
  WHERE dd.hospital_id = pm.hospital_id 
  AND dd.id = pm.id
  and dd.table_oid = pm.table_oid
)
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_disps to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


DROP TABLE APPSUPPORT.TCS_#####_disp_hashes;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_disp_hashes AS
SELECT DISTINCT
  h.method,
  h.hash,
  h.id,
  h.table_oid
FROM ehr.hash h
WHERE h.hospital_id = ####
AND (h.table_oid, h.id) IN
(
    SELECT DISTINCT
	table_oid,
	id
    FROM APPSUPPORT.TCS_#####_disps 
)
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_disp_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE APPSUPPORT.TCS_#####_dh_join;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_dh_join AS
SELECT DISTINCT
    d.drug_dispensation_id,
    d.table_oid,
    d.id,
    d.created_on,
    dh.method,
    dh.hash
FROM APPSUPPORT.TCS_#####_disps d
JOIN APPSUPPORT.TCS_#####_disp_hashes dh
    ON dh.table_oid = d.table_oid
    AND dh.id = d.id
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_dh_join to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE APPSUPPORT.TCS_#####_demo_hashes;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_demo_hashes AS
SELECT DISTINCT
  h.method,
  h.hash,
  h.table_oid,
  SUM(case when me.id is null then # else # end) has_me
FROM ehr.hash h
LEFT JOIN ehr.map_exception me
  on me.hospital_id = h.hospital_id
  and me.table_oid = h.table_oid
  and me.id = h.id
WHERE h.hospital_id = ####
AND (h.method, h.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS_#####_disp_hashes
)
and h.table_oid IN
(
    SELECT DISTINCT
	table_oid
    FROM APPSUPPORT.TCS_#####_demo_tables
)
GROUP BY
  h.method,
  h.hash,
  h.table_oid
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_demo_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



timing stop;
