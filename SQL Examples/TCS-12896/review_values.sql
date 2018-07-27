spool ./review_claim_ref.txt;

set linesize ###;
set pagesize #####;

col num_me format a##
col persons format a##
col id_vals format a##
COL NUM_PERSONS FORMAT A##
SELECT
COUNT(DISTINCT EDI_###_CLAIM_ID) NUM_MAPS
from appsupport.kdmc_edi_###_claim
where account_number IN
(
SELECT DISTINCT
    m.value
FROM appsupport.TSP###_excepts e
JOIN  APPSUPPORT.TSP###_except_persons EP
  ON E.METHOD = EP.method
  and e.hash = ep.hash
  and e.hospital_id = ep.hospital_id
JOIN ehr.person p
  on p.person_id = ep.person_id
JOIN ehr.map m
  ON e.hospital_id = m.hospital_id
  and e.table_oid = m.table_oid
  and e.id = m.id
JOIN ehr.pool pl
  on pl.pool_id = m.pool_id
WHERE e.hospital_id = ##
AND pl.pool_type_id = #
GROUP BY 
ep.method, ep.hash,
pl.pool_type_id,
m.value
HAVING COUNT(DISTINCT ep.person_id) > #
)
;




spool off;
