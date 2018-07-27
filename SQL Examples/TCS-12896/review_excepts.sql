spool ./except_review.txt;

set linesize ###;
set pagesize #####;

col num_me format a##
col persons format a##
col id_vals format a##
COL NUM_PERSONS FORMAT A##
SELECT DISTINCT
  TO_CHAR(COUNT(DISTINCT e.ID)) NUM_ME,
  to_char(substr(freedom.implode_nl_large(distinct pl.pool_type_id || '?'|| m.value),#,####)) id_vals,
  TO_CHAR(COUNT(DISTINCT P.PERSON_ID)) NUM_PERSONS,
  to_char(substr(freedom.implode_nl_large(distinct ep.HOSPITAL_ID || '?' || p.last_name),#,####)) persons
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
-- WHERE e.hospital_id = ##
GROUP BY 
ep.method, ep.hash
order by 
COUNT(DISTINCT e.ID) desc
;




spool off;
