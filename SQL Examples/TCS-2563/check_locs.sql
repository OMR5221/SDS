spool ./check_locs.txt;


set serveroutput on;
set linesize ###;
set pagesize ####;

col mapping_value format a##
col ce_id format a##
col pharm_id format a#
COL start_time format a##;
col end_time format a##;
SELECT DISTINCT  
m.mapping_value, 
TO_CHAR(r.covered_entity_id) ce_id, ce.is_covered, 
t.start_time, t.end_time
-- r.pharmacy_id pharm_id, r.location_mapping_type_id, r.hardcoded_patient_status, r.is_in_four_walls, r.is_non_sentinel_type,
-- MIN(t.start_time) KEEP (DENSE_RANK FIRST ORDER BY t.start_time asc NULLS FIRST) MIN_START_TIME, 
-- MAX(t.end_time) KEEP (DENSE_RANK LAST ORDER BY t.end_time desc NULLS LAST) MAX_END_TIME
FROM adt.location_mapping_timeframe t
left join adt.location_mapping_range r
  on t.feed_id = r.feed_id
  and t.LOCATION_MAPPING_TIMEFRAME_ID = r.LOCATION_MAPPING_TIMEFRAME_ID
left join adt.location_mapping m
    on t.feed_id = m.feed_id
    and m.LOCATION_MAPPING_ID = r.LOCATION_MAPPING_ID
LEFT JOIN freedom.covered_entity ce
  on ce.covered_entity_id = r.covered_entity_id
where t.feed_id = ###
AND upper(M.MAPPING_VALUE) IN
(
      ?,?,?,?,?,?,?,?,?,
      ?,?,?,?,?,?,?,?,?,
      ?,?,?,'?'
)
ORDER BY m.mapping_value, start_time nulls first
;

/*
GROUP BY
m.mapping_value, 
r.covered_entity_id, ce.is_covered
-- r.pharmacy_id, r.location_mapping_type_id, r.hardcoded_patient_status, r.is_in_four_walls, r.is_non_sentinel_type
order by m.mapping_value, MIN(t.start_time) KEEP (DENSE_RANK FIRST ORDER BY t.start_time asc NULLS FIRST) NULLS FIRST
;
*/


spool off;
