spool ./check_persons.txt;

SET LINESIZE ###;
SET PAGESIZE #####;



col person_name format a##
COL CREATED_ON FORMAT A##
COL FIRST_NAME FORMAT A##
COL LAST_NAME FORMAT A##
COL TABLENAME FORMAT A##
COL ID FORMAT A##
COL VALUES FORMAT A##
col acct_num format a##
col mrns format a##
COL DEMOS FORMAT A##
COL MRN FORMAT A##
select distinct 
pm.person_id, 
  CASE WHEN d.id IS NOT NULL THEN 
    d.first_name || '?' || D.DATE_OF_BIRTH
  else
    pci.first_name || '?' || pci.last_name
  END person_name,
  pm.created_on,
    to_char(m#.id) id, m#.table_oid, freedom.tablename(m#.table_oid) tablename, 
    m#.value acct_num,
    m#.value mrn
from ehr.person p
join ehr.person_map pm on pm.person_id = p.person_id
JOIN ehr.hash h
    on h.hospital_id = pm.hospital_id
    and h.table_oid = pm.table_oid
    and h.id = pm.id
join ehr.map m# on m#.id = pm.id and m#.table_oid = pm.table_oid and m#.hospital_id = ####
and m#.pool_id in (select distinct ep#.pool_id from ehr.pool ep# where ep#.pool_id = m#.pool_id and ep#.pool_type_id = #)
join ehr.map m# on m#.id = pm.id and m#.table_oid = pm.table_oid and m#.hospital_id = ####
and m#.pool_id in (select distinct ep#.pool_id from ehr.pool ep# where ep#.pool_id = m#.pool_id and ep#.pool_type_id = #)
left join ehr.demographics d on d.id = pm.id and d.table_oid = pm.table_oid and d.hospital_id = ####
left JOIN ehr.drug_dispensation dd
  on dd.id = pm.id and dd.table_oid = pm.table_oid and dd.hospital_id = ####
left join SOURCE.pharmacy_charges_import pci
  on pci.charges_person_id = pm.id 
  and pm.table_oid = freedom.tableoid('?') 
  and pci.feed_id in (select distinct feed_id from adt.feed where covered_entity_system_id = ####)
  and pci.pharmacy_charges_import_id = dd.src_id
  and dd.src_table_oid = freedom.tableoid('?')
WHERE pm.hospital_id = ####
AND (h.method, h.hash) IN
(
  select distinct 
      h.method,
      h.hash
  from ehr.hash h
  WHERE (h.hospital_id, h.table_oid, h.id) IN
  (
    select distinct 
      m#.hospital_id,
      m#.table_oid,
      m#.id
    from ehr.person p
    join ehr.person_map pm on pm.person_id = p.person_id
    join ehr.map m on m.id = pm.id and m.table_oid = pm.table_oid and m.hospital_id = ####
    left join ehr.demographics d on d.id = pm.id and d.table_oid = pm.table_oid and d.hospital_id = ####
    join ehr.map m# on m#.id = pm.id and m#.table_oid = pm.table_oid and m#.hospital_id = ####
    and m#.pool_id in (select distinct ep#.pool_id from ehr.pool ep# where ep#.pool_id = m#.pool_id and ep#.pool_type_id = #)
    join ehr.map m# on m#.id = pm.id and m#.table_oid = pm.table_oid and m#.hospital_id = ####
    and m#.pool_id in (select distinct ep#.pool_id from ehr.pool ep# where ep#.pool_id = m#.pool_id and ep#.pool_type_id = #)
    where m#.value = ?
  )
)
order by pm.created_on
;




spool off;
