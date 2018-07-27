DROP TABLE appsupport.tcs####_locations;

CREATE TABLE appsupport.tcs####_locations AS
SELECT DISTINCT
    lm.mapping_value location_code
FROM adt.location_mapping lm
WHERE
lm.feed_id = ###
and upper(trim(both from lm.mapping_value)) IN 
(
    ?,?,?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,
    ?,?,?,'?'
)
;

GRANT ALL ON appsupport.tcs####_locations TO appuser, FREEDOM;





DROP TABLE appsupport.tcs####_disps;

CREATE TABLE appsupport.tcs####_disps AS
SELECT DISTINCT md.drug_dispensation_id
FROM
(
SELECT distinct
dd.drug_dispensation_id,
dd.covered_entity_id
FROM ehr.drug_dispensation dd
LEFT JOIN ehr.LOCATION_CODE lc  
ON lc.covered_entity_system_id = dd.hospital_id 
AND lc.location_id = dd.location_id
LEFT JOIN ehr.LOCATION l
ON l.covered_entity_system_id = dd.hospital_id 
AND l.location_id = dd.location_id
join sentinel.new_invoice_record n 
on n.site_id = (select s.site_id from sentinel.site s where s.hospital_id = ####)
and n.ehr_drug_dispensation_id = dd.drug_dispensation_id
WHERE dd.hospital_id = ####
AND 
(
trunc(dd.dispensed_on) >=  TO_DATE('?')
AND 
trunc(dd.dispensed_on) <=  TO_DATE('?')
)
AND 
(
UPPER(TRIM(BOTH FROM lc.LOCATION_CODE)) IN
(
  ?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,
  ?,?,?,'?'
)
OR
UPPER(TRIM(BOTH FROM l.LOCATION_CODE)) IN
(
  ?,?,?,?,?,?,?,?,?,
  ?,?,?,?,?,?,?,?,?,
  ?,?,?,'?'
)
)
) md
LEFT JOIN FREEDOM.covered_entity ce 
ON ce.covered_entity_id = md.covered_entity_id 
WHERE ce.is_covered = # or ce.is_covered is null
;

GRANT ALL ON appsupport.tcs####_disps TO appuser, FREEDOM;
