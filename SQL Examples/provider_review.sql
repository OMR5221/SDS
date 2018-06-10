

SELECT * FROM OPTIREV.CONTRACT WHERE ACTIVE = 1 AND IS_330 = 1 AND START_DATE IS NOT NULL AND covered_entity_id IN (165,166,167,171,12878,12879,12880);



CLEAR screen
COLUMN cid FORMAT A10 HEADING "CLIENT ID"
COLUMN c_name FORMAT A30 HEADING "CLIENT NAME"
COLUMN hid FORMAT A10 HEADING "HOSP ID"
COLUMN h_name FORMAT A30 HEADING "HOSP NAME"
COLUMN ces_id FORMAT A10 HEADING "CE SYS ID"
COLUMN ce_id FORMAT A6 HEADING "CE ID"
COLUMN ctid FORMAT A12 HEADING "CONTRACT ID"
COLUMN ct_name FORMAT A20 HEADING "CONTRACT NAME"
COLUMN ssid FORMAT A8 HEADING "SITE ID"
COLUMN ss_name WORD_WRAPPED FORMAT A20 HEADING "SITE NAME"
COLUMN government_id FORMAT A14 HEADING "GOV ID"
SELECT DISTINCT
    oc.contract_id,
	oc.name ct_name
FROM optirev.contract oc
WHERE oc.is_330 = 1
AND (oc.end_date is null OR oc.termination_date IS NULL)
AND oc.go_live_date IS NOT NULL
ORDER by oc.contract_id ASC
;


-- FIND CE_ID related to the CES in question: ALameda, Thundermist and WellOne
CLEAR screen
COLUMN cid FORMAT A10 HEADING "CLIENT ID"
COLUMN c_name FORMAT A30 HEADING "CLIENT NAME"
COLUMN hid FORMAT A10 HEADING "HOSP ID"
COLUMN h_name FORMAT A30 HEADING "HOSP NAME"
COLUMN ces_id FORMAT A10 HEADING "CE SYS ID"
COLUMN ce_id FORMAT A6 HEADING "CE ID"
COLUMN ctid FORMAT A12 HEADING "CONTRACT ID"
COLUMN ct_name FORMAT A20 HEADING "CONTRACT NAME"
COLUMN ssid FORMAT A8 HEADING "SITE ID"
COLUMN ss_name WORD_WRAPPED FORMAT A20 HEADING "SITE NAME"
COLUMN government_id FORMAT A14 HEADING "GOV ID"
SELECT DISTINCT
    fc.client_id cid, 
    SUBSTR(fc.client_name,1,30) c_name,
    ce.covered_entity_system_id ces_id,
	ce.covered_entity_id,
    ce.hospital_id,
    af.feed_id,
    oc.contract_id,
    oc.is_330
	--oc.name ct_name,*/
FROM freedom.client fc
JOIN freedom.hospital fh
    ON fc.client_id = fh.client_id
JOIN freedom.covered_entity ce
    ON fh.hospital_id = ce.hospital_id
LEFT JOIN adt.feed af
	ON af.covered_entity_system_id = ce.covered_entity_system_id
JOIN optirev.contract oc
    ON ce.covered_entity_id = oc.covered_entity_id
join pharmacy.info i on oc.pharmacy_id = i.pharmacy_id
join pharmacy.lookup l on i.pharmacy_id = l.pharmacy_id      
join pharmacy.identifier_type t on l.identifier_type_id = t.identifier_type_id 
WHERE ce.covered_entity_system_id IN (17, 34, 1121)
ORDER BY ce.covered_entity_id
;


select imported_from, max(imported_on)
from SOURCE.cvs_pharmacy_list
group by imported_from
order by max(imported_on) desc;


-- Check the most recent cvs pharmacy list npi values for matches in the pdb:
select *
from SOURCE.cvs_pharmacy_list
where imported_from = 'cvs_pharmacy_list_0.csv';






-- Check the most recent cvs pharmacy list npi values for matches in the pdb:
SELECT
    p.physician_id, 
    p.last_name, 
    p.first_name, 
    e.EVENT_ID, 
    e.effective_on, 
    e.approved_for, 
    e.effective_on + e.approved_for AS current_expiry, 
    TRUNC(e.effective_on + e.approved_for - SYSDATE) AS number_days_to_expire,
    case when rp.npi is not null 
        then 1 
        else 0
    end as phys_client_approved
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
JOIN SOURCE.cred_provider_import cpi ON cpi.cred_provider_import_id = e.id
JOIN
(
    select npi
    from SOURCE.cvs_pharmacy_list
    where imported_from = 'cvs_pharmacy_list_0.csv'
)rp ON rp.npi = pi.npi
--WHERE e.table_oid = freedom.tableoid('source.cred_provider_import')
ORDER BY p.PHYSICIAN_ID DESC;


-- Check what tables pdb events are loaded in from:
select distinct tl.tablename
from pdb.event pe
join freedom.tableoid_list tl
    ON tl.tableoid = pe.table_oid
ORDER BY TL.TABLENAME;
    
    
-- Check if any of the ce ids in question were imported from the maunl_physician table
select 
    COUNT(*)
from SOURCE.manual_physicians_log
where covered_entity_id IN (165,166,167,171,12878,12879,12880)
ORDER BY npi
;



--- Check the Thundermist Providers match my previous analysis:

select COUNT(distinct mp.manual_physician_id) AS "TM:TOTAL MANUAL PHYS COUNT"
from SOURCE.manual_physicians mp
where mp.hospital_id IN (17);



select COUNT(distinct mp.manual_physician_id) AS "TM:ACTIVE MNL PHYS COUNT"
from SOURCE.manual_physicians mp
where mp.hospital_id IN (17)
AND mp.deactivated_on IS  NULL;

ORDER BY mp.manual_physician_id ASC;


select *
from SOURCE.manual_physicians_log
where covered_entity_id IN (165,166,167)
ORDER BY npi
;



-- Check the total number of providers in the PDB for feed #92:





-- Check the total number of providers in the PDB for feed #92 whose source was the manual physican table:

SELECT
   *
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
JOIN SOURCE.manual_physicians_log mpl ON mpl.manual_physicians_log_id = e.id
WHERE PM.table_oid = freedom.tableoid('source.manual_physicians_log');
AND mpl.hospital_id IN (17);


SELECT distinct ftl.tablename
FROM PDB.physician_map pm 
JOIN FREEDOM.TABLEOID_list ftl
    on ftl.tableoid = pM.TABLE_OID
    order by ftl.tablename asc;
--WHERE M.table_oid = freedom.tableoid('source.manual_physicians');

-- cHECK THUNDERMIST DETAILS
CLEAR screen
COLUMN cid FORMAT A10 HEADING "CLIENT ID"
COLUMN c_name FORMAT A30 HEADING "CLIENT NAME"
COLUMN hid FORMAT A10 HEADING "HOSP ID"
COLUMN h_name FORMAT A30 HEADING "HOSP NAME"
COLUMN ces_id FORMAT A10 HEADING "CE SYS ID"
COLUMN ce_id FORMAT A6 HEADING "CE ID"
COLUMN ctid FORMAT A12 HEADING "CONTRACT ID"
COLUMN feed_name FORMAT A30 HEADING "FEED NAME"
COLUMN government_id FORMAT A14 HEADING "GOV ID"
SELECT DISTINCT
    fc.client_id cid, 
    SUBSTR(fc.client_name,1,30) c_name,
    ce.covered_entity_system_id ces_id,
	ce.covered_entity_id,
    ce.hospital_id,
    oc.contract_id,
    oc.name,
    af.feed_id,
    af.feed_name
FROM freedom.client fc
JOIN freedom.hospital fh
    ON fc.client_id = fh.client_id
JOIN freedom.covered_entity ce
    ON fh.hospital_id = ce.hospital_id
join optirev.contract oc
    on oc.covered_entity_id = ce.covered_entity_id
LEFT JOIN adt.feed af
	ON af.covered_entity_system_id = ce.covered_entity_system_id
WHERE ce.covered_entity_system_id IN (17)
ORDER BY oc.contract_id asc
;




-- cHECK FOR alameda: NONE
COLUMN first_name FORMAT A16
COLUMN last_name FORMAT A16
COLUMN npi_number FORMAT A12
COLUMN state_license_expiration_date FORMAT A12
COLUMN dea_expiration_date FORMAT A12
SELECT NPI_NUMBER, FIRST_NAME, LAST_NAME, state_license_expiration_date, dea_expiration_date
FROM SOURCE.cred_provider_import
WHERE FEED_ID IN (522)
--AND IMPORTED_FROM LIKE '20151005%'
ORDER BY npi_number;


-- cHECK FOR THE THUNDERMIST CRED FILE
COLUMN first_name FORMAT A16
COLUMN last_name FORMAT A16
COLUMN npi_number FORMAT A12
COLUMN state_license_expiration_date FORMAT A12
COLUMN dea_expiration_date FORMAT A12
SELECT NPI_NUMBER, FIRST_NAME, LAST_NAME, state_license_expiration_date, dea_expiration_date
FROM SOURCE.cred_provider_import
WHERE FEED_ID IN (92)
AND IMPORTED_FROM LIKE '20151005%'
ORDER BY npi_number;



-- cHECK THE pdb FOR THUNDERMIST CRED PROVIDERS IN most recent file
SELECT
    p.physician_id, 
    p.last_name, 
    p.first_name, 
    e.EVENT_ID, 
    e.effective_on, 
    e.approved_for, 
    e.effective_on + e.approved_for AS current_expiry, 
    TRUNC(e.effective_on + e.approved_for - SYSDATE) AS number_days_to_expire,
    case when rp.npi_number is not null 
        then 1 
        else 0
    end as phys_client_approved
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
JOIN SOURCE.cred_provider_import cpi ON cpi.cred_provider_import_id = e.id
LEFT JOIN
(
    SELECT NPI_NUMBER
    FROM SOURCE.cred_provider_import
    WHERE FEED_ID IN (92)
    AND IMPORTED_FROM LIKE '20151005%'
)rp ON rp.npi_number = pi.npi
WHERE e.table_oid = freedom.tableoid('source.cred_provider_import')
AND cpi.feed_id IN (92)
ORDER BY p.PHYSICIAN_ID DESC;




-- count: ineligible providers at client level 
SELECT
    COUNT( DISTINCT p.physician_id) AS "INELIG PROV COUNT"
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
JOIN SOURCE.cred_provider_import cpi ON cpi.cred_provider_import_id = e.id
LEFT JOIN
(
    SELECT NPI_NUMBER
    FROM SOURCE.cred_provider_import
    WHERE FEED_ID IN (92)
    AND IMPORTED_FROM LIKE '20151005%'
)rp ON rp.npi_number = pi.npi
WHERE e.table_oid = freedom.tableoid('source.cred_provider_import')
AND cpi.feed_id IN (92)
and rp.npi_number IS NULL
ORDER BY p.PHYSICIAN_ID DESC;



SELECT  mp.covered_entity_id, mp.manual_physician_id, SUBSTR(mpl.last_name || ', ' || mpl.first_name,1,25) Name,
TO_CHAR(e.effective_on, 'YYYY-MM-DD') as "Approved Beginning",
TO_CHAR(e.effective_on + e.approved_for, 'YYYY-MM-DD') as "Approved Through",
TRUNC(mpl.deactivated_on) Deactivated
FROM source.manual_physicians mp
JOIN source.manual_physicians_log mpl ON mp.manual_physician_id = mpl.manual_physician_id
LEFT JOIN pdb.event e ON e.id = mpl.manual_physicians_log_id AND e.table_oid = freedom.tableoid('source.manual_physicians_log')
WHERE mp.covered_entity_id IN (165,166,167,22891)
--AND e.is_approved = 1 
--AND e.event_id IS NOT NULL
--AND mpl.deactivated_on IS NOT NULL
ORDER BY mp.covered_entity_id;







COLUMN first_name FORMAT A16
COLUMN last_name FORMAT A16
COLUMN npi_number FORMAT A12
SELECT -- '[Supporting Data|https://secure.sentryds.com/index.html?id='||e.id||chr(38)||'table_oid='||e.table_oid||chr(38)||'application_id=961]' as "Raw Data Viewer Link",
    e.physician_id,
    e.last_name, 
    e.first_name, 
    ce.display_name || case when ce.government_id is not null then ' ('||ce.government_id||')' else '' end as "Approved at CE",
    to_char(e.effective_on, 'YYYY-MM-DD') as "Approved Beginning",
    to_char(e.effective_on + approved_for, 'YYYY-MM-DD') as "Approved at CE Through "
FROM (
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name,l.covered_entity_system_id,l.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  UNION
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name, ces_map.covered_entity_system_id, ces_map.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  JOIN freedom.covered_entity ces_map ON ces_map.covered_entity_system_id = l.covered_entity_system_id
  UNION
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name, ces.covered_entity_system_id,ce.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  JOIN freedom.covered_entity_system ces ON ces.client_id = l.client_id
  join freedom.covered_entity ce on ce.covered_entity_system_id = ces.covered_entity_system_id
) e
JOIN freedom.covered_entity ce on ce.covered_entity_id = e.covered_entity_id
WHERE physician_id IN
(
    SELECT DISTINCT
        p.physician_id
        /*,
        case when rp.npi_number is not null 
            then 1 
            else 0
        end as phys_approved*/
    FROM PDB.event e
    JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
    JOIN PDB.physician p ON p.physician_id = pm.physician_id 
    JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
    JOIN SOURCE.cred_provider_import cpi ON cpi.cred_provider_import_id = e.id
    LEFT JOIN
    (
        SELECT NPI_NUMBER
        FROM SOURCE.cred_provider_import
        WHERE FEED_ID IN (92)
        AND IMPORTED_FROM LIKE '20151005%'
    )rp ON rp.npi_number = pi.npi
    WHERE e.table_oid = freedom.tableoid('source.cred_provider_import')
    -- not Eligilbe against most recent cred provider file
    AND rp.npi_number IS NULL 
    AND cpi.feed_id IN (92)
)
AND e.is_approved = 1
AND e.effective_on + approved_for >= trunc(SYSDATE)
AND e.covered_entity_system_id IN (17, 34, 1121)
ORDER BY 1, e.effective_on
;


-- COUNT
COLUMN first_name FORMAT A16
COLUMN last_name FORMAT A16
COLUMN npi_number FORMAT A12
SELECT -- '[Supporting Data|https://secure.sentryds.com/index.html?id='||e.id||chr(38)||'table_oid='||e.table_oid||chr(38)||'application_id=961]' as "Raw Data Viewer Link",
    COUNT(DISTINCT e.physician_id) AS "CLIENT INELIG COUNT"
FROM (
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name,l.covered_entity_system_id,l.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  UNION
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name, ces_map.covered_entity_system_id, ces_map.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  JOIN freedom.covered_entity ces_map ON ces_map.covered_entity_system_id = l.covered_entity_system_id
  UNION
  SELECT e.id, e.table_oid, e.is_approved, e.effective_on, e.approved_for, pm.physician_id, p.last_name, p.first_name, ces.covered_entity_system_id,ce.covered_entity_id
  FROM pdb.physician_map pm
  JOIN PDB.physician p ON p.physician_id = pm.physician_id 
  JOIN pdb.physician_location l ON pm.id = l.id AND pm.table_oid = l.table_oid
  JOIN pdb.event e ON l.id = e.id AND l.table_oid = e.table_oid AND l.physician_location_id = e.physician_location_id
  JOIN freedom.covered_entity_system ces ON ces.client_id = l.client_id
  join freedom.covered_entity ce on ce.covered_entity_system_id = ces.covered_entity_system_id
) e
JOIN freedom.covered_entity ce on ce.covered_entity_id = e.covered_entity_id
WHERE physician_id IN
(
    SELECT DISTINCT
        p.physician_id
        /*,
        case when rp.npi_number is not null 
            then 1 
            else 0
        end as phys_approved*/
    FROM PDB.event e
    JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
    JOIN PDB.physician p ON p.physician_id = pm.physician_id 
    JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
    JOIN SOURCE.cred_provider_import cpi ON cpi.cred_provider_import_id = e.id
    LEFT JOIN
    (
        SELECT NPI_NUMBER
        FROM SOURCE.cred_provider_import
        WHERE FEED_ID IN (92)
        AND IMPORTED_FROM LIKE '20151005%'
    )rp ON rp.npi_number = pi.npi
    WHERE e.table_oid = freedom.tableoid('source.cred_provider_import')
    -- not Eligilbe against most recent cred provider file
    AND rp.npi_number IS NULL 
    AND cpi.feed_id IN (92)
)
AND e.is_approved = 1
AND e.effective_on + approved_for >= trunc(SYSDATE)
AND e.covered_entity_system_id IN (17, 34, 1121)
ORDER BY 1, e.effective_on
;




--- Check THE total number of providers in the pdb FOR THUNDERMIST:
COLUMN first_name FORMAT A16
COLUMN last_name FORMAT A16
COLUMN npi_number FORMAT A14
column covered_entity_system_id format a6 heading "CE SYS ID"
COLUMN tablename format a30
SELECT DISTINCT
    p.physician_id,
    case when h.cred_provider_import_id is not null 
        then 1 
        else 0
    end as cred_approved,
    case when mpl.manual_physicians_log_id is not null 
        then 1 
        else 0
    end as manual_approved,
    p.last_name, 
    p.first_name,
    tbl.tablename,
    e.event_id,
    e.effective_on,
    e.approved_for, 
    e.effective_on + e.approved_for AS current_expiry, 
    TRUNC(e.effective_on + e.approved_for - SYSDATE) AS num_days_b4_exp
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID --AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
LEFT JOIN source.manual_physicians_log mpl
    ON e.id = mpl.manual_physicians_log_id 
    AND e.table_oid = freedom.tableoid('source.manual_physicians_log')
    AND mpl.covered_entity_id IN (165,166,167,22891)
LEFT JOIN source.CRED_PROVIDER_IMPORT h
    on h.cred_provider_import_id = e.id
    AND H.FEED_ID IN (92)
LEFT JOIN FREEDOM.TABLEOID_LIST TBL
    ON TBL.TABLEOID = E.TABLE_OID
WHERE h.cred_provider_import_id is not null or mpl.manual_physicians_log_id is not null 
/*GROUP BY
    pl.covered_entity_system_id,
    p.physician_id, 
    p.last_name, 
    p.first_name,
    tbl.tablename,
    e.approved_for, 
    e.effective_on + e.approved_for, 
    TRUNC(e.effective_on + e.approved_for - SYSDATE)*/
ORDER BY p.physician_id ASC, e.event_id DESC
    ;
    
    
-- Check the total number of providers in the PDB for feed #92 whose source was the cred provider file:

SELECT
    pl.covered_entity_system_id,
    p.physician_id, 
    p.last_name, 
    p.first_name, 
    tbl.tablename,
    E.EVENT_ID,
    case when H.PHYSICIAN_ID is not null 
        then 1 
        else 0
    end as cred_approved,
    e.effective_on,
    e.approved_for, 
    e.effective_on + e.approved_for AS current_expiry, 
    TRUNC(e.effective_on + e.approved_for - SYSDATE) AS number_days_to_expire
FROM PDB.event e
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_identifiers pi ON pi.physician_id = pm.physician_id
JOIN PDB.physician_location pl ON PL.PHYSICIAN_LOCATION_ID = E.PHYSICIAN_LOCATION_ID
LEFT JOIN source.CRED_PROVIDER_IMPORT h
    on h.cred_provider_import_id=e.id
    and h.feed_id IN (92)
LEFT JOIN FREEDOM.TABLEOID_LIST TBL
    ON TBL.TABLEOID = E.TABLE_OID
AND e.table_oid = freedom.tableoid('source.cred_provider_import');


COLUMN imported_from FORMAT A25

select pl.covered_entity_system_id, pl.covered_entity_id, h.imported_from, TRUNC(h.imported_on),  p.physician_id, p.last_name, p.first_name 
from pdb.event e
join source.CRED_PROVIDER_IMPORT h
    on h.cred_provider_import_id=e.id
JOIN PDB.physician_map pm ON pm.ID = e.ID AND pm.table_oid = e.table_oid
JOIN PDB.physician p ON p.physician_id = pm.physician_id 
JOIN PDB.physician_location pl ON PL.PHYSICIAN_LOCATION_ID = E.PHYSICIAN_LOCATION_ID
where h.feed_id=92
and e.table_oid=freedom.tableoid('source.CRED_PROVIDER_IMPORT')
--and h.IMPORTED_FROM LIKE '20151005%'
order by TRUNC(h.imported_on) DESC;



-- uNION THE CLIENT LEVEL(121): CRED FILE TO THE
-- CES/CE ID LEVEL: MANUAL ENTRY TOOL 
