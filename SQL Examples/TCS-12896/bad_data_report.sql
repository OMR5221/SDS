spool ./TCS####_bd_report.txt;

SET LINESIZE ###;
SET PAGESIZE #####;


/*
col id_vals format a##
col persons format a##
COL DEMOS FORMAT A##
select distinct
(bd.pool_type_id || '?' || bd.value) id_val,
-- to_char(substr(freedom.implode_nl_large(distinct bd.pool_type_id || '?' || bd.value),#,####)) ID_VALS,
to_char(substr(freedom.implode_nl_large(distinct bd.person_id || '?'|| bd.p_dob),#,####)) persons,
to_char(substr(freedom.implode_nl_large(distinct bd.hospital_id || '?'|| bd.d_dob),#,####)) DEMOS
from appsupport.TCS####_bad_client_data bd
group by 
bd.method,
bd.hash,
bd.pool_type_id,
bd.value
HAVING count(distinct bd.person_id) > #
-- ) or (COUNT(DISTINCT bd.d_fname) > # or COUNT(DISTINCT bd.d_lname) > #) AND COUNT(DISTINCT bd.d_dob) > # )
;
*/

-- superpersons
col person_id format a##;
col hashed format a##;
col id_val format a##
col person format a##
COL DEMO FORMAT A##
select distinct
    person_id,
    person,
    id_val,   
    to_char(substr(freedom.implode_nl_large(distinct demo),#,####)) DEMOS
FROM
(
    select 
    to_char(bd.person_id) person_id,
    (BD.METHOD || '?'|| BD.HASH) HASHED,
    (bd.pool_type_id || '?' || bd.value) id_val,
    (bd.p_fname || '?'|| bd.p_dob) person,
    (bd.d_fname || '?'|| bd.d_dob) DEMO
    from appsupport.TCS####_bad_client_data bd
)
GROUP BY
    hashed,
    person_id,
    person,
    id_val
HAVING COUNT(DISTINCT demo) > #
ORDER BY
PERSON_ID,
id_val
;


spool off;

