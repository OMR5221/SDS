SET SERVEROUTPUT OFF;

timing start log_merges_#;

-- Reference of all map excepts for KDMC:

DROP TABLE APPSUPPORT.TCS_#####_excepts;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_excepts AS
select distinct 
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.map_exception me
WHERE me.hospital_id IN (####)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;





-- Get all hashes associated map exceptions:


DROP TABLE appsupport.TCS_#####_except_hashes;
COMMIT;

CREATE TABLE appsupport.TCS_#####_except_hashes AS
SELECT DISTINCT 
    h.method,
    h.hash,
    h.id,
    h.table_oid,
    h.hospital_id
FROM ehr.hash h
WHERE (h.hospital_id, h.table_oid, h.id) IN
(
    SELECT DISTINCT
	hospital_id,
	table_oid,
	id
     FROM APPSUPPORT.TCS_#####_excepts
)
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_except_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



-- Perform lookup by exception hashes to get all related id/tableoids which are also map exceptions:


DROP TABLE appsupport.TCS_#####_hash_lookup;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hash_lookup AS
SELECT DISTINCT 
    h#.method,
    h#.hash,
    h#.hospital_id,
    h#.table_oid,
    h#.id
FROM ehr.hash h#
WHERE (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS_#####_except_hashes
)
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hash_lookup to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;






DROP TABLE APPSUPPORT.TCS_#####_sp_log_#;
COMMIT;


CREATE TABLE APPSUPPORT.TCS_#####_sp_log_# AS
select distinct
    sp.person_id
from
( 
    select DISTINCT
	pm.person_id
    from ehr.person_map pm
    JOIN ehr.demographics d
	on d.hospital_id = pm.hospital_id
	and d.table_oid = pm.table_oid
	and d.id = pm.id
    WHERE pm.person_id IN
    (
	SELECT DISTINCT pm.person_id
	FROM ehr.person_map pm
	WHERE (pm.hospital_id, pm.table_oid, pm.id) IN
	(
	    SELECT DISTINCT
		hospital_id,
		table_oid,
		id
	    FROM APPSUPPORT.TCS_#####_hash_lookup
	)
    ) 
    GROUP BY pm.person_id
    HAVING (COUNT(DISTINCT first_name) > # AND COUNT(DISTINCT last_name) > # AND COUNT(DISTINCT d.date_of_birth) > #)
) sp
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_sp_log_# to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE appsupport.TCS_#####_hl_join;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hl_join AS
SELECT DISTINCT 
    eh.id me_id,
    eh.table_oid me_table_oid,
    eh.hospital_id me_hospital_id,
    eh.method,
    eh.hash,
    hl.id,
    hl.table_oid,
    hl.hospital_id
FROM APPSUPPORT.TCS_#####_EXCEPT_hashes eh
JOIN APPSUPPORT.TCS_#####_hash_lookup hl
    ON hl.method = eh.method
    AND hl.hash = eh.hash
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hl_join to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE appsupport.TCS_#####_hlj_pm;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_pm AS
SELECT DISTINCT
pm.person_id,
hlj.me_id,
hlj.me_table_oid
FROM  APPSUPPORT.TCS_#####_hl_join hlj
JOIN ehr.person_map pm
on pm.hospital_id = hlj.hospital_id
and pm.table_oid = hlj.table_oid
and pm.id = hlj.id
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_pm to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;





DROP TABLE appsupport.TCS_#####_hlj_mult;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_mult AS
SELECT DISTINCT
hl#.person_id main_person_id,
hl#.person_id alt_person_id
FROM
(
    SELECT DISTINCT
      person_id,
      me_id,
      me_table_oid
    FROM APPSUPPORT.TCS_#####_hlj_pm
) hl#
JOIN 
(
    SELECT DISTINCT
      person_id,
      me_id,
      me_table_oid
    FROM APPSUPPORT.TCS_#####_hlj_pm
) hl#
on hl#.me_table_oid = hl#.me_table_oid
and hl#.me_id = hl#.me_id
where hl#.person_id <> hl#.person_id
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_mult to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;





DROP TABLE appsupport.TCS_#####_hlj_lk_a;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_lk_a AS
SELECT DISTINCT
hla.main_person_id,
hlb.alt_person_id
FROM appsupport.TCS_#####_hlj_mult hla
JOIN appsupport.TCS_#####_hlj_mult hlb
    on hla.alt_person_id = hlb.main_person_id
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_lk_a to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE appsupport.TCS_#####_hlj_lk_b;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_lk_b AS
SELECT DISTINCT
mi#.person_id mi#_person_id,
mi#.person_id mi#_person_id
FROM appsupport.TCS_#####_hlj_pm mi#
join  appsupport.TCS_#####_hlj_pm mi#
    on mi#.me_id = mi#.me_id
    and mi#.me_table_oid = mi#.me_table_oid
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_lk_b to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE appsupport.TCS_#####_hlj_lk;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_lk AS
SELECT DISTINCT
hl.main_person_id hla_mp,
freedom.implode(distinct mi.mi#_person_id) p_list,
COUNT(DISTINCT mi.mi#_person_id) num_alt_persons
FROM appsupport.TCS_#####_hlj_lk_a hl
join  appsupport.TCS_#####_hlj_lk_b mi
    on mi.mi#_person_id = hl.alt_person_id
group by
hl.main_person_id
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_lk to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE appsupport.TCS_#####_hlj_split;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_split AS
SELECT DISTINCT
p_list,
to_number(p_id) person_id
FROM
(
    SELECT DISTINCT
	npl.p_list,
	trim(REGEXP_SUBSTR(npl.p_list,  '?', #, levels.column_value)) p_id
    fROM APPSUPPORT.TCS_#####_hlj_lk npl,
    table(cast(multiset(select level from dual connect by level <= length (regexp_replace(npl.p_list, '?')) + #) as sys.OdciNumberList)) levels
    WHERE npl.num_alt_persons <= #
)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_split to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


DROP TABLE appsupport.TCS_#####_hlj_ma;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_ma AS
SELECT distinct
  hlk.HLA_MP main_person_id,
  to_number(hls.person_id) person_id,
  min(to_number(hls.person_id)) over (partition by hlk.HLA_MP) min_person_id
FROM appsupport.TCS_#####_hlj_lk hlk
JOIN appsupport.TCS_#####_hlj_split hls
  on HLS.P_LIST = HLK.P_LIST
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_ma to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE appsupport.TCS_#####_hlj_mb;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_mb AS
SELECT DISTINCT
    hlk#.HLA_MP main_person_id,
    to_number(hls#.person_id) OTHER_person_id
FROM appsupport.TCS_#####_hlj_lk hlk#
JOIN appsupport.TCS_#####_hlj_split hls#
  on HLS#.P_LIST = HLK#.P_LIST
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_mb to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


DROP TABLE appsupport.TCS_#####_hlj_mc;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_mc AS
SELECT distinct
    ma.main_person_id,
    ma.person_id,
    ma.min_person_id,
    mb.other_person_id
FROM appsupport.TCS_#####_hlj_ma ma 
JOIN appsupport.TCS_#####_hlj_mb mb
  ON ma.main_person_id = mb.other_person_id
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_mc to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE appsupport.TCS_#####_hlj_m#;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_m# AS
SELECT DISTINCT
min_person_id,
OTHER_person_id,
CASE WHEN min_person_id = OTHER_person_id THEN # ELSE # END IS_MIN_PERSON
from APPSUPPORT.TCS_#####_hlj_mc
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_m# to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



DROP TABLE appsupport.TCS_#####_hlj_m#;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_m# AS
SELECT DISTINCT
    min_person_id,
    count(distinct OTHER_person_id) num_other_p_ids,
    freedom.implode(distinct OTHER_person_id) other_person_id_list
from APPSUPPORT.TCS_#####_hlj_mc
GROUP BY
min_person_id
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_m# to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




DROP TABLE appsupport.TCS_#####_hlj_merge;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hlj_merge AS
SELECT DISTINCT
    fm.other_person_id_list person_id_list,
    hs.person_id,
    SUM(CASE WHEN sp.person_id IS NULL THEN # ELSE # END) OVER (PARTITION BY other_person_id_list ORDER BY other_person_id_list) sp_sum
FROM
(
    select distinct
	min_person_id,
	sum(other_is_a_min) other_min_sum,
	sum(keep) keep_num,
	sum(remove) remove_sum,
	other_person_id_list
    from
    (
	SELECT DISTINCT
	    m#.min_person_id,
	    m#.OTHER_person_id,
	    case when m#.min_person_id is null then # else # end other_is_a_min,
	    CASE WHEN m#.min_person_id is null then # else CASE WHEN m#.min_person_id = m#.OTHER_person_id THEN # ELSE # END end keep,
	    CASE WHEN m#.min_person_id is null then # else CASE WHEN m#.OTHER_person_id < m#.min_person_id  THEN # ELSE # END end remove,
	    num_other_p_ids,
	    M#.other_person_id_list
	from APPSUPPORT.TCS_#####_hlj_m# m#
	left join APPSUPPORT.TCS_#####_hlj_m# m#
	    on m#.other_person_id = m#.min_person_id
    )
    GROUP BY
    min_person_id,
    other_person_id_list
) fm
LEFT JOIN appsupport.TCS_#####_hlj_lk hk
  on hk.HLA_MP = fm.min_person_id
LEFT join appsupport.TCS_#####_hlj_split hs
  on hs.P_LIST = hk.P_LIST
LEFT JOIN appsupport.TCS_#####_sp_log_# sp
    ON sp.person_id = hs.person_id
WHERE fm.remove_sum < #
AND fm.OTHER_PERSON_ID_LIST IS NOT NULL
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hlj_merge to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;


DROP TABLE appsupport.TCS_#####_hl_excepts;
commit;

CREATE TABLE appsupport.TCS_#####_hl_excepts AS
SELECT DISTINCT 
    hl.method,
    hl.hash,
    me.hospital_id,
    me.table_oid,
    me.id
FROM appsupport.TCS_#####_hash_lookup hl
JOIN ehr.map_exception me
    ON me.hospital_id = hl.hospital_id
    AND me.table_oid = hl.table_oid
    and me.id = hl.id
;


-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hl_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;




-- Perform lookup by exception hashes to get all related id/tableoids which are not map exceptions but are mssing a person_map:


DROP TABLE appsupport.TCS_#####_hl_nonexcepts;
COMMIT;

CREATE TABLE appsupport.TCS_#####_hl_nonexcepts AS
SELECT DISTINCT 
    hl.method,
    hl.hash,
    hl.hospital_id,
    hl.table_oid,
    hl.id
FROM appsupport.TCS_#####_hash_lookup hl
LEFT JOIN ehr.map_exception me
    ON me.hospital_id = hl.hospital_id
    AND me.table_oid = hl.table_oid
    and me.id = hl.id
LEFT JOIN ehr.person_map pm
    ON pm.hospital_id = hl.hospital_id
    AND pm.table_oid = hl.table_oid
    and pm.id = hl.id
WHERE (me.id IS NULL and pm.id IS NULL)
;


grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hl_nonexcepts to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;


-- Perform lookup by exception hashes to get all related id/tableoids which have a person map:

DROP TABLE APPSUPPORT.TCS_#####_hl_persons;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_hl_persons AS
select distinct 
    h#.method,
    h#.hash,
    pm.person_id,
    pm.hospital_id,
    pm.table_oid,
    pm.id
FROM appsupport.TCS_#####_hash_lookup h#
JOIN ehr.person_map pm
    ON pm.hospital_id = h#.hospital_id
    and pm.table_oid = h#.table_oid
    and pm.id = h#.id
;

grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_hl_persons to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;

/*
-- Find all remaining superpersons for KDMC:


DROP table appsupport.TCS_#####_person_list;
COMMIT;

create table appsupport.TCS_#####_person_list AS
SELECT DISTINCT
  method,
  hash,
  COUNT(DISTINCT person_id) num_person_ids,
  SUM(is_sp) sp_sum,
  freedom.implode(DISTINCT person_id) person_id_list
from
(
  select distinct
      ep.method,
      ep.hash,
      ep.person_id,
      CASE WHEN sp.person_id IS NULL THEN # ELSE # END is_sp
  FROM APPSUPPORT.TCS_#####_hl_persons ep
  LEFT JOIN appsupport.TCS_#####_sp_log_# sp
      ON sp.person_id = ep.person_id
)
GROUP BY method, hash
;


grant select, insert, update, delete, alter on appsupport.TCS_#####_person_list to freedom, ehr, adt, sentinel, appuser, appsupport;
commit;


-- Using person maps found associated to the map exxeptions by a hash lookup
-- #. Remove all of thoser that are superpersons
-- #. Create listing of person ids with the same hash to consider for merge


DROP TABLE APPSUPPORT.TCS_#####_person_list_split;
COMMIT;



CREATE TABLE APPSUPPORT.TCS_#####_person_list_split AS
SELECT DISTINCT
    pl.person_id_list,
    ep.person_id,
    pl.sp_sum
FROM APPSUPPORT.TCS_#####_person_list pl
JOIN appsupport.TCS_#####_hl_persons ep
    on ep.method = pl.method
    and ep.hash = pl.hash
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_person_list_split to freedom, ehr, adt, sentinel, appuser, appsupport;
*/



DROP TABLE APPSUPPORT.TCS_#####_demo_hosps;
COMMIT;



CREATE TABLE APPSUPPORT.TCS_#####_demo_hosps AS
SELECT DISTINCT
HOSPITAL_ID,
TABLE_OID
FROM ehr.demographics d
WHERE d.hospital_id IN 
(
    SELECT DISTINCT
	hospital_id
    FROM APPSUPPORT.TCS_#####_hl_persons
)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_demo_hosps to freedom, ehr, adt, sentinel, appuser, appsupport;




--	#. Get all demographic ids for the persons associated to map exceptions:


DROP TABLE APPSUPPORT.TCS_#####_pl_ids;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_pl_ids AS
SELECT
    pl.person_id_list,
    pl.sp_sum,
    pm.person_id,
    pm.id,
    pm.table_oid,
    pm.hospital_id
FROM ehr.person_map pm
JOIN 
(
    SELECT DISTINCT 
	person_id_list,
	person_id,
	sp_sum
    FROM APPSUPPORT.TCS_#####_hlj_merge
    /*
    UNION
    SELECT DISTINCT 
	person_id_list,
	person_id,
	sp_sum
    FROM APPSUPPORT.TCS_#####_mh_split
    */
) pl    
on pm.person_id = pl.person_id
WHERE pm.person_id IN
(
    SELECT DISTINCT 
	to_number(person_id) person_id
    FROM APPSUPPORT.TCS_#####_hlj_merge
    /*
    UNION
    SELECT DISTINCT 
	to_number(person_id) person_id
    FROM APPSUPPORT.TCS_#####_mh_split
    */
)
AND pm.table_oid IN
(
    SELECT DISTINCT
	d.table_oid
    FROM APPSUPPORT.TCS_#####_demo_hosps d
    WHERE d.hospital_id = pm.hospital_id
)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_pl_ids to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;



--	#. Preliminary determination of if person_id_list should be merged based on the count of hashes 
-- 	associated to method # of the ehr.autohash() function (SSN-DOB)

--	#. Those person_id_lists with only a single hash for method one are considered can_merge = #


DROP TABLE APPSUPPORT.TCS_#####_pl_classify;
COMMIT;


CREATE TABLE APPSUPPORT.TCS_#####_pl_classify AS
SELECT DISTINCT
    person_id_list,
    COUNT(DISTINCT person_id) num_persons,
    COUNT(DISTINCT ssn) num_ssn,
    CASE WHEN SUM(is_sp) > # THEN # ELSE # END has_sp,
    CASE
    WHEN COUNT(DISTINCT prelim_merge) = # THEN
        CASE WHEN SUM(prelim_merge) >= # THEN 
          CASE 
            WHEN COUNT(DISTINCT ssn) > # THEN #
            ELSE #
            END
        ELSE #
        END
    WHEN COUNT(DISTINCT prelim_merge) > # THEN
        CASE
        WHEN COUNT(DISTINCT ssn) <= # THEN #
        ELSE #
        END
    ELSE #
    END can_merge
FROM
(
    SELECT DISTINCT
        person_id_list,
        person_id,
        ssn,
        CASE
            WHEN ((p_fname = d_fname OR p_lname = d_lname)
            AND ((p_dob = d_dob))) THEN #
            WHEN ((p_fname = d_fname AND p_lname = d_lname)) THEN #
            ELSE #
        END prelim_merge,
        CASE WHEN SUM(sp_sum) > # THEN # ELSE # END is_sp
    FROM
    (
        SELECT DISTINCT
            person_id_list,
            person_id,
            sp_sum,
            p_fname,
            p_lname,
            p_dob,
            d_fname,
            d_lname,
            d_dob,
            ssn
        FROM
        (
          SELECT
            fpl.person_id_list,
            fpl.person_id,
            sp_sum,
            UPPER(REGEXP_REPLACE(trim(both from p.first_name), '?' , null)) p_fname,
            UPPER(REGEXP_REPLACE(trim(both from p.last_name), '?' , null)) p_lname,
            p.date_of_birth p_dob,
            UPPER(REGEXP_REPLACE(trim(both from d.first_name), '?' , null)) d_fname,
            UPPER(REGEXP_REPLACE(trim(both from d.last_name), '?' , null)) d_lname,
            d.date_of_birth d_dob,
            d.ssn
          FROM 
	  (
	    SELECT DISTINCT
		person_id_list,
		person_id
	    FROM APPSUPPORT.TCS_#####_hlj_merge
	    /*
	    UNION
	    SELECT DISTINCT
		person_id_list,
		person_id
	    FROM APPSUPPORT.TCS_#####_mh_split
	    */
	  ) fpl
          JOIN ehr.person p
              on p.person_id = fpl.person_id
          JOIN APPSUPPORT.TCS_#####_pl_ids pm
              on pm.person_id = fpl.person_id
          JOIN ehr.demographics d
            ON d.hospital_id = pm.hospital_id
            AND d.table_oid = pm.table_oid
            AND d.id = pm.id
        )
        GROUP BY
            person_id_list,
            person_id,
            sp_sum,
            p_fname,
            p_lname,
            p_dob,
            d_fname,
            d_lname,
            d_dob,
            ssn
    )
    GROUP BY
        person_id_list,
        person_id,
        ssn,
        CASE
        WHEN ((p_fname = d_fname OR p_lname = d_lname)
              AND ((p_dob = d_dob)))
          THEN #
        WHEN ((p_fname = d_fname AND p_lname = d_lname))
          THEN #
        ELSE #
        END
)
GROUP BY
person_id_list
;
                                   

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_pl_classify to freedom, ehr, adt, sentinel, appuser, appsupport;




--    #. Get all id/tableoids associated to potential merges:



DROP TABLE APPSUPPORT.TCS_#####_mp_prelim;
commit;

CREATE TABLE APPSUPPORT.TCS_#####_mp_prelim AS
SELECT DISTINCT
    pm.person_id,
    PM.ID,
    PM.TABLE_OID,
    pm.hospital_id
FROM APPSUPPORT.TCS_#####_pl_ids pm
WHERE (pm.person_id) IN
(
    SELECT DISTINCT 
	pl.person_id
    FROM appsupport.TCS_#####_pl_classify mp
    JOIN 
    (	
	SELECT
	    person_id_list,
	    person_id
	FROM APPSUPPORT.TCS_#####_hlj_merge
	/*
	UNION
	SELECT
	    person_id_list,
	    person_id
	FROM APPSUPPORT.TCS_#####_mh_split
	*/
    ) PL
	on pl.person_id_list = mp.person_id_list
    where mp.person_id_list is not null
    and mp.can_merge = #
    and mp.has_sp = #
)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_mp_prelim to freedom, ehr, adt, sentinel, appuser, appsupport;



    -- #. Get hashes associated to the potential merges id/tableoids

DROP TABLE APPSUPPORT.TCS_#####_mp_hashes;
commit;

CREATE TABLE APPSUPPORT.TCS_#####_mp_hashes AS
SELECT DISTINCT
    pl.person_id,
    h#.method,
    h#.hash
FROM APPSUPPORT.TCS_#####_mp_prelim pl
JOIN 
(
    SELECT 
	method, 
	hash,
	hospital_id,
	table_oid,
	id
    FROM ehr.hash
    WHERE (hospital_id, table_oid, id) IN
    (
	SELECT DISTINCT
	    hospital_id,
	    table_oid,
	    id
	FROM APPSUPPORT.TCS_#####_mp_prelim
    )
) h#
    on h#.hospital_id = pl.hospital_id
    and h#.table_oid = pl.table_oid
    and h#.id = pl.id
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_mp_hashes to freedom, ehr, adt, sentinel, appuser, appsupport;


-- #. Perform a hash lookup to get all related  records for the merge id/tableoids
-- that are associated to a map exception




DROP TABLE APPSUPPORT.TCS_#####_mp_excepts;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_mp_excepts AS
SELECT DISTINCT
    me.id,
    me.table_oid,
    me.hospital_id
FROM ehr.hash h#
LEFT JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
where (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS_#####_mp_hashes
)
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_mp_excepts to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;


 
--#. Perform a hash lookup to get all related  records for the merge id/tableoids
-- that are not associated to a map exception, but still do not have a person map




DROP TABLE APPSUPPORT.TCS_#####_mp_nonexcepts;
COMMIT;

CREATE TABLE APPSUPPORT.TCS_#####_mp_nonexcepts AS
SELECT DISTINCT
    h#.id,
    h#.table_oid,
    h#.hospital_id
FROM ehr.hash h#
LEFT JOIN ehr.map_exception me
  ON me.hospital_id = h#.hospital_id
  and me.table_oid = h#.table_oid
  and me.id = h#.id
LEFT JOIN ehr.person_map pm
  ON pm.hospital_id = h#.hospital_id
  and pm.table_oid = h#.table_oid
  and pm.id = h#.id
where (h#.method, h#.hash) IN
(
    SELECT DISTINCT
	method,
	hash
    FROM APPSUPPORT.TCS_#####_mp_hashes
)
and me.id IS NULL 
AND pm.person_id IS NULL
;

-- grant permissions on this table
grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_mp_nonexcepts to freedom, ehr, adt, sentinel, appuser, appsupport;





DROP table appsupport.TCS_#####_merge_log;
COMMIT;

create table appsupport.TCS_#####_merge_log
(
   "PERSON_ID_LIST" VARCHAR#(###)
);



grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_merge_log to freedom, ehr, adt, sentinel, appuser, appsupport;

COMMIT;



drop table appsupport.TCS_#####_mismatch_log;
COMMIT;

create table appsupport.TCS_#####_mismatch_log
(
   "PERSON_ID_LIST" VARCHAR#(###)
);


grant select, insert, update, delete, alter on APPSUPPORT.TCS_#####_mismatch_log to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;

timing stop;
