timing start map_table_load;

DROP TABLE APPSUPPORT.TSP###_#_ec_prelim;


CREATE TABLE APPSUPPORT.TSP###_#_ec_prelim AS
SELECT *
FROM
(
    SELECT
	ec.feed_id,
	af.covered_entity_system_id hospital_id,
	edi_###_claim_id id,
	freedom.tableoid('?') table_oid,
	--account_number bad_claim_ref_num,
	substr(account_number,#,#) bad_claim_ref_num,
	substr(account_number,#,#) good_claim_ref_num,
	medical_record_number,
	NVL(member_identification_number,?) member_id_num,
	(billing_provider_organization || pay_to_provider_address_# || pay_to_provider_zip) provider_dtl,
	imported_from
    FROM source.edi_###_claim ec
    join adt.feed af
	on af.feed_id = ec.feed_id
    WHERE ec.feed_id = ##
    and UPPER(regexp_replace(imported_from,'?'
    AND length(account_number) = #
    AND account_number IS NOT NULL
    AND medical_record_number IS NOT NULL
)
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_ec_prelim to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;




DROP TABLE APPSUPPORT.TSP###_#_pb_prelim;

CREATE TABLE APPSUPPORT.TSP###_#_pb_prelim AS
SELECT 
    substr(account_number,#,#) bad_claim_ref_num,
    substr(account_number,#,#) good_claim_ref_num,
    account_number,
    medical_record_number,
    NVL(member_identification_number,?) member_id_num,
    (billing_provider_organization || pay_to_provider_address_# || pay_to_provider_zip) provider_dtl,
    imported_from
FROM appsupport.kdmc_edi_###_claim kc
WHERE UPPER(regexp_replace(imported_from,'?'
AND account_number IS NOT NULL
AND medical_record_number IS NOT NULL
-- Exclude already used edi account numbers from consideration:
AND NOT EXISTS 
(
    SELECT #
    FROM source.edi_###_claim oec 
    where oec.feed_id = ## 
    and oec.medical_record_number = kc.medical_record_number
    and oec.account_number = kc.account_number
)
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_pb_prelim to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;




DROP TABLE APPSUPPORT.TSP###_#_ec_recs_full;


CREATE TABLE APPSUPPORT.TSP###_#_ec_recs_full AS
SELECT
    id,
    table_oid,
    feed_id,
    hospital_id,
    bad_claim_ref_num,
    imported_from,
    account_number,
    mrn
FROM
(
    SELECT DISTINCT
        mp.id,
        mp.table_oid,
        mp.feed_id,
        mp.hospital_id,
        mp.bad_claim_ref_num,
        mp.medical_record_number mrn,
	mp.imported_from,
        ec.account_number
    FROM appsupport.TSP###_#_ec_prelim mp
    JOIN appsupport.TSP###_#_pb_prelim ec
        on mp.bad_claim_ref_num = ec.bad_claim_ref_num
        AND mp.medical_record_number = ec.medical_record_number
        AND mp.imported_from = ec.imported_from
        AND mp.member_id_num = ec.member_id_num
        AND mp.provider_dtl = ec.provider_dtl
)
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_ec_recs_full to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;

DROP TABLE APPSUPPORT.TSP###_#_ec_recs;


CREATE TABLE APPSUPPORT.TSP###_#_ec_recs AS
SELECT
    id,
    table_oid,
    feed_id,
    hospital_id,
    bad_claim_ref_num,
    imported_from,
    substr(TO_CHAR(GOOD_CLAIM_REF_NUM),#,###) good_claim_ref_num,
    mrn,
    num_good_claim_refs
FROM
(
    SELECT DISTINCT
        mp.id,
        mp.table_oid,
        mp.feed_id,
        mp.hospital_id,
        mp.bad_claim_ref_num,
        mp.medical_record_number mrn,
	mp.imported_from,
        freedom.implode(distinct substr(ec.account_number,#,#)) good_claim_ref_num,
        COUNT(DISTINCT substr(ec.account_number,#,#)) num_good_claim_refs
    FROM appsupport.TSP###_#_ec_prelim mp
    JOIN appsupport.TSP###_#_pb_prelim ec
        on mp.bad_claim_ref_num = ec.bad_claim_ref_num
        AND mp.medical_record_number = ec.medical_record_number
        AND mp.imported_from = ec.imported_from
        AND mp.member_id_num = ec.member_id_num
        AND mp.provider_dtl = ec.provider_dtl
    GROUP BY
        mp.id,
        mp.table_oid,
        mp.feed_id,
        mp.hospital_id,
        mp.bad_claim_ref_num,
        mp.medical_record_number,
	mp.imported_from
)
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_ec_recs to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;

DROP TABLE APPSUPPORT.TSP###_#_npb_recs;


CREATE TABLE APPSUPPORT.TSP###_#_npb_recs AS
SELECT DISTINCT
    npb.id,
    npb.table_oid,
    npb.feed_id,
    npb.hospital_id,
    npb.claim_ref_num,
    npb.mrn,
    CASE WHEN m.id IS NULL THEN # ELSE # END has_acctNum_map
FROM
(
    SELECT
	np.id,
	freedom.tableoid('?') table_oid,
	np.feed_id,
	## hospital_id,
	np.claim_ref_num,
	np.mrn
    FROM
    (
	SELECT
	    feed_id,
	    edi_###_claim_id id,
	    substr(account_number,#,#) claim_ref_num,
	    medical_record_number mrn
	FROM source.edi_###_claim ec
	WHERE ec.feed_id = ##
	and UPPER(regexp_replace(imported_from,'?'
	AND account_number IS NOT NULL
	AND medical_record_number IS NOT NULL
    ) np
    -- JOIN events with a ce_id value
    JOIN ehr.event e 
	on e.hospital_id = ##
	and freedom.tableoid('?') = e.table_oid 
	AND np.id = e.id
    WHERE e.covered_entity_id is null
) npb
LEFT JOIN 
(
    -- Acct Num maps:
    SELECT 
	m.hospital_id,
	m.table_oid,
	m.id
    FROM ehr.map m
    JOIN ehr.pool p
	on m.pool_id = p.pool_id
	and p.pool_type_id = #
    WHERE m.hospital_id = ##
    and m.table_oid = freedom.tableoid('?')
) m
    ON m.hospital_id = npb.hospital_id
    AND m.table_oid = npb.table_oid
    AND m.id = npb.id
;



-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_npb_recs to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;


timing stop;
