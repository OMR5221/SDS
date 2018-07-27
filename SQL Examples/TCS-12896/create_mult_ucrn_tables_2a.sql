timing start mult_ucrn_load;

DROP TABLE APPSUPPORT.TSP###_#_bad_mcf;
COMMIT;

-- Table for values with multiple claim refs but only # account number
-- and a mudulo == # nuratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_bad_mcf AS
SELECT 
    ef.*,
    NUM_GOOD_CLAIM_REFS,
    GOOD_CLAIM_REF_NUM
FROM
(
    SELECT distinct
	id,
	num_good_claim_refs,
	GOOD_CLAIM_REF_NUM 
    FROM appsupport.TSP###_#_EC_RECS 
    WHERE num_good_claim_refs > #
) es
JOIN appsupport.TSP###_#_ec_recs_full ef
    on ef.id = es.id
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_bad_mcf to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;




DROP TABLE APPSUPPORT.TSP###_#_rank_files;
COMMIT;

-- Table for records with a corresponding ratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_rank_files AS
SELECT AN.*,
DENSE_RANK() OVER 
(
  PARTITION BY GOOD_CLAIM_REF_NUM, MRN
  ORDER BY MRN, IMPORTED_FROM, GOOD_CLAIM_REF_NUM
) AS RANKED_FILES
FROM
(
  SELECT DISTINCT 
    MR.GOOD_CLAIM_REF_NUM,
    MR.MRN,
    MR.IMPORTED_FROM
  FROM appsupport.TSP###_#_bad_mcf mr
) AN
ORDER BY
    MRN,
    IMPORTED_FROM,
    GOOD_CLAIM_REF_NUM
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_rank_files to freedom, ehr, adt, sentinel, appuser, appsupport;
COMMIT;



DROP TABLE APPSUPPORT.TSP###_#_rank_acctnum_file;
COMMIT;

-- Per File:
-- Table for records with a corresponding ratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_rank_acctnum_file AS
SELECT AN.*,
DENSE_RANK() OVER 
(
  PARTITION BY IMPORTED_FROM, GOOD_CLAIM_REF_NUM, MRN
  ORDER BY MRN, GOOD_CLAIM_REF_NUM, IMPORTED_FROM, ACCOUNT_NUMBER
) AS RANKED_ACCT_NUMS
FROM
(
  SELECT DISTINCT 
    MR.GOOD_CLAIM_REF_NUM,
    MR.ACCOUNT_NUMBER,
    MR.MRN,
    MR.IMPORTED_FROM
  FROM appsupport.TSP###_#_bad_mcf mr
) AN
ORDER BY
    MRN,
    IMPORTED_FROM,
    GOOD_CLAIM_REF_NUM,
    ACCOUNT_NUMBER
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_rank_acctNum_file to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;



DROP TABLE APPSUPPORT.TSP###_#_rank_acctNum;


-- Table for records with a corresponding ratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_rank_acctNum AS
SELECT AN.*,
DENSE_RANK() OVER 
(
  PARTITION BY GOOD_CLAIM_REF_NUM, MRN
  ORDER BY MRN, GOOD_CLAIM_REF_NUM, ACCOUNT_NUMBER
) AS RANKED_ACCT_NUMS
FROM
(
  SELECT DISTINCT 
    MR.GOOD_CLAIM_REF_NUM,
    MR.ACCOUNT_NUMBER,
    MR.MRN
  FROM appsupport.TSP###_#_bad_mcf mr
) AN
ORDER BY
    MRN,
    GOOD_CLAIM_REF_NUM,
    ACCOUNT_NUMBER
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_rank_acctNum to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;
DROP TABLE APPSUPPORT.TSP###_#_rank_id;


-- Table for records with a corresponding ratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_rank_id AS
SELECT DISTINCT 
AN.*,
ROW_NUMBER() OVER
(
  PARTITION BY GOOD_CLAIM_REF_NUM, MRN
  ORDER BY MRN, GOOD_CLAIM_REF_NUM
) AS RANK_ID
FROM
(
  SELECT DISTINCT
    MR.ID,
    MR.GOOD_CLAIM_REF_NUM,
    KC.MEDICAL_RECORD_NUMBER MRN
  FROM appsupport.TSP###_#_bad_mcf mr
  JOIN appsupport.kdmc_edi_###_claim kc
    on MR.MRN = KC.MEDICAL_RECORD_NUMBER
    and mr.bad_claim_ref_num = substr(kc.account_number, #,#)
) AN
ORDER BY
    MRN,
    GOOD_CLAIM_REF_NUM,
    ID
;

-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_rank_id to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;



DROP TABLE APPSUPPORT.TSP###_#_final_mcf;


-- Table for records with a corresponding ratio of claim refs to maps
CREATE TABLE APPSUPPORT.TSP###_#_final_mcf AS
SELECT DISTINCT
    id,
    table_oid,
    hospital_id,
    good_claim_ref_num,
    bad_claim_ref_num,
    account_number,
    mrn,
    imported_from
FROM
(
    SELECT DISTINCT
	mr.id,
	mr.table_oid,
	mr.hospital_id,
	mr.good_claim_ref_num,
	mr.bad_claim_ref_num,
	mr.mrn,
	ds.num_ec_maps,
	ds.num_good_claim_refs,
	mr.imported_from,
	rf.RANKED_FILES,
	rlk#.RANKED_ACCT_NUMS,
	rlk#.RANKED_ACCT_NUMS,
        CASE
        WHEN mult_file_dist <> # AND single_file_dist >  # AND more_maps = # THEN
            MOD(ri.rank_id, ds.num_good_claim_refs)
        ELSE
            ri.rank_id
        END rank_id,
        mult_file_disT,
        single_file_dist,
        CASE
        WHEN mult_file_dist = # THEN
	    rlk#.account_number
        -- Num ids match the number of claim ref nums found:
        WHEN single_file_dist >= # THEN
            rlk#.account_number
        END account_number,
        CASE
        WHEN mult_file_dist = # THEN
            CASE WHEN ranked_files > # THEN
              (ds.NUM_GOOD_CLAIM_REFS * (RANKED_FILES - #)) + rlk#.RANKED_ACCT_NUMS
            ELSE
              rlk#.RANKED_ACCT_NUMS
            END
        -- Num ids match the number of claim ref nums found:
        WHEN single_file_dist >= # THEN
            rlk#.ranked_acct_nums
	/*
	WHEN single_file_dist > # THEN
            CASE WHEN ranked_files > # THEN
              (ds.NUM_GOOD_CLAIM_REFS * (RANKED_FILES - #)) + rlk#.RANKED_ACCT_NUMS
            ELSE
              rlk#.RANKED_ACCT_NUMS
            END
	*/
        END UPDATED_ACCTNUM_RANK
    FROM appsupport.TSP###_#_bad_mcf mr
    LEFT JOIN
    (
	select distinct 
	    DG.*,
            ((num_ec_maps / num_good_claim_refs) / num_import_files) mult_file_dist,
            MOD(num_ec_maps, num_good_claim_refs) single_file_dist,
	    CASE WHEN num_ec_maps > num_good_claim_refs THEN # ELSE # END more_maps
	FROM
	(
	  SELECT DISTINCT
	    bad_claim_ref_num,
	    good_claim_ref_num,
	    mrn,
	    NUM_GOOD_CLAIM_REFS ,
	    COUNT(DISTINCT imported_from) num_import_files,
	    count(distinct id) num_ec_maps
	  FROM APPSUPPORT.TSP###_#_bad_mcf
	  GROUP BY
	    bad_claim_ref_num,
	    good_claim_ref_num,
	    mrn,
	    NUM_GOOD_CLAIM_REFS
	) dg
    ) ds
	on ds.bad_claim_ref_num = mr.bad_claim_ref_num
	and ds.mrn = mr.mrn
        AND ds.good_claim_ref_num = mr.good_claim_ref_num
    LEFT JOIN APPSUPPORT.TSP###_#_rank_acctNum_file rlk#
	on rlk#.mrn = mr.mrn
	and rlk#.good_claim_ref_num = mr.good_claim_ref_num
	and rlk#.imported_from = mr.imported_from
    LEFT JOIN APPSUPPORT.TSP###_#_rank_acctNum rlk#
	on rlk#.mrn = mr.mrn
	and rlk#.good_claim_ref_num = mr.good_claim_ref_num
    LEFT JOIN APPSUPPORT.TSP###_#_rank_files rf
	on rf.mrn = mr.mrn
	and rf.good_claim_ref_num = mr.good_claim_ref_num
	and rf.imported_from = mr.imported_from
    LEFT JOIN APPSUPPORT.TSP###_#_rank_id ri
	on ri.mrn = mr.mrn
	and ri.good_claim_ref_num = mr.good_claim_ref_num
	and ri.id = mr.id
) fc
WHERE updated_acctnum_rank = rank_id
;


-- grant permissions on this table

grant select, insert, update, delete on APPSUPPORT.TSP###_#_final_mcf to freedom, ehr, adt, sentinel, appuser, appsupport;


COMMIT;


timing stop;
