set serveroutput on
declare

    type t_disps is record(drug_dispensation_id integer, invoice_record_id integer);
    type tt_disps is table of t_disps index by binary_integer;
    disps tt_disps;

    i integer := #;
    v_reversing_nir integer;
    v_reversing_dd integer;
    v_site_id integer := ##;
    v_hospital_id integer := #;
    v_feed_id integer := #;
    v_user_id integer;
    v_reverse_reason varchar#(###) := '?';
    v_ticket_id integer := #######;
    v_cnt number := #;
    v_cnt# number := #;
    v_cnt# number := #;
    v_cnt# number := #;

begin

    select user_id into v_user_id from freedom.users where username = ?;

    with disp_lists AS
    (
     SELECT distinct
	disp_id_list, min_disp_id
     FROM
     (
	select
	  freedom.implode(dd.drug_dispensation_id) disp_id_list,
	  min(dd.drug_dispensation_id) min_disp_id
	from ehr.drug_dispensation dd
	join source.charges_import i
	  on i.feed_id in (select distinct feed_id from adt.feed where covered_entity_system_id =  dd.hospital_id)
	  and i.charges_import_id = dd.src_id
	  and src_table_oid = freedom.tableoid('?')
	left join sentinel.new_invoice_record n
	  on n.site_id = (select site_id from sentinel.site where hospital_id = dd.hospital_id)
	  and n.ehr_drug_dispensation_id = dd.drug_dispensation_id
	    where dd.hospital_id = v_hospital_id
	    and i.imported_from like '?'
	    and trunc(dd.created_on) > to_date('?')
	    and i.feed_id  = v_feed_id
	GROUP BY i.mrn, I.ADMIT_DATE, i.charges_person_id, i.account_number, i.last_name, i.first_name, i.middle_initial, i.cdm_code, i.charge_amount, i.hcpc_code, i.rev_code, i.cpt_code, i. j_code, i.quantity, i.service_date, i.post_date, i.journal
	HAVING COUNT(DISTINCT dd.drug_dispensation_id) > #
	and count(distinct i.charges_import_id) > #
     )
    )
    select distinct
	mi.drug_dispensation_id,
	nir.invoice_record_id
     BULK COLLECT INTO disps
    FROM
    (
     SELECT DISTINCT
	disp_id_list,
	TO_NUMBER(d_id) as drug_dispensation_id
     FROM
     (
	SELECT
	  il.disp_id_list,
	  trim(REGEXP_SUBSTR(il.disp_id_list,'?', #, levels.column_value)) d_id,
	  il.min_disp_id
	FROM disp_lists il,
	table(cast(multiset(select level from dual connect by level <= length (regexp_replace(il.disp_id_list, '?')) + #) as sys.OdciNumberList)) levels
     )
     WHERE TO_NUMBER(d_id) <> min_disp_id
    ) mi
    LEFT JOIN sentinel.new_invoice_record nir
     on nir.site_id = v_site_id
     and nir.ehr_drug_dispensation_id = mi.drug_dispensation_id
    ;

    

    dbms_output.put_line('?');

    if disps.count > # then
        for i in disps.first .. disps.last
        loop

	    delete from ehr.pdap_dispensation_queue
	    where hospital_id=v_hospital_id
	    and drug_dispensation_id = disps(i).drug_dispensation_id;

	    IF disps(i).invoice_record_id IS NOT NULL THEN

		v_reversing_nir := sentinel.reverse_new_invoice_record(disps(i).invoice_record_id, v_site_id, v_reverse_reason);

		insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
		values (v_reversing_nir, freedom.tableoid_e('?' || disps(i).invoice_record_id, v_user_id);
		    v_cnt := sql%rowcount + v_cnt;

		insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
		values (disps(i).invoice_record_id, freedom.tableoid_e('?' || v_reversing_nir, v_user_id);
		    
		v_cnt# := sql%rowcount + v_cnt#;
	
	    ELSE

		v_reversing_dd := ehr.reverse_ehr_drug_dispensation(disps(i).drug_dispensation_id, v_hospital_id);


		insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
		values (v_reversing_dd, freedom.tableoid_e('?' || disps(i).drug_dispensation_id, v_user_id);
		    
		v_cnt# := sql%rowcount + v_cnt#;

		insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
		values (disps(i).drug_dispensation_id, freedom.tableoid_e('?' || v_reversing_dd, v_user_id);
		    
		v_cnt# := sql%rowcount + v_cnt#;


	    END IF;

            if mod(i,###) = # then
                commit;
            end if;

        end loop;
        commit;
    end if;

    dbms_output.put_line('?' || v_cnt);
    dbms_output.put_line('?' || v_cnt#);
    dbms_output.put_line('?' || v_cnt#);
    dbms_output.put_line('?' || v_cnt#);

end;
/
