set serveroutput on
declare

    type t_disps is record(drug_dispensation_id integer, invoice_record_id integer);
    type tt_disps is table of t_disps index by binary_integer;
    disps tt_disps;

    i integer := #;
    v_reversing_nir integer;
    v_reversing_dd integer;
    v_site_id integer := ###;
    v_hospital_id integer := ####;
    v_feed_id integer := ###;
    v_user_id integer;
    v_reverse_reason varchar#(###) := '?';
    v_ticket_id integer := #######;
    v_cnt number := #;
    v_cnt# number := #;
    v_cnt# number := #;
    v_cnt# number := #;

begin

    select user_id into v_user_id from freedom.users where username = ?;

    with invoice_lists AS
    (
      SELECT distinct
	invoice_id_list, min_invoice_id
      FROM
      (
	select
	  freedom.implode(n.invoice_record_id) invoice_id_list,
	  min(n.invoice_record_id) min_invoice_id
	from ehr.drug_dispensation dd
	join source.charges_import i
	  on i.feed_id in (select distinct feed_id from adt.feed where covered_entity_system_id =  dd.hospital_id)
	  and i.charges_import_id = dd.src_id
	  and src_table_oid = freedom.tableoid('?')
	join sentinel.new_invoice_record n
	  on n.site_id = (select site_id from sentinel.site where hospital_id = dd.hospital_id)
	  and n.ehr_drug_dispensation_id = dd.drug_dispensation_id
	where dd.hospital_id = v_hospital_id
	and i.imported_from IN ('?')
	and i.feed_id  = v_feed_id
	and n.site_id = v_site_id
	GROUP BY i.mrn, I.ADMIT_DATE, i.charges_person_id, i.account_number, i.last_name, i.first_name, i.middle_initial, i.cdm_code, i.charge_amount, i.hcpc_code, i.rev_code, i.cpt_code, i. j_code, i.quantity, i.service_date, i.post_date, i.journal
	HAVING COUNT(DISTINCT dd.drug_dispensation_id) > #
	and count(distinct i.charges_import_id) > #
	AND COUNT(DISTINCT n.invoice_record_id) > #
      )
    )
    select distinct 
	nir.ehr_drug_dispensation_id,
	mi.invoice_record_id
    BULK COLLECT INTO disps
    FROM
    (
      SELECT DISTINCT 
	invoice_id_list,
	TO_NUMBER(i_id) as invoice_record_id
      FROM
      (
	SELECT 
	  il.invoice_id_list,
	  trim(REGEXP_SUBSTR(il.invoice_id_list,'?', #, levels.column_value)) i_id,
	  il.min_invoice_id
	FROM invoice_lists il,
	table(cast(multiset(select level from dual connect by level <= length (regexp_replace(il.invoice_id_list, '?')) + #) as sys.OdciNumberList)) levels
      )
      WHERE TO_NUMBER(i_id) <> min_invoice_id
    ) mi
    JOIN sentinel.new_invoice_record nir
      on nir.site_id = v_site_id
      and nir.invoice_record_id = mi.invoice_record_id
    ;

    

    dbms_output.put_line('?');

    if disps.count > # then
        for i in disps.first .. disps.last
        loop

            v_reversing_nir := sentinel.reverse_new_invoice_record(disps(i).invoice_record_id, v_site_id, v_reverse_reason);

            insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
            values (v_reversing_nir, freedom.tableoid_e('?' || disps(i).invoice_record_id, v_user_id);
		v_cnt := sql%rowcount + v_cnt;

            insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
            values (disps(i).invoice_record_id, freedom.tableoid_e('?' || v_reversing_nir, v_user_id);
		v_cnt# := sql%rowcount + v_cnt#;

            select ehr_drug_dispensation_id into v_reversing_dd 
            from sentinel.new_invoice_record 
            where invoice_record_id = v_reversing_nir
            and site_id = v_site_id;

            insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
            values (v_reversing_dd, freedom.tableoid_e('?' || disps(i).drug_dispensation_id, v_user_id);
		
		v_cnt# := sql%rowcount + v_cnt#;

            insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
            values (disps(i).drug_dispensation_id, freedom.tableoid_e('?' || v_reversing_dd, v_user_id);
		v_cnt# := sql%rowcount + v_cnt#;

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
