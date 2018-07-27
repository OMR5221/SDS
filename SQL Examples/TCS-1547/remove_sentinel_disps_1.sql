timing start rm_sent_disps;

-- Duplicate Charge Files (period ##.##.## - ##.##.##) Need Reversal - Carle Hoopeston 
set serveroutput on size unlimited format word_wrapped

declare

    -- who to jabber
    v_jabber varchar#(###) := '?';

    -- object definition
    type t_set is record(set_id integer);
    type tt_sets is table of t_set index by binary_integer;
    alloc_sets tt_sets;

    type t_disps is record(invoice_record_id integer, has_alloc_item integer);
    type tt_disps is table of t_disps index by binary_integer;
    disps tt_disps;

    i integer := #;
    v_reversing_nir integer;
    v_reversing_dd integer;
    v_user_id integer;
    v_site_id NUMBER := ##;
    v_hospital_id NUMBER := #;
    v_reverse_reason varchar#(###) := '?';
    v_ticket_id integer := #######;

begin

    select user_id into v_user_id from freedom.users where username = ?;


    SELECT distinct 
	ast.allocation_set_id set_id
    bulk collect into alloc_sets
    from 
    (
	SELECT DISTINCT 
	    n.site_id,
	    n.invoice_record_id
	from sentinel.new_invoice_record n
	LEFT JOIN EHR.DRUG_DISPENSATION DD
	    on DD.hospital_id = v_hospital_id
	    and n.ehr_drug_dispensation_id = dd.drug_dispensation_id
	where N.SITE_ID = v_site_id
	AND dd.drug_dispensation_id IS NULL
    ) nir
    JOIN sentinel.allocation_set_item asi
	ON asi.site_id = v_site_id
	AND asi.event_id = nir.invoice_record_id
	AND asi.event_tableoid = freedom.tableoid('?')
    JOIN sentinel.allocation_set ast
	ON asi.site_id = ast.site_id
	AND asi.allocation_set_id = ast.allocation_set_id
    ;

    IF alloc_sets.count > # THEN

	FOR i in alloc_sets.FIRST .. alloc_sets.LAST
	LOOP

	    sentinel.allocation_reversal.reverse_and_delete_set(v_site_id, alloc_sets(i).set_id, v_reverse_reason, ?);

	END LOOP;

	COMMIT;

    END IF;



    SELECT distinct 
	nir.invoice_record_id, 
	case when asi.event_id is not null then # else # end has_alloc_item
    bulk collect into disps
    from 
    (
	SELECT DISTINCT 
	    n.site_id,
	    n.invoice_record_id
	from sentinel.new_invoice_record n
	LEFT JOIN EHR.DRUG_DISPENSATION DD
	    on DD.hospital_id = v_hospital_id
	    and n.ehr_drug_dispensation_id = dd.drug_dispensation_id
	where N.SITE_ID = v_site_id
	AND dd.drug_dispensation_id IS NULL
    ) nir
    LEFT JOIN sentinel.allocation_set_item asi
	ON asi.site_id = v_site_id
	AND asi.event_id = nir.invoice_record_id
	AND asi.event_tableoid = freedom.tableoid('?')
    LEFT JOIN sentinel.allocation_set ast
	ON asi.site_id = ast.site_id
	AND asi.allocation_set_id = ast.allocation_set_id
    ;

    freedom.send_jabber(v_jabber,'?');

    if disps.count > # then
        
	for i in disps.first .. disps.last
        loop

	    IF disps(i).has_alloc_item = # THEN

		DELETE FROM sentinel.allocation_set_item asi
		WHERE asi.site_id = v_site_id
		AND asi.event_tableoid = freedom.tableoid('?')
		AND asi.event_id = disps(i).invoice_record_id
		;

		COMMIT;

	    END IF;
	    
	

	    DELETE FROM sentinel.new_invoice_record nir
	    WHERE nir.site_id = v_site_id
	    AND nir.invoice_record_id = disps(i).invoice_record_id;

            if mod(i,###) = # then
               commit;
                if mod(i,####) = # then
                    freedom.send_jabber(v_jabber,'?' || i);
                end if;
            end if;

        end loop;
       commit;
    end if;
end;
/


timing stop;
