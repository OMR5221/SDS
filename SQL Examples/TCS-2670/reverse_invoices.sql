DECLARE

    v_username VARCHAR#(##) := ?;
    v_ticket_number varchar#(##) := '?';
    v_time_stamp VARCHAR#(##);
    v_jabber varchar#(###) := '?';   
    v_result NUMBER;
    v_site_id NUMBER := ###;
    v_user_id NUMBER;
    v_reverse_reason varchar#(###) := '?';
    v_ticket_id NUMBER := #######;

    TYPE r_record IS RECORD (inpatient_po_item_id NUMBER, inpatient_po_id NUMBER, site_id NUMBER, total_price NUMBER, quantity NUMBER);
    TYPE t_table  IS TABLE OF r_record;
    v_invoices t_table;


      v_new_inpatient_po_id number;
      v_new_inpatient_po_item_id number;
BEGIN


    SELECT DISTINCT user_id INTO v_user_id 
    FROM freedom.users 
    WHERE username = v_username;

   --Grab the invoice items that need to   

    SELECT ip.inpatient_po_item_id, ip.inpatient_po_id, ip.site_id, ip.total_price, ip.quantity 
    BULK COLLECT INTO v_invoices
    FROM SENTINEL.INPATIENT_PO I
    JOIN SENTINEL.INPATIENT_PO_ITEM IP ON IP.INPATIENT_PO_ID = I.INPATIENT_PO_ID
    join wholesaler.account wa on wa.account_id = i.wholesaler_account_id
    WHERE I.SITE_ID = v_site_id
    and wholesaler_account_id in (#####)
    and invoice_number IN
    (
	SELECT DISTINCT
	    invoice_number
	FROM appsupport.tcs####_invoices
    )
    and not exists
    (
	select #
	from sentinel.allocation_set_item asi
	join sentinel.allocation_set ast on asi.allocation_set_id = ast.allocation_set_id and ast.site_id = i.site_id
	where asi.event_id = ip.inpatient_po_item_id and asi.event_tableoid = tableoid('?') and asi.site_id = i.site_id
	and ast.never_touch = #
    );

         

    FOR i IN v_invoices.FIRST .. v_invoices.LAST
    LOOP

	--Reverse
	v_result := sentinel.reverse_inpatient_po_item(v_invoices(i).inpatient_po_item_id, v_site_id, v_reverse_reason);


	-- Log the reversal 
	insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
	values (v_invoices(i).inpatient_po_item_id, freedom.tableoid('?' ||v_invoices(i).inpatient_po_item_id, v_user_id);


	IF MOD(i, ####) = # then
	    COMMIT;
	END IF;
     
    END LOOP;

    COMMIT;

    -- Reimport onto new account:
    
    
    FOR i IN v_invoices.FIRST .. v_invoices.LAST
    LOOP


	select sentinel.inpatient_po_item_id_seq.nextval into v_new_inpatient_po_item_id from dual;
	
	INSERT INTO sentinel.inpatient_po_item (site_id, inpatient_po_id, inpatient_po_item_id, ndc, quantity, date_ordered, unit_price, total_price, id, table_oid)
	select site_id, v_new_inpatient_po_id, v_new_inpatient_po_item_id, ndc, quantity, date_ordered, unit_price, total_price, id, table_oid
	from sentinel.inpatient_po_item 
	where site_id = v_site_id and inpatient_po_id = v_invoices(i).inpatient_po_id and inpatient_po_item_id = v_invoices(i).inpatient_po_item_id;
	
	
       -- Log the new record
       insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
       values ( v_new_inpatient_po_item_id, freedom.tableoid('?' || v_new_inpatient_po_item_id, v_user_id);
       commit;

	IF MOD(i, ####) = # then
	    COMMIT;
	END IF;
     

    END LOOP;

    COMMIT;


--


END;
/
