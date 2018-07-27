declare
 type t_pos is record(inpatient_po_id number);
 type tt_pos is table of t_pos index by binary_integer;
 pos tt_pos;
 
 type t_pois is record(inpatient_po_item_id number);
 type tt_pois is table of t_pois index by binary_integer;
 pois tt_pois;
 
  v_hospital_id number := ####;
  v_site_id number := ###;
  
  v_user_id integer;
  v_reverse_reason varchar#(###) := '?';
  v_ticket_id integer := #######;
  v_old_account_id number := #####;
  v_new_account_id number := #####;
  v_result number;
  
  v_new_inpatient_po_id number;
  v_new_inpatient_po_item_id number;
  
  type vdarray is varray(####) of varchar#(##);
  v_invoice_number vdarray;
 
begin
 
    select user_id into v_user_id from freedom.users where username = ?;

    
    -- Fetch all po's still needing to be reversed and reimported
    SELECT invoice_number
    BULK COLLECT INTO v_invoice_number
    FROM appsupport.tcs####_invoices
    ;

    freedom.send_jabber(?, '?');

    for i in v_invoice_number.first .. v_invoice_number.last 
    loop
	
	select distinct 
	    p.inpatient_po_id 
	bulk collect into pos
	from sentinel.inpatient_po p
	join sentinel.inpatient_po_item poi 
	    on poi.inpatient_po_id = p.inpatient_po_id 
	    and poi.site_id = p.site_id
	join sentinel.allocation_set_item asi
	    on asi.event_id = poi.inpatient_po_item_id
	    and asi.event_tableoid = freedom.tableoid('?')
	    and asi.site_id = poi.site_id
	join sentinel.allocation_set ast
	    on asi.allocation_set_id = ast.allocation_set_id
	    and ast.site_id = asi.site_id
	where p.site_id = v_site_id 
	and wholesaler_account_id = v_old_account_id 
	and invoice_number = v_invoice_number(i)
	and ast.never_touch = #
	-- Remove allocation sets with a zeroed out quantity:
	and (ast.allocation_set_id) NOT IN
	(
	  select
	    aset.allocation_set_id
	  from sentinel.inpatient_po po
	  join sentinel.inpatient_po_item poi
	      on poi.site_id = po.site_id
	      and poi.inpatient_po_id = po.inpatient_po_id
	  join sentinel.site s on s.site_id = poi.site_id
	  join sentinel.allocation_set_item asi
	      on asi.event_id = poi.inpatient_po_item_id
	      and asi.event_tableoid = freedom.tableoid('?')
	      and asi.site_id = poi.site_id
	  join sentinel.allocation_set aset
	      on asi.allocation_set_id = aset.allocation_set_id
	      and aset.site_id = asi.site_id
	  where po.site_id = ###
	  and po.wholesaler_account_id in (#####)
	  group by
	    aset.allocation_set_id
	  having sum(asi.uom_quantity) = #
	  and count(distinct poi.INPATIENT_PO_ITEM_ID) > #
	)
	;

     
	if pos.count > # then

	    for i in pos.first .. pos.last loop
		  
		  --Create the new PO ID
		  select sentinel.inpatient_po_id_seq.nextval into v_new_inpatient_po_id from dual;
		  
		  insert into sentinel.inpatient_po (inpatient_po_id, site_id, po_num, invoice_number, is_###b, wholesaler_account_id, date_created)
		  select v_new_inpatient_po_id, site_id, po_num, invoice_number, is_###b, v_new_account_id, date_created
		  from sentinel.inpatient_po 
		  where site_id = v_site_id 
		  and inpatient_po_id = pos(i).inpatient_po_id;
		
		  
		  commit;
		  
		  -- Go through all unreversed PO Item IDs
		  -- Confirm if allocated only those with never_touch = # are processed:
		  select distinct poi.inpatient_po_item_id 
		    bulk collect into pois
		  from sentinel.inpatient_po p
		  join sentinel.inpatient_po_item poi 
		    on poi.inpatient_po_id = p.inpatient_po_id 
		    and poi.site_id = p.site_id
		  join sentinel.allocation_set_item asi
		    on asi.event_id = poi.inpatient_po_item_id
		    and asi.event_tableoid = freedom.tableoid('?')
		    and asi.site_id = poi.site_id
		  join sentinel.allocation_set ast
		    on asi.allocation_set_id = ast.allocation_set_id
		    and ast.site_id = asi.site_id
		  left join freedom.record_changes_log rcl 
		    on rcl.id = poi.inpatient_po_item_id 
		    and rcl.table_oid = freedom.tableoid('?') 
		    and rcl.ticket_number = v_ticket_id
		  where p.site_id = v_site_id 
		    and wholesaler_account_id = v_old_account_id 
		    and rcl.id is null 
		    and p.inpatient_po_id = pos(i).inpatient_po_id
		    -- Filter allocated invoices with never_touch = #:
		    and ast.never_touch = #    
		    and (ast.allocation_set_id) NOT IN
		    (
		      select
			aset.allocation_set_id
		      from sentinel.inpatient_po po
		      join sentinel.inpatient_po_item poi
			  on poi.site_id = po.site_id
			  and poi.inpatient_po_id = po.inpatient_po_id
		      join sentinel.site s on s.site_id = poi.site_id
		      join sentinel.allocation_set_item asi
			  on asi.event_id = poi.inpatient_po_item_id
			  and asi.event_tableoid = freedom.tableoid('?')
			  and asi.site_id = poi.site_id
		      join sentinel.allocation_set aset
			  on asi.allocation_set_id = aset.allocation_set_id
			  and aset.site_id = asi.site_id
		      where po.site_id = ###
		      and po.wholesaler_account_id in (#####)
		      group by
			aset.allocation_set_id
		      having sum(asi.uom_quantity) = #
		      and count(distinct poi.INPATIENT_PO_ITEM_ID) > #
		    )      
		    ;
	 
		    if pois.count > # then

		      for j in pois.first .. pois.last loop
			    
			    --Reverse
			    v_result := sentinel.reverse_inpatient_po_item(pois(j).inpatient_po_item_id, v_site_id, v_reverse_reason);
			    -- Log the reversal 
			    insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
			    values (pois(j).inpatient_po_item_id, freedom.tableoid('?' ||pois(j).inpatient_po_item_id, v_user_id);
			    
			    -- Reinsert the PO Item under the new PO ID
		    
			    select sentinel.inpatient_po_item_id_seq.nextval into v_new_inpatient_po_item_id from dual;
			    
			    INSERT INTO sentinel.inpatient_po_item (site_id, inpatient_po_id, inpatient_po_item_id, ndc, quantity, date_ordered, unit_price, total_price, id, table_oid)
			    select site_id, v_new_inpatient_po_id, v_new_inpatient_po_item_id, ndc, quantity, date_ordered, unit_price, total_price, id, table_oid
			    from sentinel.inpatient_po_item 
			    where site_id = v_site_id and inpatient_po_id = pos(i).inpatient_po_id and inpatient_po_item_id = pois(j).inpatient_po_item_id;
			    
			    
			    --Log the new record
			   insert into freedom.record_changes_log(id, table_oid, ticket_number, note, user_id)
			   values ( v_new_inpatient_po_item_id, freedom.tableoid('?' || v_new_inpatient_po_item_id, v_user_id);
			   commit;
			    
		     end loop;
		 end if;
	 end loop;
	commit;
     end if;
     
       ---Update the account id in wholesaler invoice table
	  update wholesaler.invoice set account_id =  v_new_account_id 
	  where account_id =  v_old_account_id and  invoice_number = v_invoice_number(i);
	  commit;
	  
	  IF MOD(i, ###) = # THEN
		commit;
	      freedom.send_jabber(?, '?');
	   END IF;
     end loop; 

     freedom.send_jabber(?, '?');
 
end;
/
