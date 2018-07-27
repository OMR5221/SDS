DECLARE
	my_inv_record SENTINEL.INPATIENT_PO_ITEM%ROWTYPE;
	type pArray is table of integer index by binary_integer;
	items pArray;
	pos pArray;
	i integer := #;
	foo integer := #;
	my_site_id integer := ###;
	my_user_id integer;
	counter integer := #;
begin
	select user_id into my_user_id from freedom.users where username = ?;

	--reverse currently allocated invoices
	for cur in 
	(
	    select distinct poi.inpatient_po_item_id
	    from sentinel.inpatient_po po
	    join sentinel.inpatient_po_item poi 
		on poi.site_id = po.site_id 
		and poi.inpatient_po_id = po.inpatient_po_id
	    join sentinel.site s on s.site_id = poi.site_id
	    LEFT join sentinel.allocation_set_item asi 
		on asi.event_id = poi.inpatient_po_item_id 
		and asi.event_tableoid = freedom.tableoid('?') 
		and asi.site_id = poi.site_id
	    LEFT join sentinel.allocation_set aset 
		on asi.allocation_set_id = aset.allocation_set_id 
		and aset.site_id = asi.site_id
	    where po.site_id = my_site_id
	    and po.wholesaler_account_id in (####, ###, ###, ###)
	    AND asi.event_id is null
	) 
	loop

	    foo := sentinel.reverse_inpatient_po_item(cur.inpatient_po_item_id, my_site_id, '?');

	    counter := counter + #;
	    if (counter > ###) then
		    commit;	
		    counter := #;
	    end if;

	end loop;

	commit;
end;
/
