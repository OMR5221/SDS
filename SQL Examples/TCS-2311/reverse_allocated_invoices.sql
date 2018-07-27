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
	    join sentinel.allocation_set_item asi 
		on asi.event_id = poi.inpatient_po_item_id 
		and asi.event_tableoid = freedom.tableoid('?') 
		and asi.site_id = poi.site_id
	    join sentinel.allocation_set aset 
		on asi.allocation_set_id = aset.allocation_set_id 
		and aset.site_id = asi.site_id
	    where po.site_id = my_site_id
	    and po.wholesaler_account_id in (####, ###, ###, ###) 
	    and aset.never_touch = #
	    and (poi.inpatient_po_id, po.id, poi.table_oid) NOT IN
	    (
		select distinct 
		  poi.inpatient_po_id,
		  poi.id, 
		  poi.table_oid
		FROM
		(
		    select distinct 
		      poi.inpatient_po_id,
		      poi.id, 
		      poi.table_oid,
		      count(distinct poi.inpatient_po_item_id) num_po_item_ids
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
		    and po.wholesaler_account_id in (####, ###, ###, ###)
		    and aset.never_touch = #
		    group by
		      poi.inpatient_po_id,
		      poi.id, 
		      poi.table_oid
		    having count(distinct poi.inpatient_po_item_id) > #
		) poi
	    )
	    and (poi.inpatient_po_item_id) NOT IN
	    (
		select distinct
		  poi.inpatient_po_item_id
		FROM
		(
		    select distinct
		      poi.inpatient_po_item_id,
		      COUNT(DISTINCT aset.never_touch) never_touch_stat,
		      COUNT(DISTINCT aset.allocation_set_id) num_alloc_sets
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
		    and po.wholesaler_account_id in (####, ###, ###, ###)
		    group by
		      poi.inpatient_po_item_id
		    having count(distinct ASET.NEVER_TOUCH) > #
		    and COUNT(DISTINCT aset.allocation_set_id) > #
		) poi
	    )
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
