DECLARE

    v_feed_id NUMBER := ##;

BEGIN
    
    DELETE FROM ehr.ce_id_queue
    where feed_id = v_feed_id
    and table_oid = freedom.tableoid('?')
    AND TRUNC(CREATED_ON) < TO_DATE('?')
    ;

    COMMIT;

END;
