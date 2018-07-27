timing start del_ucrn;

DECLARE

    v_feed_id NUMBER := ##;

BEGIN
    
    DELETE FROM source.ucrn_list ul
    where ul.feed_id = v_feed_id
    AND ul.imported_from not in ('?')
    and trunc(ul.imported_on) > to_date(?,?)
    and trunc(ul.imported_on) < to_date(?, ?)
    ;

    COMMIT;

END;
/

timing stop;
