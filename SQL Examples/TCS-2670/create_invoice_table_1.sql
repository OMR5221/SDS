timing start create_temp_tables_#;

DROP TABLE APPSUPPORT.TCS####_invoices;


CREATE TABLE APPSUPPORT.TCS####_invoices
(
    "INVOICE_NUMBER" VARCHAR#(###)
);


-- grant permissions on this table
grant select, insert, update, delete on APPSUPPORT.TCS####_invoices to freedom, ehr, adt, sentinel, appuser, appsupport;
