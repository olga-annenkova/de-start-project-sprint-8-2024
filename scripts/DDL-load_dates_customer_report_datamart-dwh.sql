-- dwh.load_dates_customer_report_datamart определение

-- Drop table

-- DROP TABLE dwh.load_dates_customer_report_datamart;

CREATE TABLE dwh.load_dates_customer_report_datamart (
	id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	load_dttm date NOT NULL,
	CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);