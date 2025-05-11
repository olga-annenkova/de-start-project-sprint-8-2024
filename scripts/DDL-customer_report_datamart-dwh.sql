DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- COMMENT 'идентификатор записи',
    customer_id BIGINT NOT NULL, -- COMMENT 'идентификатор заказчика',
    customer_name VARCHAR NOT NULL, -- COMMENT 'Ф. И. О. заказчика',
    customer_address VARCHAR NOT NULL, -- COMMENT 'его адрес',
    customer_birthday DATE NOT NULL, -- COMMENT 'дата рождения',
    customer_email VARCHAR NOT NULL, -- COMMENT 'электронная почта',
	platform_money BIGINT NOT NULL, -- COMMENT 'сумма, которую потратил заказчик',
	customer_money NUMERIC(15,2) NOT NULL, -- COMMENT 'сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик)',
	craftsman_id BIGINT NOT NULL, -- COMMENT 'идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого',
    count_order BIGINT NOT NULL, -- COMMENT 'количество заказов у заказчика за месяц',
    avg_price_order NUMERIC(10,2) NOT NULL, -- COMMENT 'средняя стоимость одного заказа у заказчика за месяц',
    median_time_order_completed NUMERIC(10,1), -- COMMENT 'медианное время в днях от момента создания заказа до его завершения за месяц',
    top_craftsman BIGINT NOT NULL, -- COMMENT 'идентификатор самого популярного мастера',
    top_product_category VARCHAR NOT NULL, -- COMMENT 'самая популярная категория товаров у этого заказчика за месяц',
    count_order_created BIGINT NOT NULL, -- COMMENT 'количество созданных заказов за месяц',
    count_order_in_progress BIGINT NOT NULL, -- COMMENT 'количество заказов в процессе изготовки за месяц',
    count_order_delivery BIGINT NOT NULL, -- COMMENT 'количество заказов в доставке за месяц',
    count_order_done BIGINT NOT NULL, -- COMMENT 'количество завершённых заказов за месяц',
    count_order_not_done BIGINT NOT NULL, -- COMMENT 'количество незавершённых заказов за месяц',
    report_period VARCHAR NOT NULL, -- COMMENT 'отчётный период (год и месяц)',
    CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);
    COMMENT ON COLUMN dwh.customer_report_datamart.id IS 'идентификатор записи';
    COMMENT ON COLUMN dwh.customer_report_datamart.customer_id IS 'идентификатор заказчика';
    COMMENT ON COLUMN dwh.customer_report_datamart.customer_name IS 'Ф. И. О. заказчика';
    COMMENT ON COLUMN dwh.customer_report_datamart.customer_address IS 'адрес';
    COMMENT ON COLUMN dwh.customer_report_datamart.customer_birthday IS 'дата рождения';
    COMMENT ON COLUMN dwh.customer_report_datamart.customer_email IS 'электронная почта';
	COMMENT ON COLUMN dwh.customer_report_datamart.platform_money IS 'сумма, которую потратил заказчик';
	COMMENT ON COLUMN dwh.customer_report_datamart.customer_money IS 'сумма, которую заработала платформа от покупок заказчика за месяц (10% от суммы, которую потратил заказчик)';
	COMMENT ON COLUMN dwh.customer_report_datamart.craftsman_id IS 'идентификатор самого популярного мастера ручной работы у заказчика. Если заказчик сделал одинаковое количество заказов у нескольких мастеров, возьмите любого';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order IS 'количество заказов у заказчика за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.avg_price_order IS 'средняя стоимость одного заказа у заказчика за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.median_time_order_completed IS 'медианное время в днях от момента создания заказа до его завершения за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.top_craftsman IS 'идентификатор самого популярного мастера';
    COMMENT ON COLUMN dwh.customer_report_datamart.top_product_category IS 'самая популярная категория товаров у этого заказчика за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order_created IS 'количество созданных заказов за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order_in_progress IS 'количество заказов в процессе изготовки за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order_delivery IS 'количество заказов в доставке за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order_done IS 'количество завершённых заказов за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.count_order_not_done IS 'количество незавершённых заказов за месяц';
    COMMENT ON COLUMN dwh.customer_report_datamart.report_period IS 'отчётный период (год и месяц)';
    ---
    COMMENT ON TABLE dwh.customer_report_datamart IS 'таблица учета данных по заказчикам';