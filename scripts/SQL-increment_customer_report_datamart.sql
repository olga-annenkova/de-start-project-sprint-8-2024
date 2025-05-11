WITH
dwh_delta AS (
	-- Шаг 2
	SELECT 	
		fo.customer_id AS customer_id,
		dcs.customer_name AS customer_name,
		dcs.customer_address AS customer_address,
		dcs.customer_birthday AS customer_birthday,
		dcs.customer_email AS customer_email,
		fo.order_id AS order_id,
		dp.product_id AS product_id,
		dp.product_price AS product_price,
		dp.product_type AS product_type,
		dc.craftsman_id as craftsman_id,
		fo.order_completion_date - fo.order_created_date AS diff_order_date, 
		fo.order_status AS order_status,
		to_char(fo.order_created_date, 'yyyy-mm') AS report_period,
		dcs.customer_id AS exist_customer_id,
        fo.load_dttm AS order_load_dttm,
		dc.load_dttm AS craftsman_load_dttm,
		dcs.load_dttm AS customers_load_dttm,
		dp.load_dttm AS products_load_dttm
			FROM dwh.f_order fo 
				INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id=dc.craftsman_id 
				INNER JOIN dwh.d_customer dcs ON fo.customer_id=dcs.customer_id 
				INNER JOIN dwh.d_product dp ON fo.product_id=dp.product_id  
				LEFT JOIN dwh.customer_report_datamart crd ON dcs.customer_id = crd.customer_id
					WHERE 
                    fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
					dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
					dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart) OR
					dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)
),
dwh_update_delta AS (
	-- Шаг 3
	SELECT DISTINCT d.customer_id
	FROM dwh_delta d
	LEFT JOIN dwh.customer_report_datamart crd
    ON d.customer_id = crd.customer_id
	WHERE 
	d.exist_customer_id IS NOT NULL
),
dwh_delta_insert_result AS (
	-- Шаг 4
	    SELECT  
        T4.customer_id AS customer_id,
        T4.customer_name AS customer_name,
        T4.customer_address AS customer_address,
        T4.customer_birthday AS customer_birthday,
        T4.customer_email AS customer_email,
        T4.customer_money AS customer_money,
        T4.platform_money AS platform_money,
        T4.count_order AS count_order,
        T4.avg_price_order AS avg_price_order,
        --T4.top_product_category AS top_product_category,
        T4.craftsman_id as top_craftsman,
        T4.product_type AS top_product_category,
        T4.median_time_order_completed AS median_time_order_completed,
        T4.count_order_created AS count_order_created,
        T4.count_order_in_progress AS count_order_in_progress,
        T4.count_order_delivery AS count_order_delivery,
        T4.count_order_done AS count_order_done,
        T4.count_order_not_done AS count_order_not_done,
        T4.report_period AS report_period 
    FROM (
        SELECT
            *,--T2.*,
            RANK() OVER (PARTITION BY T2.customer_id ORDER BY count_order DESC) AS rank_count_product,
            FIRST_VALUE (top_craftsman_id) OVER(PARTITION BY t2.customer_id ORDER BY top_count_order DESC)  AS craftsman_id
        FROM (
            SELECT
                T1.customer_id,
                T1.customer_name,
                T1.customer_address,
                T1.customer_birthday,
                T1.customer_email,
                T1.report_period,
                SUM(T1.product_price) * 0.9 AS customer_money,
                SUM(T1.product_price) * 0.1 AS platform_money,
                COUNT(order_id) AS count_order,
                AVG(T1.product_price) AS avg_price_order,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY T1.diff_order_date) AS median_time_order_completed,
                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done
            FROM dwh_delta AS T1
            WHERE T1.exist_customer_id IS NULL
            GROUP BY 
                T1.customer_id,
                T1.customer_name,
                T1.customer_address,
                T1.customer_birthday,
                T1.customer_email,
                T1.report_period
        ) AS T2
    INNER JOIN (
      -- T3: топ-мастер по клиенту
      SELECT
        dd.customer_id AS customer_id_for_craftsman,
        dd.craftsman_id AS top_craftsman_id,
        COUNT(dd.order_id) AS top_count_order
      FROM dwh_delta AS dd
      WHERE dd.exist_customer_id IS NULL
      GROUP BY dd.customer_id, dd.craftsman_id
    ) AS T3 ON T2.customer_id = T3.customer_id_for_craftsman
    INNER JOIN (
      -- T5: топ-продукт по клиенту
      SELECT
		dd.customer_id AS customer_id_for_product_type, 
		dd.product_type,-- AS top_product_category, 
		count(dd.product_id) AS count_product
      FROM dwh_delta AS dd
      GROUP BY dd.customer_id, dd.product_type
      ORDER BY count_product DESC 
    ) AS t5 ON T2.customer_id = T5.customer_id_for_product_type
  ) AS T4
  WHERE T4.rank_count_product = 1
  ORDER BY T4.report_period
),
dwh_delta_update_result AS (
	-- Шаг 5
	SELECT 
			T4.customer_id AS customer_id,
			T4.customer_name AS customer_name,
			T4.customer_address AS customer_address,
			T4.customer_birthday AS customer_birthday,
			T4.customer_email AS customer_email,
			T4.customer_money AS customer_money,
			T4.platform_money AS platform_money,
			T4.count_order AS count_order,
			T4.avg_price_order AS avg_price_order,
			T4.product_type AS top_product_category,
			T4.craftsman_id AS top_craftsman,
			T4.median_time_order_completed AS median_time_order_completed,
			T4.count_order_created AS count_order_created,
			T4.count_order_in_progress AS count_order_in_progress,
			T4.count_order_delivery AS count_order_delivery, 
			T4.count_order_done AS count_order_done, 
			T4.count_order_not_done AS count_order_not_done,
			T4.report_period AS report_period 
			FROM (
				SELECT 	-- в этой выборке объединяем две внутренние выборки по расчёту столбцов витрины и применяем оконную функцию, чтобы определить самую популярную категорию товаров
						*,
						RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_order DESC) AS rank_count_product,
						FIRST_VALUE (top_craftsman_id) OVER(PARTITION BY t2.customer_id ORDER BY top_count_order DESC)  AS craftsman_id
						FROM (
							SELECT -- в этой выборке делаем расчёт по большинству столбцов, так как все они требуют одной и той же группировки, кроме столбца с самой популярной категорией товаров у мастера. Для этого столбца сделаем отдельную выборку с другой группировкой и выполним join
								T1.customer_id AS customer_id,
								T1.customer_name AS customer_name,
								T1.customer_address AS customer_address,
								T1.customer_birthday AS customer_birthday,
								T1.customer_email AS customer_email,
								SUM(T1.product_price) - (SUM(T1.product_price) * 0.1) AS customer_money,
								SUM(T1.product_price) * 0.1 AS platform_money,
								COUNT(order_id) AS count_order,
								AVG(T1.product_price) AS avg_price_order,
							--	AVG(T1.customer_age) AS avg_age_customer,
								PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
								SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created, 
								SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
								SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
								SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
								SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
								T1.report_period AS report_period
								FROM (
									SELECT 	-- в этой выборке достаём из DWH обновлённые или новые данные по мастерам, которые уже присутствуют в витрине
											dcs.customer_id AS customer_id,
											dcs.customer_name AS customer_name,
											dcs.customer_address AS customer_address,
											dcs.customer_birthday AS customer_birthday,
											dcs.customer_email AS customer_email,
											fo.order_id AS order_id,
											dp.product_id AS product_id,
											dp.product_price AS product_price,
											dp.product_type AS product_type,
											fo.order_completion_date - fo.order_created_date AS diff_order_date,
											fo.order_status AS order_status, 
											to_char(fo.order_created_date, 'yyyy-mm') AS report_period
											FROM dwh.f_order fo 
												INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
												INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
												INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
												INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id
								) AS T1
									GROUP BY 
                                    T1.customer_id,
                                    T1.customer_name,
                                    T1.customer_address,
                                    T1.customer_birthday,
                                    T1.customer_email,
                                    T1.report_period
							) AS T2 
								INNER JOIN (
								      -- T3: топ-мастер по клиенту
							      SELECT
							        dd.customer_id AS customer_id_for_craftsman,
							        dd.craftsman_id AS top_craftsman_id,
							        COUNT(dd.order_id) AS top_count_order
							      FROM dwh_delta AS dd
							      WHERE dd.exist_customer_id IS NULL
							      GROUP BY dd.customer_id, dd.craftsman_id
							    ) AS T3 ON T2.customer_id = T3.customer_id_for_craftsman
							    INNER JOIN (
							      -- T5: топ-продукт по клиенту
							      SELECT
									dd.customer_id AS customer_id_for_product_type, 
									dd.product_type, 
									count(dd.product_id) AS count_product
							      FROM dwh_delta AS dd
							      GROUP BY dd.customer_id, dd.product_type
								  ORDER BY count_product DESC 
								) AS t5 ON T2.customer_id = t5.customer_id_for_product_type
  ) AS T4
  WHERE T4.rank_count_product = 1
  ORDER BY T4.report_period
),
insert_delta AS (
	-- Шаг 6
INSERT INTO dwh.customer_report_datamart 
(
	customer_id,
	customer_name,
	customer_address,
	customer_birthday,
	customer_email,
	customer_money,
	platform_money,
	count_order,
	avg_price_order,
	median_time_order_completed,
	top_product_category, 
	top_craftsman,
	count_order_created,
	count_order_in_progress,
	count_order_delivery, 
	count_order_done, 
	count_order_not_done,
	report_period)
SELECT 
	customer_id,
	customer_name,
	customer_address,
	customer_birthday,
	customer_email,
	customer_money,
	platform_money,
	count_order,
	avg_price_order,
	top_craftsman,
	top_product_category,
	median_time_order_completed,
	count_order_created,
	count_order_in_progress,
	count_order_delivery, 
	count_order_done, 
	count_order_not_done,
	report_period
FROM  dwh_delta_insert_result
),
update_delta AS (
	-- Шаг 7
 UPDATE dwh.customer_report_datamart SET
        customer_id = update.customer_id,
        customer_name = update.customer_name,
        customer_address = update.customer_address,
        customer_birthday = update.customer_birthday,
        customer_email = update.customer_email,
        customer_money = update.customer_money,
        platform_money = update.platform_money,
        count_order = update.count_order,
        avg_price_order = update.avg_price_order,
		top_product_category = update.top_product_category, 
		top_craftsman = update.top_craftsman,
        median_time_order_completed = update.median_time_order_completed,
        count_order_created = update.count_order_created,
        count_order_in_progress = update.count_order_in_progress,
        count_order_delivery = update.count_order_delivery,
        count_order_done = update.count_order_done,
        count_order_not_done = update.count_order_not_done,
        report_period = update.report_period
	FROM (
        SELECT 
			customer_id,
			customer_name,
			customer_address,
			customer_birthday,
			customer_email,
			customer_money,
            platform_money,
            count_order,
            avg_price_order,
            top_craftsman,
            top_product_category,
            median_time_order_completed,
            count_order_created,
            count_order_in_progress,
            count_order_delivery, 
            count_order_done, 
            count_order_not_done,
            report_period
        FROM dwh_delta_update_result) AS update
	WHERE dwh.customer_report_datamart.customer_id = update.customer_id
),
insert_load_date AS (
	-- Шаг 8
INSERT INTO dwh.load_dates_customer_report_datamart (
		load_dttm
	)
    SELECT 
    GREATEST(
    COALESCE(MAX(craftsman_load_dttm), NOW()),
    COALESCE(MAX(customers_load_dttm), NOW()),
    COALESCE(MAX(products_load_dttm), NOW()))
		FROM dwh_delta
)
SELECT 'increment datamart';
