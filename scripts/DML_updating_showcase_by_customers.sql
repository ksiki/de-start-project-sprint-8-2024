with
last_load_date as (
	select coalesce(max(load_dttm), '1900-01-01'::date) as last_dttm
	from dwh.load_dates_customer_report_datamart
), 
dwh_delta as (
	select
		dc.customer_id as customer_id,
		dc.customer_name as customer_name,
		dc.customer_address as customer_address,
		dc.customer_birthday as customer_birthday,
		dc.customer_email as customer_email,
		fo.order_id AS order_id,
		dcf.craftsman_id as craftsman_id,
		dp.product_id AS product_id,
		dp.product_price AS product_price,
		dp.product_type AS product_type,
		(fo.order_completion_date - fo.order_created_date) AS diff_order_date, 
		fo.order_status as order_status,
		crd.customer_id AS exist_customer_id,
		dc.load_dttm AS customer_load_dttm,
		dcf.load_dttm AS craftsman_load_dttm,
		dp.load_dttm AS products_load_dttm,
		to_char(fo.order_created_date, 'yyyy-mm') AS report_period
	from dwh.f_order fo 
	inner join dwh.d_customer dc on fo.customer_id = dc.customer_id
	inner join dwh.d_craftsman dcf on fo.craftsman_id = dcf.craftsman_id
	inner join dwh.d_product dp on fo.product_id = dp.product_id
	left join dwh.customer_report_datamart crd on fo.customer_id = crd.customer_id
	cross join last_load_date lld
	where fo.load_dttm > lld.last_dttm
		or dc.load_dttm > lld.last_dttm
		or dcf.load_dttm > lld.last_dttm
		or dp.load_dttm > lld.last_dttm
),
dwh_update_delta AS (
	select 	
		customer_id
	from dwh_delta dd 
	where exist_customer_id is not null 
),
dwh_delta_insert_result as (
	select 
		cus_rep.customer_id as customer_id,
		cus_rep.customer_name as customer_name,
		cus_rep.customer_address as customer_address, 
		cus_rep.customer_birthday as customer_birthday,
		cus_rep.customer_email as customer_email,
		cus_rep.customer_money_spend as customer_money_spend,
		cus_rep.platform_money as platform_money,
		cus_rep.count_order as count_order,
		cus_rep.avg_price_order as avg_price_order,
		cus_rep.median_time_order_completed as median_time_order_completed,
		fc.favorite_category as favorite_category, 
		fm.favorite_master_id as favorite_master_id,
		cus_rep.count_order_created as count_order_created,
		cus_rep.count_order_in_progress as count_order_in_progress,
		cus_rep.count_order_delivery as count_order_delivery,
		cus_rep.count_order_done as count_order_done,
		cus_rep.count_order_not_done as count_order_not_done,
		cus_rep.report_period as report_period
	from (
		select 
			customer_id,
			customer_name,
			customer_address, 
			customer_birthday,
			customer_email,
			sum(product_price) as customer_money_spend,
			sum(product_price * 0.1) as platform_money,
			count(distinct order_id) as count_order,
			avg(product_price) as avg_price_order,
			coalesce(percentile_cont(0.5) within group (order by diff_order_date) filter (where order_status = 'done'), 0) as median_time_order_completed,
			sum(case when order_status = 'created' then 1 else 0 end) as count_order_created,
			sum(case when order_status = 'in progress' then 1 else 0 end) as count_order_in_progress,
			sum(case when order_status = 'delivery' then 1 else 0 end) as count_order_delivery,
			sum(case when order_status = 'done' then 1 else 0 end) as count_order_done,
			sum(case when order_status <> 'done' then 1 else 0 end) as count_order_not_done,
			report_period
		from dwh_delta dd
		where exist_customer_id is null
		group by
			customer_id,
			customer_name,
			customer_address, 
			customer_birthday,
			customer_email,
			report_period
	) as cus_rep
	inner join (
	    select 
	    	customer_id,
	        report_period,
	        favorite_category
	    from (
	        select
	            customer_id,
	            report_period,
	            product_type as favorite_category,
	            count(*) as count_product,
	            row_number() over (partition by customer_id, report_period order by count(*) desc) as rank_by_category
	        from dwh_delta dd
	        group by
	            customer_id,
	            report_period,
	            product_type
	    ) as ifc
	    where rank_by_category = 1
	) as fc on cus_rep.customer_id = fc.customer_id and cus_rep.report_period = fc.report_period
	inner join (
	    select 
	    	customer_id,
            report_period,
            favorite_master_id
	    from (
	        select
	            customer_id,
	            report_period,
	            craftsman_id as favorite_master_id,
	            count(*) as count_order_for_master,
	            row_number() over (partition by customer_id, report_period order by count(*) desc) as rank_by_master
	        from dwh_delta dd
	        group by
	            customer_id,
	            report_period,
	            craftsman_id
	    ) as ifm
	    where rank_by_master = 1
	) as fm on cus_rep.customer_id = fm.customer_id and cus_rep.report_period = fm.report_period
	order by cus_rep.report_period 
),
dwh_delta_update_result as (
	select 
		cus_rep.customer_id as customer_id,
		cus_rep.customer_name as customer_name,
		cus_rep.customer_address as customer_address, 
		cus_rep.customer_birthday as customer_birthday,
		cus_rep.customer_email as customer_email,
		cus_rep.customer_money_spend as customer_money_spend,
		cus_rep.platform_money as platform_money,
		cus_rep.count_order as count_order,
		cus_rep.avg_price_order as avg_price_order,
		cus_rep.median_time_order_completed as median_time_order_completed,
		fc.favorite_category as favorite_category, 
		fm.favorite_master_id as favorite_master_id,
		cus_rep.count_order_created as count_order_created,
		cus_rep.count_order_in_progress as count_order_in_progress,
		cus_rep.count_order_delivery as count_order_delivery,
		cus_rep.count_order_done as count_order_done,
		cus_rep.count_order_not_done as count_order_not_done,
		cus_rep.report_period as report_period
	from (
		select 
			customer_id,
			customer_name,
			customer_address, 
			customer_birthday,
			customer_email,
			sum(product_price) as customer_money_spend,
			sum(product_price * 0.1) as platform_money,
			count(distinct order_id) as count_order,
			avg(product_price) as avg_price_order,
			coalesce(percentile_cont(0.5) within group (order by diff_order_date) filter (where order_status = 'done'), 0) as median_time_order_completed,
			sum(case when order_status = 'created' then 1 else 0 end) as count_order_created,
			sum(case when order_status = 'in progress' then 1 else 0 end) as count_order_in_progress,
			sum(case when order_status = 'delivery' then 1 else 0 end) as count_order_delivery,
			sum(case when order_status = 'done' then 1 else 0 end) as count_order_done,
			sum(case when order_status <> 'done' then 1 else 0 end) as count_order_not_done,
			report_period
		from (
			select 
				dc.customer_id as customer_id,
				dc.customer_name as customer_name,
				dc.customer_address as customer_address,
				dc.customer_birthday as customer_birthday,
				dc.customer_email as customer_email,
				fo.order_id AS order_id,
				dcf.craftsman_id as craftsman_id,
				dp.product_id AS product_id,
				dp.product_price AS product_price,
				dp.product_type AS product_type,
				(fo.order_completion_date - fo.order_created_date) AS diff_order_date, 
				fo.order_status as order_status,
				to_char(fo.order_created_date, 'yyyy-mm') AS report_period
			from dwh.f_order fo 
			inner join dwh.d_customer dc on fo.customer_id = dc.customer_id 
			inner join dwh.d_craftsman dcf on fo.craftsman_id = dcf.craftsman_id 
			inner join dwh.d_product dp on fo.product_id = dp.product_id
			inner join dwh_update_delta ud on fo.customer_id = ud.customer_id
		) as new_customer_data
		group by
			customer_id,
			customer_name,
			customer_address, 
			customer_birthday,
			customer_email,
			report_period
	) as cus_rep
	inner join (
	    select 
	    	customer_id,
            report_period,
            favorite_category
	    from (
	        select
	            customer_id,
	            report_period,
	            product_type as favorite_category,
	            count(*) as count_product,
	            row_number() over (partition by customer_id, report_period order by count(*) desc) as rank_by_category
	        from dwh_delta dd
	        group by
	            customer_id,
	            report_period,
	            product_type
	    ) as ifc
	    where rank_by_category = 1
	) as fc on cus_rep.customer_id = fc.customer_id and cus_rep.report_period = fc.report_period
	inner join (
	    select 
	    	customer_id,
            report_period,
            favorite_master_id
	    from (
	        select
	            customer_id,
	            report_period,
	            craftsman_id as favorite_master_id,
	            count(*) as count_order_for_master,
	            row_number() over (partition by customer_id, report_period order by count(*) desc) as rank_by_master
	        from dwh_delta dd
	        group by
	            customer_id,
	            report_period,
	            craftsman_id
	    ) as ifm
	    where rank_by_master = 1
	) as fm on cus_rep.customer_id = fm.customer_id and cus_rep.report_period = fm.report_period
	order by cus_rep.report_period
),
insert_delta as (
	insert into dwh.customer_report_datamart (customer_id, customer_name, customer_address, customer_birthday, customer_email,
									customer_money_spend, platform_money, count_order, avg_price_order, median_time_order_completed,
									favorite_category, favorite_master_id, count_order_created, count_order_in_progress, count_order_delivery,
									count_order_done, count_order_not_done, report_period)
	select distinct
		customer_id,
		customer_name,
		customer_address, 
		customer_birthday,
		customer_email,
		customer_money_spend,
		platform_money,
		count_order,
		avg_price_order,
		median_time_order_completed,
		favorite_category, 
		favorite_master_id,
		count_order_created,
		count_order_in_progress,
		count_order_delivery,
		count_order_done,
		count_order_not_done,
		report_period
	from dwh_delta_insert_result
),
update_delta as (
	update dwh.customer_report_datamart crd set
		customer_name = dur.customer_name,
		customer_address = dur.customer_address, 
		customer_birthday = dur.customer_birthday,
		customer_email = dur.customer_email,
		customer_money_spend = dur.customer_money_spend,
		platform_money = dur.platform_money,
		count_order = dur.count_order,
		avg_price_order = dur.avg_price_order,
		median_time_order_completed = dur.median_time_order_completed,
		favorite_category = dur.favorite_category, 
		favorite_master_id = dur.favorite_master_id,
		count_order_created = dur.count_order_created,
		count_order_in_progress = dur.count_order_in_progress,
		count_order_delivery = dur.count_order_delivery,
		count_order_done = dur.count_order_done,
		count_order_not_done = dur.count_order_not_done,
		report_period = dur.report_period
	from dwh_delta_update_result as dur
	where crd.customer_id = dur.customer_id
		and crd.report_period = dur.report_period
),
insert_load_date as (
	insert into dwh.load_dates_customer_report_datamart (
		load_dttm
	)
	select coalesce(greatest(max(craftsman_load_dttm), max(customer_load_dttm), max(products_load_dttm)), now())  
	from dwh_delta
)
select 'increment datamart';
