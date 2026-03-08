drop table if exists dwh.customer_report_datamart;
create table if not exists dwh.customer_report_datamart (
	id bigint generated always as identity not null,
	customer_id bigint not null,
	customer_name varchar not null,
	customer_address varchar not null,
	customer_birthday date not null,
	customer_email varchar not null,
	customer_money_spend numeric(15, 2) not null,
	platform_money numeric(15, 2) not null,
	count_order bigint not null,
	avg_price_order numeric(10, 2) not null,
	median_time_order_completed numeric(10, 1) not null,
	favorite_category varchar not null,
	favorite_master_id bigint not null,
	count_order_created bigint not null,
	count_order_in_progress bigint not null,
	count_order_delivery bigint not null,
	count_order_done bigint not null,
	count_order_not_done bigint not null,
	report_period varchar not null,
	constraint customer_report_datamart_pk primary key (id)
);

comment on table dwh.customer_report_datamart is 'витрина с агрегированными метриками по клиентам за отчетный период';
comment on column dwh.customer_report_datamart.id is 'первичный ключ записи витрины';
comment on column dwh.customer_report_datamart.customer_id is 'идентификатор клиента';
comment on column dwh.customer_report_datamart.customer_name is 'имя клиента';
comment on column dwh.customer_report_datamart.customer_address is 'адрес клиента';
comment on column dwh.customer_report_datamart.customer_birthday is 'дата рождения клиента';
comment on column dwh.customer_report_datamart.customer_email is 'email клиента';
comment on column dwh.customer_report_datamart.customer_money_spend is 'общая сумма расходов клиента за отчетный период';
comment on column dwh.customer_report_datamart.platform_money is 'сумма дохода платформы по заказам клиента за отчетный период';
comment on column dwh.customer_report_datamart.count_order is 'общее количество заказов клиента за отчетный период';
comment on column dwh.customer_report_datamart.avg_price_order is 'средняя стоимость заказа клиента за отчетный период';
comment on column dwh.customer_report_datamart.median_time_order_completed is 'медианное время завершения заказа в днях';
comment on column dwh.customer_report_datamart.favorite_category is 'любимая категория клиента по количеству заказов';
comment on column dwh.customer_report_datamart.favorite_master_id is 'идентификатор мастера, у которого клиент заказывает чаще всего';
comment on column dwh.customer_report_datamart.count_order_created is 'количество заказов клиента в статусе created';
comment on column dwh.customer_report_datamart.count_order_in_progress is 'количество заказов клиента в статусе in progress';
comment on column dwh.customer_report_datamart.count_order_delivery is 'количество заказов клиента в статусе delivery';
comment on column dwh.customer_report_datamart.count_order_done is 'количество заказов клиента в статусе done';
comment on column dwh.customer_report_datamart.count_order_not_done is 'количество заказов клиента в статусах, отличных от done';
comment on column dwh.customer_report_datamart.report_period is 'отчетный период формирования витрины в "yyyy-mm"';