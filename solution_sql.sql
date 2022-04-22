select * from 
	(select DISTINCT on (table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id) 'month' as period_type,
	table_prodazhi.start_date, table_prodazhi.end_date, prodavtsy.fio as salesman_fio , shef.fio as chif_fio , table_aggr.sales_count,
	table_aggr.sales_sum, table_prodazhi.item_id,table_prodazhi.max_overcharge_percent from (
		(select to_char(date_trunc('month', p.sale_date), 'YYYY.MM.DD') as start_date, 
			to_char(date_trunc('month', p.sale_date) + interval '1 month','YYYY.MM.DD') as end_date, *, p.final_price/p.quantity - t.price as max_overcharge,
			(round(p.final_price/p.quantity/t.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p
		join tovary t on (p.item_id = t.id)
			where p.item_id = t.id 
			and t.is_actual = 1
			and p.sale_date >= t.sdate
			and p.sale_date <= t.edate
		union all select to_char(date_trunc('month', p.sale_date),'YYYY.MM.DD') as start_date,
			to_char(date_trunc('month', p.sale_date) + interval '1 month','YYYY.MM.DD') as end_date,*, p.final_price/p.quantity - u.price as max_overcharge,
			(round(p.final_price/p.quantity/u.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p
		join uslugi u on (p.item_id = u.id)
			where p.item_id = u.id 
			and u.is_actual = 1
			and p.sale_date >= u.sdate
			and p.sale_date <= u.edate) as table_prodazhi
		join (
			select to_char(date_trunc('month', p2.sale_date),'YYYY.MM.DD') as start_date, 
			to_char(date_trunc('month', p2.sale_date) + interval '1 month','YYYY.MM.DD') as end_date,
			salesman_id,
			sum(p2.quantity) as sales_count,
			sum(p2.final_price) as sales_sum 
			from prodazhi p2
			group by date_trunc('month', p2.sale_date), p2.salesman_id) as table_aggr
			on (table_aggr.salesman_id = table_prodazhi.salesman_id) 
			and (table_aggr.start_date = table_prodazhi.start_date) 
			and (table_aggr.end_date = table_prodazhi.end_date)),
		prodavtsy
		join otdely on (prodavtsy.department_id  = otdely.department_id)
		join prodavtsy shef on (otdely.dep_chif_id = shef.id)
			where table_prodazhi.salesman_id = prodavtsy.id
	order by table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id, table_prodazhi.max_overcharge desc) as result_table
union all
select * from 
	(select DISTINCT on (table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id) 'week' as period_type,
	table_prodazhi.start_date, table_prodazhi.end_date, prodavtsy.fio as salesman_fio , shef.fio as chif_fio , table_aggr.sales_count,
	table_aggr.sales_sum, table_prodazhi.item_id,table_prodazhi.max_overcharge_percent from (
		(select to_char(date_trunc('week', p.sale_date), 'YYYY.MM.DD') as start_date, 
			to_char(date_trunc('week', p.sale_date) + interval '1 week','YYYY.MM.DD') as end_date, *, p.final_price/p.quantity - t.price as max_overcharge,
			(round(p.final_price/p.quantity/t.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p
		join tovary t on (p.item_id = t.id)
			where p.item_id = t.id 
			and t.is_actual = 1
			and p.sale_date >= t.sdate
			and p.sale_date <= t.edate
		union all select to_char(date_trunc('week', p.sale_date),'YYYY.MM.DD') as start_date,
			to_char(date_trunc('week', p.sale_date) + interval '1 week','YYYY.MM.DD') as end_date,*, p.final_price/p.quantity - u.price as max_overcharge,
			(round(p.final_price/p.quantity/u.price::numeric,2)-1)*100 as max_overcharge_percent from prodazhi p
		join uslugi u on (p.item_id = u.id)
			where p.item_id = u.id 
			and u.is_actual = 1
			and p.sale_date >= u.sdate
			and p.sale_date <= u.edate) as table_prodazhi
		join (
			select to_char(date_trunc('week', p2.sale_date),'YYYY.MM.DD') as start_date, 
			to_char(date_trunc('week', p2.sale_date) + interval '1 week','YYYY.MM.DD') as end_date,
			salesman_id,
			sum(p2.quantity) as sales_count,
			sum(p2.final_price) as sales_sum 
			from prodazhi p2
			group by date_trunc('week', p2.sale_date), p2.salesman_id) as table_aggr
			on (table_aggr.salesman_id = table_prodazhi.salesman_id) 
			and (table_aggr.start_date = table_prodazhi.start_date) 
			and (table_aggr.end_date = table_prodazhi.end_date)),
		prodavtsy
		join otdely on (prodavtsy.department_id  = otdely.department_id)
		join prodavtsy shef on (otdely.dep_chif_id = shef.id)
			where table_prodazhi.salesman_id = prodavtsy.id
	order by table_prodazhi.start_date, table_prodazhi.end_date, table_prodazhi.salesman_id, table_prodazhi.max_overcharge desc) as result_table
order by salesman_fio, start_date