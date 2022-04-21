select * from (select 'week' as period_type,
to_char(date_trunc('week', prodazhi2.sale_date), 'YYYY.MM.DD') as start_date ,
to_char(date_trunc('week', prodazhi2.sale_date) + interval '1 week', 'YYYY.MM.DD') as end_date,
p2.fio as salesman_fio,
p3.fio as chif_fio,
sum(prodazhi2.quantity) as sales_count,
sum(prodazhi2.final_price) as sales_sum,
prodazhi2.item_id as max_overcharge_item,
max((round(prodazhi2.final_price/prodazhi2.quantity/prodazhi2.price::numeric,2)-1)*100) as max_overcharge_percent 
from 
(select * from (select * from prodazhi p
join tovary t on (p.item_id = t.id)
where t.is_actual = 1
union all select * from prodazhi p
join uslugi u on (p.item_id = u.id)
where u.is_actual = 1) as prodazhi1) as prodazhi2,
prodavtsy p2,
otdely o
join prodavtsy p3 on (o.dep_chif_id = p3.id)
where date_trunc('week', prodazhi2.sale_date) >= prodazhi2.sdate 
and date_trunc('week', prodazhi2.sale_date) <= prodazhi2.edate
and prodazhi2.salesman_id = p2.id
and p2.department_id = o.department_id
group by prodazhi2.sale_date, p2.fio, p3.fio, prodazhi2.item_id
union all select 'month' as period_type,
to_char(date_trunc('month', prodazhi2.sale_date), 'YYYY.MM.DD') as start_date ,
to_char(date_trunc('month', prodazhi2.sale_date) + interval '1 month', 'YYYY.MM.DD') as end_date,
p2.fio as salesman_fio,
p3.fio as chif_fio,
sum(prodazhi2.quantity) as sales_count,
sum(prodazhi2.final_price) as sales_sum,
prodazhi2.item_id as max_overcharge_item,
max((round(prodazhi2.final_price/prodazhi2.quantity/prodazhi2.price::numeric,2)-1)*100) as max_overcharge_percent 
from 
(select * from (select * from prodazhi p
join tovary t on (p.item_id = t.id)
where t.is_actual = 1
union all select * from prodazhi p
join uslugi u on (p.item_id = u.id)
where u.is_actual = 1) as prodazhi1) as prodazhi2,
prodavtsy p2,
otdely o
join prodavtsy p3 on (o.dep_chif_id = p3.id)
where date_trunc('month', prodazhi2.sale_date) >= prodazhi2.sdate 
and date_trunc('month', prodazhi2.sale_date) <= prodazhi2.edate
and prodazhi2.salesman_id = p2.id
and p2.department_id = o.department_id
group by prodazhi2.sale_date, p2.fio, p3.fio, prodazhi2.item_id) as result_sql
order by result_sql.salesman_fio, result_sql.start_date