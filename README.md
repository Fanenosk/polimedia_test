Задание
Необходимо написать SQL запрос по следующей постановке задачи:

Для принятия решения о премировании продавцов необходимо провести анализ результатов их работы. Для этого необходимо провести агрегацию данных о продажах и представить информацию в виде таблицы следующего вида:

period_type - тип периода (month, week)

start_date - дата начала периода

end_date - дата окончания периода

salesman_fio - ФИО продавца

chif_fio - ФИО руководителя отдела

sales_count - количество сделок

sales_sum - сумма сделок

max_overcharge_item - товар или услуга с наибольшей наценкой за выбранный период

max_overcharge_percent - максимальный процент наценки за выбранный период

Агрегацию необходимо произвести по месяцам и неделям и сложить результаты в одну таблицу указав для каждого периода уровень периода, дату начала периода и дату окончания периода. В результирующей таблице должны присутствовать только периоды, по которым есть данные по продажам.

Данные вывести в отсортированном порядке по ФИО продавца и дате начала периода.