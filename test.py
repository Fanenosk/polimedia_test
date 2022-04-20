t = {'id': 'string', 'name': 'string', 'price': 'int', 'sdate': 'timestamp', 'edate': 'timestamp', 'is_actual': 'int'}
t1 = {'id': 'string', 'name': 'string', 'price': 'int', 'sdate': 'timestamp', 'edate': 'timestamp', 'is_actual': 'int'}


print(list(map(lambda key, item: key + ' ' + item, t, t.values())))

