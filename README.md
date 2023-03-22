### This is my naive implementation of MapReduce, it was implemented and tested in Python version 3.10.6. Only standart python library is nescicery to use it. 

#### Methods in MapReduce:

* mapper - maps and shuffles, retruns shuffled map over a key

and several redcer methods: 

* counter_grouped - counts entries over keys

* aggregate_grouped - aggregates (min, max or sum) over values that corenspond to keys

* join - calculates cartesian product of two maps and joins them

* filter - selects single key, value pair form shuffled map 

* select - selects myltiple columns

* drop - removes multiple columns



#### Class Table is a wrapper class around MapReduce for convinience, sintax design was inspired by pySpark.SQL.

### This is my naive implementation of MapReduce, it was implemented and tested in Python version 3.10.6. Only standard python library is necessary to use it.

#### Methods in MapReduce:
* mapper - maps and shuffles, returns shuffled map over a key and several reducer methods:
* counter_grouped - counts entries over keys
* aggregate_grouped - aggregates (min, max or sum) over values that corenspond to keys
* join - calculates cartesian product of two maps and joins them
* filter - selects single key, value pair form shuffled map
* select - selects multiple columns
* drop - removes multiple columns


#### Class Table is a wrapper class around MapReduce for convenience, syntax design was inspired by pySpark.SQL.


## Task 1 
Having in mind data/clicks dataset with "date" column, count how many clicks there were for each date and write the results to data/total_clicks dataset with "date" and "count" columns.

1. ##### <span style="color:CornflowerBlue ">First, let's read data into a table.</span>

```
number_of_nodes = 1
path1 = '/home/filambr/Documents/vinted_homework/data/clicks/'
schema1 = {'date':str, 'user_id':int, 'click_target':str}
table1 = Table(folder_path = path1, schema = schema1, number_of_nodes = number_of_nodes) 
```
2. ##### <span style="color:CornflowerBlue ">Let's apply count_grouped_by method.</span>

```
daily_click = table1.count_groupe_by('date','click_target','daily_clicks')
daily_click.show(1000) # Big number is entered to show all the results since talbe isn't to big
```
output:

    {'date': '2017-12-20', 'daily_clicks': 13}
    {'date': '2017-12-19', 'daily_clicks': 8}
    {'date': '2017-12-21', 'daily_clicks': 7}
    {'date': '2017-12-10', 'daily_clicks': 4}
    {'date': '2017-12-12', 'daily_clicks': 12}
    {'date': '2017-12-17', 'daily_clicks': 12}
    {'date': '2017-12-13', 'daily_clicks': 9}
    {'date': '2017-12-14', 'daily_clicks': 10}
    {'date': '2017-12-16', 'daily_clicks': 10}
    {'date': '2017-12-11', 'daily_clicks': 4}
    {'date': '2017-12-15', 'daily_clicks': 6}
    {'date': '2017-12-18', 'daily_clicks': 5}

3. ##### <span style="color:CornflowerBlue ">Let's write final table a file in data/total_clicks.</span>
```
path_out = 'data/total_clicks/part-001.csv'
daily_click.write_to_csv(path_out) # this will also work if total_clicks folder is not present
```
## Task 2  

There are two datasets:

* data/users dataset with columns "id" and "country"
* data/clicks dataset with columns "date", "user_id" and "click_target"

We'd like to produce a new dataset called data/filtered_clicks that includes only those clicks that belong to users from Lithuania (country=LT).

1. ##### <span style="color:CornflowerBlue ">As before lets read data into a Tables.</span>

```
number_of_nodes = 2

path1 = '/home/filambr/Documents/vinted_homework/data/clicks/'
schema1 = {'date':str, 'user_id':int, 'click_target':str}
table1 = Table(folder_path = path1, schema = schema1,  number_of_nodes=number_of_nodes) 
print(f'Table from {path1}')
table1.show()

path2 = '/home/filambr/Documents/vinted_homework/data/users/'
schema2 = {'id': int, 'city':str,'country': str}
table2 = Table(folder_path = path2, schema = schema2, number_of_nodes=number_of_nodes) 
print(f'Table from {path2}')
table2.show()
```

2. ##### <span style="color:CornflowerBlue ">Lets join tables on 'user_id' and 'id'.</span>

```
joined_table = table1.join(table2, 'user_id', 'id')
joined_table.show()
```
output:

    {'date': '2017-12-20', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-19', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-21', 'user_id': 3, 'click_target': 'profile', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-12', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-17', 'user_id': 3, 'click_target': 'item', 'id': 3, 'city': 'None', 'country': 'LT'}

It so happened that the first 5 rows of the joined_table only contain 'LT' users, but joined_table does contain non 'LT' users.


3. ##### <span style="color:CornflowerBlue ">Since we are only interested in clicks from Lithuanian ('LT') users, let's apply a filter method to isolate 'LT' users.</span>

```
filttered_table = joined_table.filter('country', 'LT')
filttered_table.show()
```
output:

    {'date': '2017-12-20', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-19', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-21', 'user_id': 3, 'click_target': 'profile', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-12', 'user_id': 3, 'click_target': 'ad', 'id': 3, 'city': 'None', 'country': 'LT'}
    {'date': '2017-12-17', 'user_id': 3, 'click_target': 'item', 'id': 3, 'city': 'None', 'country': 'LT'}


4. ##### <span style="color:CornflowerBlue ">Finally lets write new table to 'data/filtered_clicks/part-001.csv'.</span>


```
filttered_table.write_to_csv('data/filtered_clicks/part-001.csv') 
```

More detailed examples together with implementations without using wrapper class 'Table' can be followed along in example.ipynb
