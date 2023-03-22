from collections import defaultdict
import os
import csv
# from multiprocessing import Process, Pool, Manager
from functools import partial
from concurrent.futures import ThreadPoolExecutor as Executor
import itertools 

def int_(x):
    if x is None:
        return None
    else:
        return int(x)
    
def float_(x):
    if x is None:
        return None
    else:
        return float(x)
    

def csv_reader(path, schema):
    if os.path.isdir(path):
        files = os.listdir(path)
        file_paths = []
        for fl in files:
            file_path = os.path.join(path, fl)
            file_paths.append(file_path)
    else:
        file_paths = [path]


    schema_keys = [k for k in schema]
    decorators = {str: str, int: int_, float: float_}
    schema_types = [decorators[schema[k]] for k in schema]

    contents_of_files = []
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            dict_reader = csv.DictReader(f)
            list_of_dict = list(dict_reader)
            for dict_ in list_of_dict:
                for k in schema_keys:
                    if k in dict_:
                        'do_nothing'
                    else:
                        dict_[k] = None
                to_append = {k:t(dict_[k]) for k,t in zip(schema_keys, schema_types)}
                contents_of_files.append(to_append)

    return contents_of_files

def select(data, keys):
    return {k:data[k] for k in keys} 

def drop(data, keys):
    [data.pop(k) for k in keys]
    return data

def mapper(data, key):
    return data[key],data


def shuffler(mapped):
    shuffled = defaultdict(list)
    for key, value in mapped:
        shuffled[key].append(value)
    return shuffled

def aggregate_grouped(key, shuffled, group_by_key, value_to_aggregate, agg_type='sum',  alias=None):
    if alias is None:
        alias = value_to_aggregate
    agg_types = {'min':min, 'max':max, 'sum':sum}
    aggregator = agg_types[agg_type]
    group = shuffled[key]
    values = []
    for entrie in group:
        values.append(entrie[value_to_aggregate])

    return {group_by_key:key, alias:aggregator(values)}

def counter(key, shuffled, group_by_key, value_to_aggregate, alias=None):
    if alias is None:
        alias = value_to_aggregate
    return {group_by_key:key, alias:len(shuffled[key])}

def cartesian_product(key, self_shuffled, other_shuffled):
    return list(itertools.product(self_shuffled[key], other_shuffled[key]))

def join(product):
    return {**product[0], **product[1]}

def async_map(executor, mapper, data):
    futures = []
    for datum in data:
        futures.append(executor.submit(mapper, datum)) 
    return futures

class MapReduce:
    def __init__(self, number_of_nodes):
        self.executor = Executor(max_workers=number_of_nodes)



    # def aggregate_grouped(self, shuffled, key):
    #     keys = shuffled.keys()
    #     futures = async_map(self.executor, partial(aggregate_grouped,
    #                                                value_to_aggregate=value_to_aggregate,
    #                                                shuffled=shuffled, 
    #                                                , alias=alias), keys)

    def mapper(self, data, key):
        futures = async_map(self.executor, partial(mapper, key=key), data)
        mapped = map(lambda f: f.result(), futures)  
        shuffled = shuffler(mapped)
        return shuffled
    
    def aggregate_grouped(self, shuffled, group_by_key, value_to_aggregate, alias, agg_type):
        if alias is None:
            alias = value_to_aggregate
        keys = shuffled.keys()
        futures = async_map(self.executor, partial(aggregate_grouped, shuffled=shuffled, 
                                                    group_by_key=group_by_key, 
                                                    value_to_aggregate=value_to_aggregate,
                                                    alias=alias, agg_type=agg_type), keys)
        aggregated = map(lambda f: f.result(), futures)
        return list(aggregated)


    def counter_grouped(self, shuffled, group_by_key, value_to_aggregate, alias=None):
        if alias is None:
            alias = value_to_aggregate
        keys = shuffled.keys()
        futures = async_map(self.executor, partial(counter, shuffled=shuffled, 
                                                    group_by_key=group_by_key, 
                                                    value_to_aggregate=value_to_aggregate
                                                    , alias=alias), keys)
        counted = map(lambda f: f.result(), futures)
        return list(counted)
    
    def join(self, self_shuffled, other_shuffled):
        keys = self_shuffled.keys()
        futures = async_map(self.executor, partial(cartesian_product, self_shuffled=self_shuffled, other_shuffled=other_shuffled), keys)
        product = sum(map(lambda f: f.result(), futures), [])
        join_futures = async_map(self.executor, join, product)
        results = map(lambda f: f.result(), join_futures)
        return list(results)
    
    def select(self, data, keys):
        futures = async_map(self.executor, partial(select, keys=keys), data)
        results = map(lambda f: f.result(), futures)
        return list(results)
    
    def drop(self, data, keys):
        futures = async_map(self.executor, partial(drop, keys=keys), data)
        results = map(lambda f: f.result(), futures)
        return list(results)

    def filter(self, key, shuffled):
        return shuffled[key]



class Table():
    def __init__(self, folder_path = None, file_path = None, data = None, reader=csv_reader, schema=None, number_of_nodes=2):
        path = [path for path in [folder_path, file_path] if path != None]
        self.number_of_nodes = number_of_nodes

        if len(path) == 2:
            raise ValueError('Path to a table should be a single path to folder where separate files corespoding to the same table are stored "folder_path" OR \
        path to a signle file "file path", NOT both.')
        elif len(path) == 1:
            self.path = path[0]
            self.reader = reader  
            self.schema = schema
            self.data = self.reader(self.path, self.schema)
            
        elif data is not None:
            self.data = data
        else:
            raise ValueError('Nither "folder_path", nor "file_path" or "data" were given, you must imput somtheing')

        self.mapreduce = MapReduce(self.number_of_nodes)
            
    def join(self, other_table, self_key, other_key):
        # # with Pool() as pool:
        # if self.number_of_nodes > 1:
        #     mapped_table = self.pool.map(partial(mapper, key=self_key), self.data)
        #     mapped_other_table = self.pool.map(partial(mapper, key=other_key), other_table.data)
        
        #     # with Pool(self.number_of_nodes) as pool:
        #     shuffled_table = self.pool.map(shuffler, mapped_table)
        #     shuffled_other_table = self.pool.map(shuffler, mapped_other_table )

        #     merged_shuffled_table = merge_dicts(shuffled_table)
        #     merged_other_shuffled_table = merge_dicts(shuffled_other_table)
        # else: 
        #     mapped_table = mapper(self.data, self_key)
        #     mapped_other_table = mapper(other_table.data, other_key)

        #     merged_shuffled_table = shuffler(mapped_table)
        #     merged_other_shuffled_table = shuffler(mapped_other_table)
        # # reduce block below

        # joined_data = []
        # for key1 in merged_shuffled_table:
        #     for value1 in merged_shuffled_table[key1]:
        #         for value2 in merged_other_shuffled_table[key1]:
        #             joined_data.append({**value1,**value2})
        # joined_data = partition(joined_data, self.number_of_nodes)
        mapped_self = self.mapreduce.mapper(self.data, self_key)
        mapped_other = self.mapreduce.mapper(other_table.data, other_key)
        joined = self.mapreduce.join(mapped_self , mapped_other)
        return Table(data = joined, number_of_nodes=self.number_of_nodes)


    def filter(self, key, value):
        # new_table = []
        # for index in range(self.number_of_nodes):
        #     temp_table = []
        #     for raw in self.data[index]:
        #         if condition(raw[key], value):
        #             temp_table.append(raw)
        #         else:
        #             'do nothing'
        #     new_table.append(temp_table)
        mapped = self.mapreduce.mapper(self.data, key)
        filtered = self.mapreduce.filter(value, mapped)
        return Table(data = filtered, number_of_nodes=self.number_of_nodes)
    
    def select(self, keys):
        selected = self.mapreduce.select(self.data, keys)
        return Table(data = selected, number_of_nodes=self.number_of_nodes)
    
    def drop(self, keys):
        droped = self.mapreduce.drop(self.data, keys)
        return Table(data = droped, number_of_nodes=self.number_of_nodes)
    
    def count_groupe_by(self, key, value , alias=None):
        if alias is None:
            alias = value
        mapped = self.mapreduce.mapper(self.data, key = key)
        counted_gouped = self.mapreduce.counter_grouped(mapped, group_by_key=key, value_to_aggregate=value, alias=alias)
        # if alias is None:
        #     alias = key

        # if self.number_of_nodes >1:
        #     mapped_table = self.pool.map(partial(mapper, key=key), self.data)
        #     shuffled_table = self.pool.map(shuffler, mapped_table)
        #     merged_shuffled_table = merge_dicts(shuffled_table)

        #     # reduce block below

        #     counted_grouped = []
        #     for k, v in merged_shuffled_table.items():
        #         counted_grouped.append({key:k, alias: len(v)})

        #     counted_grouped = partition(counted_grouped, self.number_of_nodes)
        #     return Table(data = counted_grouped)
        # else:

        #     mapped_table = mapper(self.data, key)
        #     shufled_table = shuffler(mapped_table)

        #     # reduce block below

        #     counted_grouped = []
        #     for k, v in shufled_table.items():
        #         counted_grouped.append({key:k, alias: len(v)})
        return Table(data = counted_gouped, number_of_nodes=self.number_of_nodes)
    
    def agg_groupe_by(self, key, value , alias=None, agg_type='sum'):
        if alias is None:
            alias = value

        mapped = self.mapreduce.mapper(self.data, key = key)
        aggregated_grouped = self.mapreduce.aggregate_grouped(mapped, group_by_key=key, value_to_aggregate=value, alias=alias, agg_type=agg_type)
        return Table(data = aggregated_grouped, number_of_nodes=self.number_of_nodes)
    
    def count(self, key, alias=None):
        # if alias is None:
        #     alias = key
        # if self.number_of_nodes > 1:
        #     mapped_table = self.pool.map(partial(mapper, key=key), self.data)
        #     countall = sum([len(x) for x in mapped_table])
        #     return {alias: countall}
        # else:
        mapped = self.mapreduce.mapper(self.data, key)
        return {alias: len(mapped)}
    

    # def sort_by(self, key):
    #     ''' sorts by key values in not particular order i.e. not in alphabetical 
    #         or asceding or descedning order, just goups rows with the same key value
    #     '''
    #     if self.number_of_nodes>1:
    #         mapped_table = self.pool.map(partial(mapper, key=key), self.data)
    #         shuffled_table = self.pool.map(shuffler, mapped_table)
    #         merged_shuffled_table = merge_dicts(shuffled_table)

    #         # reduce block below

    #         new_table = []
    #         for key in merged_shuffled_table:
    #             for row in merged_shuffled_table[key]:
    #                 new_table.append(row)
            
    #         new_table = partition(new_table, self.number_of_nodes)
    #     else: 
    #         mapped_table = mapper(self.data, key)
    #         merged_shuffled_table = shuffler(mapped_table)

    #         # reduce block below

    #         new_table = []
    #         for key in merged_shuffled_table:
    #             for row in merged_shuffled_table[key]:
    #                 new_table.append(row)

    #         new_table = partition(new_table, self.number_of_nodes)
    #     return Table(data = new_table, number_of_nodes=self.number_of_nodes)
    
    def show(self, number_to_show=5):
        for i, row in enumerate(self.data):
            if i+1<=number_to_show:
                print(row)
            else:
                break
        return None
    
    # def drop(self, drop_keys):
    #     if self.number_of_nodes >1:
    #         droped_data = []
    #         for index in range(self.number_of_nodes):
    #             for row in self.data[index]:
    #                 [row.pop(k) for k in drop_keys]
    #                 droped_data.append(row)
    #         droped_data = partition(droped_data, self.number_of_nodes)
    #     else:
    #         droped_data = []
    #         for row in self.data:
    #             [row.pop(k) for k in drop_keys]
    #             droped_data.append(row)

    #     self.data = droped_data
    #     return self
    
    def write_to_csv(self, output_path):
        folder, file_name = os.path.split(output_path)
        if not os.path.isdir(folder):
            os.mkdir(folder)
        


        to_csv = self.data
        keys = to_csv[0].keys()

        with open(output_path, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(to_csv)

        return None