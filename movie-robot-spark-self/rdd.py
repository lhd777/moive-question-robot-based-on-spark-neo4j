from itertools import *
import io
from collections import Iterable,defaultdict
from multiprocessing import Pool
from functools import reduce
import cloudpickle
import pickle
from cache_manager import *
import os


class Partition:
    def __init__(self, x, idx):
        self.index = idx
        self._x = list(x)

    def get(self):
        return self._x

    def hashCode(self):
        return self.index


class RDD:
    def __init__(self, partitions, ctx):
        self._p = list(partitions)
        self.ctx = ctx
        self.name = None
        self._rdd_id = ctx.newRddId()

    def cache(self):
        return PersistedRDD(self)

    def compute(self, split, task_context):
        return split.get()
    
    def collect(self):
        return self.ctx.runJob(self, unit_map, resultHandler=unit_collect, allowLocal=True,)

    def first(self):
        return self.ctx.runJob(
            self,
            lambda tc, iterable: iterable,
            allowLocal=True,
            resultHandler=lambda l: next(chain.from_iterable(l)),
        )
    
    def flatMap(self, f, preservesPartitioning=True):
        return MapPartitionedRDD(self, lambda tc, i, x: (e for xx in x for e in f(xx)), preservesPartitioning=preservesPartitioning)
    
    def groupByKey(self, numPartitions=None):
        if not numPartitions:
            numPartitions=len(self._p)
        r=defaultdict(list)
        for k,v in self.collect():
            r[k].append(v)
        return self.ctx.parallelize(r.items(), numPartitions)
    
    def join(self, other, numPartitions=None):
        if not numPartitions:
            numPartitions=len(self._p)
        d1=dict(self.collect())
        d2=dict(other.collect())
        k=set(d1.keys())&set(d2.keys())
        return self.ctx.parallelize(((x, (d1[x], d2[x]))for x in k), numPartitions)
    
    def Map(self, f):
        return MapPartitionedRDD(self, lambda tc, i, x: (f(e) for e in x), preservesPartitioning=True)
    
    def MapPartition(self, f):
        return MapPartitionedRDD(self, lambda tc, i, x: f(x))
    
    def MapValues(self, f):
        return MapPartitionedRDD(self, lambda tc, i, x: ((e[0], f(e[1])) for e in x))
    
    def partitions(self):
        return self._p
    
    def reduceByKey(self, f, numPartitions=None):
        return self.groupByKey(numPartitions).MapValues(lambda x: reduce(f, x))
    
    def saveAsPicklefile(self, filename):
        def _map(filename, obj):
            stream = io.BytesIO()
            cloudpickle.dump(obj, stream)
            stream.seek(0)
            with io.open(filename, 'wb') as f:
                for c in stream:
                    f.write(c)
        _map(filename, self.collect())
        return self
    
    def saveAsTextfile(self, filename):
        def to_stringio(data):
            stringio = io.StringIO()
            for line in data:
                stringio.write('{}\n'.format(line))
            stringio.seek(0)
            return stringio
        with io.open(filename, 'wb') as f:
            for c in io.BytesIO(to_stringio(self.collect()).read().encode('utf8')):
                f.write(c)
        return self
    
    def sortBy(self, keyfun, ascending=True, numPartitions=None):
        if not numPartitions:
            numPartitions=len(self._p)
        return self.ctx.parallelize(sorted(self.collect(), key=keyfun, reverse=not ascending), numPartitions)
    
    def summation(self):
        return self.ctx.runJob(self, lambda tc, i: sum(i), resultHandler=sum)
        
    
class MapPartitionedRDD(RDD):
    def __init__(self, prev, f, preservesPartitioning=False):
        RDD.__init__(self, prev.partitions(), prev.ctx)
        self.prev = prev
        self.f = f
        self.preservesPartitioning = preservesPartitioning

    def compute(self, split, task_context):
        return self.f(task_context, split.index,self.prev.compute(split, task_context._create_child()))

    def partitions(self):
        return self.prev.partitions()


class PersistedRDD(RDD):
    def __init__(self, prev):
        RDD.__init__(self, prev.partitions(), prev.ctx)
        self.prev = prev

    def compute(self, split, task_context):
        if self._rdd_id is None or split.index is None:
            cid = None
        else:
            cid = (self._rdd_id, split.index)

        if not task_context.cache_manager.has(cid):
            data = list(self.prev.compute(split, task_context._create_child()))
            task_context.cache_manager.add(cid, data)
        else:
            data = task_context.cache_manager.get(cid)

        return iter(data)


class TaskContext:
    def __init__(self, cache_manager, stage_id=0, partition_id=0):
        self.stage_id = stage_id
        self.cache_manager = cache_manager
        self.partition_id = partition_id

    def _create_child(self):
        return TaskContext(self.cache_manager, stage_id=self.stage_id + 1, partition_id=self.partition_id)


def _run_task(task_context, rdd, func, partition):
        return func(task_context,rdd.compute(partition,task_context))  


def unit_map(task_context, elements):
    if isinstance(elements, Iterable):
        return list(elements)
    else:
        a = []
        a.append(elements)
        return a


def unit_collect(l):
    return [x for p in l for x in p]


def runJob_map(i):  
    (serialized_func_rdd, serialized_task_context,serialized_data) = i
    func, rdd = pickle.loads(serialized_func_rdd)
    partition = pickle.loads(serialized_data)
    task_context = pickle.loads(serialized_task_context)
    result = _run_task(task_context, rdd, func, partition)
    return cloudpickle.dumps(result)


class Context(object):
    __last_rdd_id = 0

    def __init__(self, serializer=None, deserializer=None,
                 data_serializer=None, data_deserializer=None,
                 max_retries=3, cache_manager=None):
        self.max_retries = max_retries

        self._cache_manager = cache_manager or CacheManager()
        self._serializer = serializer
        self._deserializer = deserializer
        self._data_serializer = data_serializer
        self._data_deserializer = data_deserializer

    def runJob(self, rdd, func, partitions=None, resultHandler=None, allowLocal=None):
        if not partitions:
            partitions = rdd.partitions()

        def _runJob_local(self, rdd, func, partitions):
            for partition in partitions:
                task_context = TaskContext(stage_id=0, cache_manager=self._cache_manager, partition_id=partition.index)
                yield _run_task(task_context, rdd, func, partition)

        def _runJob_multi(self, rdd, func, partitions=None, resultHandler=None):
            if not partitions:
                partitions=rdd.partitions()
            pool=Pool(len(partitions))
            serialized_func_rdd = cloudpickle.dumps((func, rdd))

            def prepare(partition):
                cm_clone = self._cache_manager.clone_contains(
                    lambda i: i[1] == partition.index)
                task_context = TaskContext(stage_id=0, cache_manager=cm_clone, partition_id=partition.index)
                serialized_task_context = cloudpickle.dumps(task_context)
                serialized_partition=cloudpickle.dumps(partition)
                return (serialized_func_rdd,serialized_task_context,serialized_partition)
            prepared_partitions=(prepare(partition) for partition in partitions)
            for d in pool.map(runJob_map, prepared_partitions):
                map_result=pickle.loads(d)
                yield map_result
            pool.close()
        if allowLocal:
            map_result=_runJob_local(self, rdd, func, partitions)
        else:
            map_result=_runJob_multi(self, rdd, func, partitions)
        result=(resultHandler(map_result) if resultHandler is not None else list(map_result))
        return result

    def newRddId(self):
        Context.__last_rdd_id += 1
        return Context.__last_rdd_id
          
    def parallelize(self, x, numPartitions):
        if numPartitions==1:
            return RDD([Partition(x, 0)], self)
        i1,i2=tee(x)
        len_x=sum(1 for _ in i1)

        def partitioned():
            for i in range(numPartitions):
                start = int(i * len_x / numPartitions)
                end = int((i + 1) * len_x /numPartitions)
                if i + 1 == numPartitions:
                    end += 1
                yield islice(i2, end-start)
        return RDD((Partition(data, i) for i, data in enumerate(partitioned())), self)
    
    def pickleFile(self,filename):
        a=filename.split(',')
        rdd_filenames = self.parallelize(a, len(a))

        def load_pickle(filename):
            with io.open(filename, 'rb') as f:
                return io.BytesIO(f.read())
        return rdd_filenames.flatMap(lambda filename:pickle.load(load_pickle(filename)))
    
    def textFile(self, filename):
        a=filename.split(',')
        rdd_filenames = self.parallelize(a,len(a))
        def load_text(filename,encoding='utf8'):
            with io.open(filename, 'r', encoding=encoding) as f:
                return io.StringIO(f.read())
        return rdd_filenames.flatMap(lambda filename:load_text(filename).read().splitlines())

