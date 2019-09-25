import sys
import os
from random import random
from operator import add
import time
from pyspark.sql import SparkSession
import pyspark

#  SPARK_HOME '/opt/softs/spark-2.2.2-bin-hadoop2.7' 必须设置
# PYTHONPATH $SPARK_HOME/python 可以不加
# PYSPARK_PYTHON 设置环境变量/usr/bin/python3 如果
# sys.path.append('/opt/softs/spark-2.2.2-bin-hadoop2.7/python')
# sys.path.append("/opt/softs/spark-2.2.2-bin-hadoop2.7/python/lib/py4j-0.10.7-src.zip") 可以不加

if __name__ == "__main__":
    import findspark

    findspark.init()
    start = time.time()
    spark = SparkSession.builder.master("local").appName("PythonPi2").getOrCreate()
    # .config("spark.pyspark.driver.python", "/usr/bin/")\
    # print(os.environ['PYTHONPATH'])

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 1000000 * partitions


    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0


    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()
    end = time.time()
    print((end - start) * 1000)
    print(spark)
    conf = pyspark.SparkConf().setAll(
        [('spark.executor.memory', '3g'), ('spark.executor.cores', '8'), ('spark.cores.max', '24'), ('spark.driver.memory', '9g')])
    # sc = pyspark.SparkContext.getOrCreate()
    # sc.stop()
    sc = pyspark.SparkContext(appName="AugustinJob", master="local[*]", conf=conf).getOrCreate()
    spark = SparkSession(sc)
    df = spark.createDataFrame([(1, 144.5, 5.9, 33, 'M'),
                                (2, 167.2, 5.4, 45, 'M'),
                                (3, 124.1, 5.2, 23, 'F'),
                                (4, 144.5, 5.9, 33, 'M'),
                                (5, 133.2, 5.7, 54, 'F'),
                                (3, 124.1, 5.2, 23, 'F'),
                                (5, 129.2, 5.3, 42, 'M'),], ['id', 'weight', 'height', 'age', 'gender'])

    # 查看重复记录
    # 1.首先删除完全一样的记录
    df2 = df.dropDuplicates()
    # 2.其次，关键字段值完全一模一样的记录（在这个例子中，是指除了id之外的列一模一样）
    # 删除某些字段值完全一样的重复记录，subset参数定义这些字段
    df3 = df2.dropDuplicates(subset=[c for c in df2.columns if c != 'id'])
    # 3.有意义的重复记录去重之后，再看某个无意义字段的值是否有重复（在这个例子中，是看id是否重复）
    # 查看某一列是否有重复值
    import pyspark.sql.functions as fn

    df3.agg(fn.count('id').alias('id_count'), fn.countDistinct('id').alias('distinct_id_count')).collect()
    # 4.对于id这种无意义的列重复，添加另外一列自增 这个就是看下有多少条记录，去重后有多少数据，

    df4 = df3.withColumn('new_id', fn.monotonically_increasing_id()).show()
    df_miss = spark.createDataFrame([
        (1, 143.5, 5.6, 28, 'M', 100000),
        (2, 167.2, 5.4, 45, 'M', None),
        (3, None, 5.2, None, None, None),
        (4, 144.5, 5.9, 33, 'M', None),
        (5, 133.2, 5.7, 54, 'F', None),
        (6, 124.1, 5.2, None, 'F', None),
        (7, 129.2, 5.3, 42, 'M', 76000), ], ['id', 'weight', 'height', 'age', 'gender', 'income'])

    # 1.计算每条记录的缺失值情况

    df_miss.rdd.map(lambda row: (row['id'], sum([c == None for c in row]))).collect()
    [(1, 0), (2, 1), (3, 4), (4, 1), (5, 1), (6, 2), (7, 0)]

    # 2.计算各列的确实情况百分比
    df_miss.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in df_miss.columns]).show()

    # 3、删除缺失值过于严重的列
    # 其实是先建一个DF，不要缺失值的列
    df_miss_no_income = df_miss.select([
        c for c in df_miss.columns if c != 'income'
    ])

    # 4、按照缺失值删除行（threshold是根据一行记录中，确实字段的百分比的定义）
    df_miss_no_income.dropna(thresh=3).show()

    # 5、填充缺失值，可以用fillna来填充缺失值，
    # 对于bool类型、或者分类类型，可以为缺失值单独设置一个类型，missing
    # 对于数值类型，可以用均值或者中位数等填充

    # fillna可以接收两种类型的参数：
    # 一个数字、字符串，这时整个DataSet中所有的缺失值都会被填充为相同的值。
    # 也可以接收一个字典｛列名：值｝这样

    # 先计算均值，并组织成一个字典
    means = df_miss_no_income.agg(*[fn.mean(c).alias(c) for c in df_miss_no_income.columns if c != 'gender']).toPandas().to_dict('records')[
        0]
    # 然后添加其它的列
    means['gender'] = 'missing'

    df_miss_no_income.fillna(means).show()

    '''
    3、异常值处理
    '''
    df_outliers = spark.createDataFrame([
        (1, 143.5, 5.3, 28),
        (2, 154.2, 5.5, 45),
        (3, 342.3, 5.1, 99),
        (4, 144.5, 5.5, 33),
        (5, 133.2, 5.4, 54),
        (6, 124.1, 5.1, 21),
        (7, 129.2, 5.3, 42),
    ], ['id', 'weight', 'height', 'age'])

    # approxQuantile方法接收三个参数：参数1，列名；参数2：想要计算的分位点，可以是一个点，也可以是一个列表（0和1之间的小数），第三个参数是能容忍的误差，如果是0，代表百分百精确计算。

    cols = ['weight', 'height', 'age']

    bounds = {}
    print(df_outliers.columns)
    for col in cols:
        quantiles = df_outliers.approxQuantile(col, [0.25, 0.75], 0.05)
        IQR = quantiles[1] - quantiles[0]
        bounds[col] = [quantiles[0] - 1.5 * IQR, quantiles[1] + 1.5 * IQR]

        # >> > bounds
        # {'age': [-11.0, 93.0], 'height': [4.499999999999999, 6.1000000000000005], 'weight': [91.69999999999999, 191.7]}
        print(bounds)
        # 为异常值字段打标志
    outliers = df_outliers.select(*['id'] + [
        ((df_outliers[c] < bounds[c][0]) | (df_outliers[c] > bounds[c][1])).alias(c + '_o') for c in cols])
    outliers.show()
        #
        # +---+--------+--------+-----+
        # | id|weight_o|height_o|age_o|
        # +---+--------+--------+-----+
        # |  1|   false|   false|false|
        # |  2|   false|   false|false|
        # |  3|    true|   false| true|
        # |  4|   false|   false|false|
        # |  5|   false|   false|false|
        # |  6|   false|   false|false|
        # |  7|   false|   false|false|
        # +---+--------+--------+-----+

        # 再回头看看这些异常值的值，重新和原始数据关联

    df_outliers = df_outliers.join(outliers, on='id')
    df_outliers.filter('weight_o').select('id', 'weight').show()
        # +---+------+
        # | id|weight|
        # +---+------+
        # |  3| 342.3|
        # +---+------+

    df_outliers.filter('age_o').select('id', 'age').show()
        # +---+---+
        # | id|age|
        # +---+---+
        # |  3| 99|
        # +---+---+

    '''
    4.建立数据的印象
    groupby().count()：可以看到数据分布的均匀性
    '''
        # 数据描述：
    df_outliers.printSchema()
        # root
        #   |-- id: long (nullable = true)
        #   |-- weight: double (nullable = true)
        #   |-- height: double (nullable = true)
        #   |-- age: long (nullable = true)
        #   |-- weight_o: boolean (nullable = true)
        #   |-- height_o: boolean (nullable = true)
        #   |-- age_o: boolean (nullable = true)

    numerical = ['weight', 'height', 'age']
    desc = df_outliers.describe(numerical)
    desc.show()

        # 用agg自定义各种汇总指标的集合:可能的统计指标有
        # avg(), count(), countDistinct(), first(), kurtosis(),
        # max(), mean(), min(), skewness(), stddev(), stddev_pop(),
        # stddev_samp(), sum(), sumDistinct(), var_pop(), var_samp() and
        # variance()

        # 用法有两种：
    import pyspark.sql.functions as fn

    df3.agg(fn.count('id').alias('id_count'), fn.countDistinct('id').alias('distinct_id_count')).collect()

    df3.agg({'weight': 'skewness'}).show()

    # 相关系数：corr
    # 目前corr只能计算两列的pearson相关系数，比如  df.corr('balance', 'numTrans')
    # 而相关系数矩阵需要这么做
    n_numerical = len(numerical)
    corr = []
    for i in range(0, n_numerical):
        temp = [None] * i
        for j in range(i, n_numerical):
            temp.append(df3.corr(numerical[i], numerical[j]))
        corr.append(temp)

