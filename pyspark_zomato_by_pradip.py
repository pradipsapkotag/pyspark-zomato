from zlib import Z_PARTIAL_FLUSH
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf,col,sum
from pyspark.sql.types import StringType
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id
from credentials import password


def convert(strr):
    if(strr == 'NEW'):
        return '0/5'
    else:
        return strr






class Zomato_Pyspark:
    
    
    
    
    
    def __init__(self):
        spark = SparkSession.builder.appName("Apache PySpark Final Project-Zomato")\
                                    .config('spark.jars', '/home/prasag/snap/dbeaver-ce/212/.local/share/DBeaverData/drivers/maven/maven-central/org.postgresql/postgresql-42.5.0.jar')\
                                    .getOrCreate()
        self.zomato_df = spark.read.csv('data/cleaned_zomato.csv',inferSchema=True,header=True)
        
        
        
        zomato_df = self.zomato_df
        convertUDF = udf(lambda string : convert(string),StringType())
        zomato_df = zomato_df.withColumn("rating", convertUDF(col('rating')))
        zomato_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task1_zomato_df',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        
        
        
        
        
        
    # Task 2: Suggest where one can open a new restaurant    
    def task2(self):
        zomato_df = self.zomato_df
        city_res_count = zomato_df.groupBy('location').count()
        city_res_count = city_res_count.sort('count')
        min_count = city_res_count.collect()[0][1]
        suggested_citis = city_res_count.filter(col('count') == min_count).select(col('location'))
        suggested_citis.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task2_suggested_cities',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    #Task 3: Compare the restaurant to whether it has the facility of an “online order” or not.
    def task3(self):
        zomato_df = self.zomato_df
        having_online_order = zomato_df.filter(zomato_df.online_order == 'Yes').select(col('name'))\
                                .withColumnRenamed('name','having_online_order')\
                                .withColumn("serial_no", monotonically_increasing_id())
        not_having_online_order = zomato_df.filter(zomato_df.online_order == 'No').select(col('name'))\
                                    .withColumnRenamed('name','not_having_online_order')\
                                    .withColumn("serial_no", monotonically_increasing_id())
        having_and_not_having = having_online_order.join(not_having_online_order,having_online_order.serial_no == not_having_online_order.serial_no,'left')\
                        .select(col('having_online_order'),col('not_having_online_order'))
        having_and_not_having.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task3_having_and_not_having',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    #Task 4 : List the top ten restaurants with the highest number of branches.
    def task4(self):
        zomato_df = self.zomato_df
        top_ten_testutants = zomato_df.groupBy('name').count()
        top_ten_testutants = top_ten_testutants.sort(col('count').desc())
        top_ten_testutants = top_ten_testutants.limit(10)
        top_ten_testutants.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task4_top_ten_resturants',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    
    
    # 5: List restaurants that are either cafes or Quick Bites
    def task5(self):
        zomato_df = self.zomato_df
        cafes_or_quick_bites = zomato_df.where(col('type').contains('cafe')\
                                       | col('type').contains('Cafe')\
                                       | col('type').contains('Quick Bites')\
                                       | col('type').contains('Quick bites')\
                                       | col('type').contains('quick bites')).select(col('name'),col('type'))
        cafes_or_quick_bites.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task5_cafes_or_quick_bites',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    
    
    # Task 6: Count the number of restaurants that allows online orders and book table.
    def task6(self):
        zomato_df = self.zomato_df
        res_allows_online_and_book_table = zomato_df.filter((zomato_df.book_table=='Yes') & (zomato_df.online_order=='Yes'))\
                                            .select(col('name'),col('online_order'),col('book_table'))
        res_allows_online_and_book_table.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task6_res_allows_online_and_book_table',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    # Task 7: Get the number of restaurants in each city.
    def task7(self):
        zomato_df = self.zomato_df
        no_of_resturnats_by_city = zomato_df.groupby('location').count()
        no_of_resturnats_by_city = no_of_resturnats_by_city.withColumnRenamed('count','no_of_resturants')
        no_of_resturnats_by_city.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task7_no_of_resturnats_by_city',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
    
    
    
    
    
    
    
    # Task 8: Find the total no. of votes in each cities using window function.
    def Task8(self):
        zomato_df = self.zomato_df
        windowSpec = Window.partitionBy('location')
        total_votes_by_city = zomato_df.withColumn('total_votes',sum(col('votes')).over(windowSpec))\
                                .select(col('location'),col('total_votes')).dropDuplicates()
        total_votes_by_city.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/pyspark_zomato',\
                              driver='org.postgresql.Driver',\
                              dbtable='task8_total_votes_by_city',\
                              user='postgres',\
                              password=password).mode('overwrite').save()
        return None
    
    
    
        


if __name__ == "__main__":
    
    
    zomato = Zomato_Pyspark()
    
    
    zomato.task2()
    zomato.task3()
    zomato.task4()
    zomato.task5()
    zomato.task6()
    zomato.task7()
    zomato.Task8()
    
    