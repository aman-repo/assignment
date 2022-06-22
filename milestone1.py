from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('milestone_one').getOrCreate()
#reading all the data files & creating their views
df_navhistory = spark.read.option("header",True).csv("/Users/syedaman/Desktop/Data/NavHistory.csv")
df_navhistory.createOrReplaceTempView("nav_data")
df_amc = spark.read.option("header",True).csv("/Users/syedaman/Desktop/Data/AMC.csv")
df_amc.createOrReplaceTempView("amc")
df_fundcategory = spark.read.option("header",True).csv("/Users/syedaman/Desktop/Data/FundCategory.csv")
df_fundcategory.createOrReplaceTempView("fund_category")
df_mutualfund = spark.read.option("header",True).csv("/Users/syedaman/Desktop/Data/MutualFund.csv")
df_mutualfund.createOrReplaceTempView("mutualfund")

#rows having zero or invalid entry

df_null = spark.sql("select * from nav_data where nav = 0 ")
#df_null.show(2)
#cat = df_null.count()
#print(cat)

#validating & transforming the date field to yyyy-mm-dd

df_datefield_upadte = df_navhistory.withColumn("nav_date",col("nav_date").cast(DateType()))
#df_datefield_upadte.show(2)

#calculating exit load

df_exitload = df_navhistory.withColumn("Exit_Load",df_navhistory.nav*0.01)
df_exitload.createOrReplaceTempView("exitload_data")

#df_exitload.show(2)

#finding bad records
#df_columns = ["nav"]
#df = df_navhistory.select((when(isnan(c)|col(c).isNull(),c)).alias(c) for c in df_columns)
#df.show()

df_null.write.format("csv").option("header", True).save("/Users/syedaman/Desktop/Data/error_data/")

#Monthly average NAV, Repurchase & Sale Price for each scheme

#f_join = spark.sql("select n.nav, n.repurchase_price, n.nav_date, n.sale_price, f.id, f.category \
                    #from nav_data as n inner join mutual_data as m on n.code = m.code \
                   #inner join fund_data as f on f.id = m.category_id").show()

df_avg = spark.sql("select month(n.nav_date), \
                   (sum(n.nav)/month(n.nav_date)) as Monthly_average_nav, \
                   (sum(n.repurchase_price)/month(n.nav_date)), \
                   (sum(n.sale_price)/month(n.nav_date)), f.category \
                   from nav_data as n join mutualfund as m on n.code = m.code \
                   join fund_category as f on f.id = m.category_id group by month(n.nav_date), \
                   f.category").show()

#scheme with non zero exit load

df_nonzeroexitload = spark.sql("select e.Exit_load,f.category from exitload_data as e join mutualfund as m on e.code=m.code join\
                               fund_category as f on f.id=m.category_id \
                                where e.Exit_load != 0 group by f.category,e.Exit_load").show(2)


#scheme’s Max and Min NAV value and Date it occurred
df_minmax = spark.sql("select min(n.nav), max(n.nav), n.nav_date, f.category \
                   from nav_data as n join mutualfund as m on n.code = m.code \
                   join fund_category as f on f.id = m.category_id group by f.category, n.nav_date").show()

df_dec = spark.sql("select nav, nav_date from nav_data \
                            where month(nav_date) == 12 and year(nav_date)== 2018")



df_dec.write.option("header", True).mode("overwrite").partitionBy("nav_date").option("compression","gzip").parquet("/Users/syedaman/Desktop/Data/processed_data/")