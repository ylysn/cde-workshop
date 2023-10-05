#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Curtis Howard, Yazan Al Yasin
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from random import random
import configparser

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('TUNING').config("spark.yarn.access.hadoopFileSystems", data_lake_name).getOrCreate()
_DEBUG_ = False
username = spark.sql("select current_user() as user").first()['user']

print("Running as Username: ", username)

#---------------------------------------------------
#                RUN LONG SQL
#---------------------------------------------------

scale = 2000

for key in range(0,6):
    spark.sparkContext.parallelize(range(0,scale))\
                      .map(lambda x: (key,int(random()*(2**32-1))))\
                      .toDF(['key','value'])\
                      .createOrReplaceTempView('key' + str(key) + '_table')

spark.sql('''SELECT a.key, a.value 
             FROM key0_table a 
             JOIN (SELECT * FROM key0_table LIMIT 5) b 
             ON a.key = b.key''').createOrReplaceTempView('key0_5x_table') 

spark.sql('''SELECT * FROM key1_table 
             UNION ALL 
             SELECT * FROM key2_table 
             UNION ALL 
             SELECT * FROM key3_table 
             UNION ALL 
             SELECT * FROM key4_table 
             UNION ALL 
             SELECT * FROM key5_table 
             UNION ALL 
             SELECT * FROM key0_5x_table''').createOrReplaceTempView('skewed_table')

df = spark.sql('''SELECT a.key, rand() as r 
                  FROM skewed_table a 
                  JOIN skewed_table b 
                  ON a.key = b.key ORDER BY r''')

df.coalesce(4).write.mode("overwrite").saveAsTable('{}_tuning_demo_input_table'.format(username))

spark.stop()
