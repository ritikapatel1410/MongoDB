
from pyspark.sql import SparkSession
import re
import pymongo
import time
from pyspark.sql.functions import monotonically_increasing_id 



def concat_df():
    #create dataframe for first filtered collection
    mongodb_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://localhost:27017/test.{0}?authSource=admin".format(filtered_collection_list[0])).load()
    #change the column name
    mongodb_df = mongodb_df.toDF(*filtered_collection_column_dic[filtered_collection_list[0]])
    #create index
    mongodb_df = mongodb_df.select("*").withColumn("id", monotonically_increasing_id())
    for df in filtered_collection_list[1:]:
        mongodb_df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://localhost:27017/test.{0}?authSource=admin".format(df)).load()
        mongodb_df1 = mongodb_df1.toDF(*filtered_collection_column_dic[df])
        mongodb_df1 = mongodb_df1.select("*").withColumn("id", monotonically_increasing_id())
        mongodb_df = mongodb_df.join(mongodb_df1,["id"])
    return mongodb_df

def main():
    #perform some rename and filter operation then save the spit dataframe into target database
    mongodb_df=concat_df()
    #mongodb_df.show()
    for split_df in filtered_collection_list:
        if(split_df=="bangalore_it_supports"):
            bangalore_it_supports_df = mongodb_df.filter(mongodb_df.bangalore_it_supports_First_Name != "Radhika")
            bangalore_it_supports_df = bangalore_it_supports_df[filtered_collection_column_dic[split_df]]
            bangalore_it_supports_df =bangalore_it_supports_df.withColumnRenamed("bangalore_it_supports_id", "_id")
            bangalore_it_supports_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(split_df)).save()     
           
        elif(split_df=="bangalore_emp"):
            bangalore_emp_df = mongodb_df.filter(mongodb_df.bangalore_emp_Last_Name == "Sharma")
            bangalore_emp_df = bangalore_emp_df[filtered_collection_column_dic[split_df]]
            bangalore_emp_df = bangalore_emp_df.withColumnRenamed("bangalore_emp_id", "_id")
            bangalore_emp_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(split_df)).save()     
            
        else:
            bangalore_city_df = mongodb_df.filter(mongodb_df.bangalore_city_id > "U1IT00004")
            bangalore_city_df = bangalore_city_df[filtered_collection_column_dic[split_df]]
            bangalore_city_df = bangalore_city_df.withColumnRenamed("bangalore_city_id", "_id")
            bangalore_city_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(split_df)).save()     
        

        
if __name__ == "__main__":
    #mongodb connection
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    #use database "test"
    source_db = myclient['test']
    #find the collection list of test database
    collection_list=source_db.list_collection_names()
    #find the collection which have pattern like bangalore_
    filtered_collection_list=list(filter(lambda x : re.search(r"bangalore_",x) ,collection_list))
    #create the dic of filtered collection with respective column
    filtered_collection_column_dic={"bangalore_it_supports":["bangalore_it_supports_Date_Of_Birth","bangalore_it_supports_First_Name", "bangalore_it_supports_Last_Name","bangalore_it_supports_id","bangalore_it_supports_e_mail","bangalore_it_supports_phone"],
    "bangalore_emp":["bangalore_emp_Date_Of_Birth","bangalore_emp_First_Name", "bangalore_emp_Last_Name","bangalore_emp_id","bangalore_emp_e_mail","bangalore_emp_phone"],
    "bangalore_city":["bangalore_city_Date_Of_Birth","bangalore_city_First_Name", "bangalore_city_Last_Name","bangalore_city_id","bangalore_city_e_mail","bangalore_city_phone"]}
    #create spark session
    spark = SparkSession.builder.master("local[*]").appName('Mongo DB').config('spark.jars.packages', 'com.mongodb.spark.sql.connector.MongoTableProvider').getOrCreate()
    main()






    








    








    







