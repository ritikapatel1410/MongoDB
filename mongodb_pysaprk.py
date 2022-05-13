# For spark version < 3.0
#mongo db lib
from pyspark.sql import SparkSession,Window
import re
import pymongo
import time
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.types import *



#take union of filtered collection
def union_df():
    #create dataframe for first filtered collection
    mongodb_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://localhost:27017/test.{0}?authSource=admin".format(filtered_collection_list[0])).load()
    dic_record_filtered_collection[filtered_collection_list[0]]=mongodb_df.count()
    for df in filtered_collection_list[1:]:
        mongodb_df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://localhost:27017/test.{0}?authSource=admin".format(df)).load()
        dic_record_filtered_collection[df]=mongodb_df1.count()
        mongodb_df=mongodb_df.union(mongodb_df1)
    #create index for split dataframe
    mongodb_df = mongodb_df.withColumn("index",row_number().over(Window.orderBy(monotonically_increasing_id())))
    return mongodb_df

def main():
    mongodb_df=union_df()
    #perform some rename and filter operation then save the spit dataframe as collection into target database
    for element in range(len(filtered_collection_list)):
        if(element==0):
            split_mongodb_df=mongodb_df.limit(dic_record_filtered_collection[filtered_collection_list[0]])
            split_mongodb_df=split_mongodb_df.filter(split_mongodb_df.First_Name != "Radhika")
            split_mongodb_df  = split_mongodb_df.drop('index')
            split_mongodb_df=split_mongodb_df.toDF(*filtered_collection_column_dic[filtered_collection_list[0]])
            split_mongodb_df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(filtered_collection_list[0])).save()     
        elif(element==1):
            split_mongodb_df1=mongodb_df.filter((mongodb_df.index > dic_record_filtered_collection[filtered_collection_list[0]]) & (mongodb_df.index <= (dic_record_filtered_collection[filtered_collection_list[0]]+dic_record_filtered_collection[filtered_collection_list[1]])))
            split_mongodb_df1 = split_mongodb_df1.filter(split_mongodb_df1.Last_Name == "Sharma")
            split_mongodb_df1  = split_mongodb_df1.drop('index')
            split_mongodb_df1=split_mongodb_df1.toDF(*filtered_collection_column_dic[filtered_collection_list[1]])       
            split_mongodb_df1.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(filtered_collection_list[1])).save()     
        else:
            split_mongodb_df2=mongodb_df.filter((mongodb_df.index > (dic_record_filtered_collection[filtered_collection_list[0]]+dic_record_filtered_collection[filtered_collection_list[1]])) & (mongodb_df.index <= (dic_record_filtered_collection[filtered_collection_list[0]]+dic_record_filtered_collection[filtered_collection_list[1]]+dic_record_filtered_collection[filtered_collection_list[2]])))
            split_mongodb_df2 = split_mongodb_df2.filter(split_mongodb_df2._id > "U1IT00004")
            split_mongodb_df2  = split_mongodb_df2.drop('index')
            split_mongodb_df2=split_mongodb_df2.toDF(*filtered_collection_column_dic[filtered_collection_list[2]])
            split_mongodb_df2.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("spark.mongodb.output.uri","mongodb://localhost:27017/target.{0}?authSource=admin".format(filtered_collection_list[2])).save()     

if __name__ == "__main__":
    #mongodb connection
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    #use database "test"
    source_db = myclient['test']
    #find the collection list of test database
    collection_list=source_db.list_collection_names()
    #find the collection which have pattern like bangalore_
    filtered_collection_list=list(filter(lambda x : re.search(r"bangalore_",x) ,collection_list))
    #create dictionary for record of filtered collection
    dic_record_filtered_collection={}
    #create the dic of filtered collection with respective column
    filtered_collection_column_dic={"bangalore_it_supports":["bangalore_it_supports_Date_Of_Birth","bangalore_it_supports_First_Name", "bangalore_it_supports_Last_Name","_id","bangalore_it_supports_e_mail","bangalore_it_supports_phone"],
    "bangalore_emp":["bangalore_emp_Date_Of_Birth","bangalore_emp_First_Name", "bangalore_emp_Last_Name","_id","bangalore_emp_e_mail","bangalore_emp_phone"],
    "bangalore_city":["bangalore_city_Date_Of_Birth","bangalore_city_First_Name", "bangalore_city_Last_Name","_id","bangalore_city_e_mail","bangalore_city_phone"]}
    #create spark session
    spark = SparkSession.builder.master("local[*]").appName('Mongo DB').config('spark.jars.packages', 'com.mongodb.spark.sql.connector.MongoTableProvider').getOrCreate()
    main()

