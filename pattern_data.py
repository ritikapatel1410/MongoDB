import datetime
import dateutil.parser
def get_database():
    from pymongo import MongoClient
    import pymongo

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    #user: "myNormalUser", pwd: "xyz123"
    CONNECTION_STRING ="mongodb://localhost/mydb"
    #CONNECTION_STRING = "mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/myFirstDatabase"

    #"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    from pymongo import MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client['test']
    
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":    
    
    # Get the database
    dbname = get_database()

collection_name = dbname["efgh"]

todays_date=datetime.datetime.now()
yesterday_date=todays_date.replace(hour=0, minute=0, second=0,microsecond=0)-datetime.timedelta(days = 1)
today_date=todays_date.replace(hour=0, minute=0, second=0,microsecond=0)

item1={
	"_id": "U1IT00007",
	"First_Name": "Radhika",
	"Last_Name": "Sharma",
	"Date_Of_Birth": "1995-09-26",
	"e_mail": "radhika_sharma.123@gmail.com",
	"phone": "9000012345"
}
item2={	"_id": "U1IT00008",
	"First_Name": "Rachel",
	"Last_Name": "Christopher",
	"Date_Of_Birth": "1990-02-16",
	"e_mail": "Rachel_Christopher.123@gmail.com",
	"phone": "9000054321"
}
item3={
	"_id": "U1IT00009",
	"First_Name": "Fathima",
	"Last_Name": "Sheik",
	"Date_Of_Birth": "1990-02-16",
	"e_mail": "Fathima_Sheik.123@gmail.com",
	"phone": "9000054321"
}

collection_name.insert_many([item1,item2,item3])
