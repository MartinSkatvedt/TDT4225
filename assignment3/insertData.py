from pprint import pprint 
from DbConnector import DbConnector
import os
import datetime

def number_to_string(number):
    if number < 10: 
        number = "00" + str(number)
            
    elif number < 100:
        number = "0" + str(number)

    return str(number)

class InsertData:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
    
    def insert_users(self):
        #Creates a dict containing which users have labeled data
        labeled_dict = {}
        with open("dataset/labeled_ids.txt", "r") as f:
            read_users = f.readlines()

            for user in read_users:
                labeled_dict.update({user.strip(): True})

        #Inserts users into the database
        for i in range(0, 182):
            user_id = number_to_string(i)
            collection = self.db['User']
            collection.insert_one(
                {
                    '_id': user_id,
                    'has_labels': labeled_dict.get(user_id, False),
                    'activities': []
                }
            )
    def insert_activities(self):
         collection = self.db['Activity']
         for i in range(0, 182):
            user_id = number_to_string(i)
            dirname="./dataset/CleanedData/" + user_id
            activity_files = os.listdir(dirname)


            for activity_file in activity_files:
                with open(dirname + "/" + activity_file, "r") as f:
                    activity_id = int(activity_file.replace(".txt", ""))
                    activity_id = user_id + number_to_string(activity_id)
                    data = f.readline()
                    data = data.strip().split(',')

                    start = datetime.datetime.strptime(data[1], "%Y-%m-%d %H:%M:%S")
                    end = datetime.datetime.strptime(data[2], "%Y-%m-%d %H:%M:%S")

                    if data[3].strip() == "undefined":
                        collection.insert_one(
                            {
                                '_id': activity_id,
                                'transportation_mode': None,
                                'start_date_time': start,
                                'end_date_time': end,
                            }
                        )
                    else:
                        collection.insert_one(
                            {
                                '_id': activity_id,
                                'transportation_mode': data[3].strip(),
                                'start_date_time': start,
                                'end_date_time': end,
                            }
                        )
                self.db['User'].update_one(
                    {
                        '_id': user_id
                    },
                    {
                        '$push': {
                            'activities': activity_id
                        }
                    }
                )

    def insert_trackpoints(self):   
        collection = self.db['Trackpoint']
        for i in range(0, 182):
            user_id = number_to_string(i)
            print(user_id)
            dirname="./dataset/CleanedData/" + user_id
            activity_files = os.listdir(dirname)

            for activity_file in activity_files:
                docs_to_insert = []
                with open(dirname + "/" + activity_file, "r") as f:
                    activity_id = int(activity_file.replace(".txt", ""))
                    activity_id = user_id + number_to_string(activity_id)
                    
                    next(f)
                    next(f)
                    for line in f.readlines():
                        data = line.split(',')
                        a_id = data[0]
                        lat = float(data[1])
                        lon = float(data[2])
                        alt = float(data[3])
                        date_time = datetime.datetime.strptime(data[4].strip(), "%Y-%m-%d %H:%M:%S")

                        docs_to_insert.append(
                            {
                                'activity_id': a_id,
                                'lat': lat,
                                'lon': lon,
                                'altitude': alt,
                                'date_time': date_time
                            }
                        )
                    collection.insert_many(docs_to_insert)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = InsertData()
        #program.drop_coll(collection_name="User")
        #program.create_coll(collection_name="User")
        #program.create_coll(collection_name="Activity")
        #program.create_coll(collection_name="Trackpoint")
        
        program.show_coll()
        
        #program.insert_users()
        #program.insert_activities()
        program.insert_trackpoints()

        #program.fetch_documents(collection_name="Activity")
        
        #program.show_coll()Ac
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
