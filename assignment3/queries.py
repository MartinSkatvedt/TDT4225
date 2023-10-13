from pprint import pprint 
from DbConnector import DbConnector

class Queries:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)

    
    def task_1(self):
        """
        How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database).
        """
        collection = self.db['User']
        print("Users:", collection.count_documents({}))
        collection = self.db['Activity']
        print("Activities:", collection.count_documents({}))
        collection = self.db['Trackpoint']
        print("Trackpoints:", collection.count_documents({}))

    def task_2(self):
        """
        Find the average number of activities per user.
        """
        collection = self.db['User']
        n_users = collection.count_documents({})
        collection = self.db['Activity']
        n_activities = collection.count_documents({})
        print("Average number of activities per user:", round(n_activities/n_users, 2))

    def task_3(self):
        """
        Find the top 20 users with the highest number of activities.
        """

        user_activities = {}

        collection = self.db['User']
        users = collection.find({})

        for user in users:
            count = len(user['activities'])
            user_activities[user['_id']] = count

        sorted_activities = sorted(user_activities.items(), key=lambda x: x[1], reverse=True)
        print("Top 20 users with the highest number of activities:")

        for i in range(20):
            print(sorted_activities[i])
    
    def task_4(self):
        """
        Find all users who have taken a taxi.
        """
        collection = self.db['Activity']
        users = collection.find({'transportation_mode': 'taxi'})

        users_set = set()
        for user in users:
            users_set.add(user['_id'][:3])
        
        print("Users who have taken a taxi:")
        print(sorted(users_set))

    def task_5(self):
        """
        Find all types of transportation modes and count how many activities that are tagged with 
        these transportation mode labels. Do not count the rows where the mode is null.
        """

        activities_dict = {}

        collection = self.db['Activity']
        activities = collection.find({'transportation_mode': {'$ne': None}})

        for activity in activities:
            mode = activity['transportation_mode']
            activities_dict.get(mode, 0)
            activities_dict[mode] = activities_dict.get(mode, 0) + 1
        
        print("Type of transportation mode : number of activities")
        pprint(activities_dict)

    def task_6_a(self):
        """
        Find the year with the most activities.
        """
        

def main():
    program = None
    try:
        program = Queries()
        program.task_6_a()
  
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()