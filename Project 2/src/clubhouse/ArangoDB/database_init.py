from arango import ArangoClient

def initialization():
    client = ArangoClient(hosts = 'http://127.0.0.1:8529')
    
    # Connect to "_Twitch" Database as root user. Returns an API wrapper
    clubhouse_db = client.db('Clubhouse', username='root', password = 'bigdata') #TOTALLY UNSAFE API AUTH
    
    # Create a new collection named "userCollection" if it does not exist.
	# This returns an API wrapper for "userCollection" collection.
    
    if clubhouse_db.has_collection('userCollection'):
        users = clubhouse_db.collection('userCollection')
    else:
        print("User Collection created \n")
        users = clubhouse_db.create_collection('userCollection')
        
    print("-----------Properties------------\n")
    print(users.properties())
    print("-----------Statistics------------\n")
    print(users.statistics())
    
    
    if clubhouse_db.has_collection('clubCollection'):
        clubs = clubhouse_db.collection('clubCollection')
    else:
        print("Club Collection created \n")
        clubs = clubhouse_db.create_collection('clubCollection')
        
    print("-----------Properties------------\n")
    print(clubs.properties())
    print("\n-----------Statistics------------\n")
    print(clubs.statistics())