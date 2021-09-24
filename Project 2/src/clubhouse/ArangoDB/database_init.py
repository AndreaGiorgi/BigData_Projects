from arango import ArangoClient

def initialization():
    client = ArangoClient(hosts = 'http://localhost:8000/')
    
    # Connect to "_Twitch" Database as root user. Returns an API wrapper
    clubhouse_db = client.db('Clubhouse', username='root', password = '') #TOTALLY UNSAFE API AUTH
    
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
    
    if clubhouse_db.has_graph('ClubhouseGraph'):
        graph = clubhouse_db.graph('ClubhouseGraph')
    else:
        graph = clubhouse_db.create_graph('ClubhouseGraph')
    
    if graph.has_edge_definition('usersEdgesCollection'):
        edges = graph.edge_collection('usersEdgesCollection')
    else:
        print("Edge Collection created \n")
        edges = graph.create_edge_definition(
            edge_collection = 'usersEdgesCollection',
            from_vertex_collections = ['userCollection'],
            to_vertex_collections=['userCollection'])
        
    print("\n----------- Edges Properties------------\n")
    print(edges.properties())
    print("\n-----------Edges Statistics------------\n")
    print(edges.statistics())
    
    if graph.has_edge_definition('clubsEdgesCollection'):
        edges = graph.edge_collection('clubsEdgesCollection')
    else:
        print("Edge Collection created \n")
        edges = graph.create_edge_definition(
            edge_collection = 'clubsEdgesCollection',
            from_vertex_collections = ['clubCollection'],
            to_vertex_collections=['userCollection'])
        
    print("\n----------- Edges Properties------------\n")
    print(edges.properties())
    print("\n-----------Edges Statistics------------\n")
    print(edges.statistics())