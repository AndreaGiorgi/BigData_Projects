from arango import ArangoClient

def initialization():
    client = ArangoClient(hosts = 'http://localhost:8000/')
    
    # Connect to "_Twitch" Database as root user. Returns an API wrapper
    twitch_db = client.db('Twitch', username='root', password = '') #TOTALLY UNSAFE API AUTH
    
    if twitch_db.has_graph('TwitchGraph'):
        graph = twitch_db.graph('TwitchGraph')
    else:
        graph = twitch_db.create_graph('TwitchGraph')
    
    # Create a new collection named "twitchCollection" if it does not exist.
	# This returns an API wrapper for "twitchCollection" collection.
    
    if twitch_db.has_collection('twitchCollection'):
        users = twitch_db.collection('twitchCollection')
    else:
        print("Twitch Collection created \n")
        users = twitch_db.create_collection('twitchCollection')
        
    print("-----------Twitch Properties------------\n")
    print(users.properties())
    print("\n-----------Twitch Statistics------------\n")
    print(users.statistics())
    
    if graph.has_edge_definition('twitchEdges'):
        edges = graph.edge_collection('twitchEdges')
    else:
        print("Edge Collection created \n")
        edges = graph.create_edge_definition(
            edge_collection = 'twitchEdges',
            from_vertex_collections = ['twitchCollection'],
            to_vertex_collections=['twitchCollection'])
        
    print("\n----------- Edges Properties------------\n")
    print(edges.properties())
    print("\n-----------Edges Statistics------------\n")
    print(edges.statistics())
    