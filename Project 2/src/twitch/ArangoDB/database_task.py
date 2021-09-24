from arango import ArangoClient
import time

def detection():
    client = ArangoClient(hosts = 'http://localhost:8000/')
    twitch_db = client.db('Twitch', username='root', password = '')
    pregel = twitch_db.pregel
   
    task_id = pregel.create_job(
		graph = 'TwitchGraph',
		algorithm= 'slpa',
		store = False,
		max_gss= 1000,
		thread_count= 4,
		result_field= 'slpa_community',
        algorithm_params = {'shardKeyAttribute' : '_from', 'maxCommunities': 10}
	)

    slpa_job = pregel.job(task_id)
    time.sleep(20) # Wait time for better SLPA execution, bad processor = bad timing
    
    
    task_id = pregel.create_job(
		graph = 'TwitchGraph',
		algorithm= 'labelpropagation',
		store = False,
		max_gss= 100,
		thread_count= 4,
		result_field= 'lpa_community',
        algorithm_params = {'shardKeyAttribute' : '_from'}
	)

    lpa_job = pregel.job(task_id)
    time.sleep(20) # Wait time for better SLPA execution, bad processor = bad timing
        

    

        
        
    

        
    
    