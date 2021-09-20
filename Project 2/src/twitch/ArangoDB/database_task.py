from arango import ArangoClient, AQLQueryExecuteError, AsyncJobCancelError, AsyncJobClearError
import time

def detection():
    client = ArangoClient(hosts = 'http://127.0.0.1:8529')
    twitch_db = client.db('Twitch', username='root', password = 'bigdata')
    pregel = twitch_db.pregel
    
    # Begin async execution. This returns an instance of AsyncDatabase, a
	# database-level API wrapper tailored specifically for async execution.
    
    async_db = twitch_db.begin_async_execution(return_result=True)
    async_aql = async_db.aql
    async_pregel = async_db.pregel #API wrapper in async env
    
    # API execution context is always set to "async"
    assert async_db.context == 'async'
    assert async_aql.context == 'async'
    assert async_pregel.context == 'async'
    
    task_id = async_pregel.create_job(
		graph = 'TwitchGraph',
		algorithm= 'slpa',
		store = False,
		max_gss= 100,
		thread_count= 8,
		async_mode= True,
		result_field= 'community'
	)
    
    slpa_job = async_pregel.job(task_id)
    time.sleep(50) # Wait time for better SLPA execution, bad processor = bad timing
    assert slpa_job.status() in {'pending', 'done', 'cancelled'}
    
    
    while slpa_job.status() != 'done':
        time.sleep(10)
        
    print("Job id: " + slpa_job.id)

    

        
        
    

        
    
    