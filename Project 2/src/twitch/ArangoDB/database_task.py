from arango import ArangoClient
import time

def detection():
    client = ArangoClient(hosts = 'http://localhost:8000/')
    twitch_db = client.db('Twitch', username='root', password = '')
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
    async_db.create_task(
        name='SLPA',
        command= '''
            var db = require('@arangodb')
            var pregel = require("@arangodb/pregel");
            var param = {maxGSS:100, resultField = "community"};
            var handle = pregel.start("slpa", "TwitchGraph", param);

            while (!["done", "canceled"].includes(pregel.status(handle).state)) {
            print("waiting for result");
            require("internal").wait(0.5); // TODO: make this more clever
            }

            var status = pregel.status(handle);
            print(status);

            if (status.state == "done") {
                print(status); }
}
        ''',
        params={},
        period=0,
        task_id='001'
    )
    
    async_db.task('001')

    #slpa_job = async_pregel.job(task_id)
    #time.sleep(50) # Wait time for better SLPA execution, bad processor = bad timing
    #assert slpa_job.status() in {'pending', 'done', 'cancelled'}
    
    
    #while slpa_job.status() != 'done':
    #    time.sleep(10)
        
    #print("Job id: " + slpa_job.id)

    

        
        
    

        
    
    