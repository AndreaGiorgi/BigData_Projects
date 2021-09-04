import * as etl from './database_etl'

async function pipeline(){

	etl.user_data_loading()
	etl.club_data_loading()
}

pipeline()