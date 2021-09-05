var init = require('./database_init');

async function pipeline(){

	init.user_data_init()
	init.club_data_init()
	init.user_user_edge_init()
	init.user_club_edge_init()
}

pipeline()