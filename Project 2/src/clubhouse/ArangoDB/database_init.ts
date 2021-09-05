import { Database, aql } from "arangojs";

const db = new Database({
	url: "http://127.0.0.1:8529",
	databaseName: "Clubhouse",
	auth: {username: "root", password: "bigdata"}
});

/*
 quattro collezioni da creare
 una per i nodi user, una per i nodi club, una per la relazione user-user e una per la relazione user - club
*/

export function user_data_init(){

	var user_collection = db.collection('userCollection');
	user_collection.create().then(
		() => console.log('Collection created'),
		err => console.error('Failed to create collection: ', err)
	);

}

export function club_data_init(){

	const club_collection = db.collection('clubCollection');
	club_collection.create().then(
		() => console.log('Collection created'),
		err => console.error('Failed to create collection: ', err)
	);

}

export function user_user_edge_init() {

	const user_user_collection = db.collection('user_userCollection');
	user_user_collection.create().then(
		() => console.log('Collection created'),
		err => console.error('Failed to create collection: ', err)
	);
	
}

export function user_club_edge_init() {

	const user_club_collection = db.collection('user_clubCollection');
	user_club_collection.create().then(
		() => console.log('Collection created'),
		err => console.error('Failed to create collection: ', err)
	);
	
}