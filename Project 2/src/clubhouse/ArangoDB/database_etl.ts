import { Database, aql } from "arangojs";

const db = new Database({
	url: "http://127.0.0.1:8529",
	databaseName: "Clubhouse",
	auth: {username: "root", password: "bigdata"}

});

export async function user_data_loading(){

}

export async function club_data_loading(){

}

export async function graph_init() {
	
}