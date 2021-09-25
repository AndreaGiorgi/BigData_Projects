var pregel = require("@arangodb/pregel");

var paramsLP = {resultField: "LP"};

var handle_LP = pregel.start("labelpropagation", "smart_Clubhouse", paramsLP);
var status = pregel.status(handle_LP);
print(status);
while (!["done", "canceled"].includes(pregel.status(handle_LP).state)) {
	print("waiting for result" + pregel.status(handle_LP).state);
	require("internal").wait(5);
  }

print(pregel.status(handle_LP).state);
print(status);

var paramsSLPA = {maxGSS: 250, resultField: "SLPA", maxCommunities: '15'};
var handle_SLPA = pregel.start("slpa", "smart_Clubhouse", paramsSLPA);

var status = pregel.status(handle_SLPA);
print(status);
while (!["done", "canceled"].includes(pregel.status(handle_SLPA).state)) {
	print("waiting for result" + pregel.status(handle_LP).state);
	  require("internal").wait(5);
}

print(pregel.status(handle_SLPA).state);
print(status);