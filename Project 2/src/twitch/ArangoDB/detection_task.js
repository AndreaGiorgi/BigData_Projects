var pregel = require("@arangodb/pregel");

var paramsLP = {resultField: "LP"};

var handle_LP = pregel.start("labelpropagation", "smart_Twitch", paramsLP);
var status = pregel.status(handle_LP);
print(status);
while (!["done", "canceled"].includes(pregel.status(handle_LP).state)) {
	print("waiting for result");
	require("internal").wait(1); // TODO: make this more clever
  }

print(pregel.status(handle_LP).state);

var paramsSLPA = {maxGSS: 250, resultField: "SLPA", maxCommunities: '15'};
var handle_SLPA = pregel.start("slpa", "smart_Twitch", paramsSLPA);

var status = pregel.status(handle_SLPA);
print(status);
while (!["done", "canceled"].includes(pregel.status(handle_SLPA).state)) {
	  print("waiting for result");
	  require("internal").wait(1); // TODO: make this more clever
}

print(pregel.status(handle_SLPA).state);