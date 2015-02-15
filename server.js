var http = require('http');
var sockjs = require('sockjs');
var redis = require('redis');
var HashMap = require('hashmap').HashMap;
var r = require("request");
var md5 = require('MD5');
var scribe = require('scribe-js')({
    createDefaultConsole : false
});

var graylog2 = require("graylog2");
var logger = new graylog2.graylog({
    servers: [
        { 'host': "logs.fiskkit.com", port: 5556 }
    ],
    hostname: 'notifications.uwan.DEV', // the name of this host 
                             // (optional, default: os.hostname()) 
    facility: 'Node.js',     // the facility for these log messages 
                             // (optional, default: "Node.js") 
    bufferSize: 1400         // max UDP packet size, should never exceed the 
                             // MTU of your system (optional, default: 1400) 

    });


var socketConsole = scribe.console({
    console : {
        colors : 'blue'
    },
    logWriter : {
        rootPath : 'WebSocketLogs' //all logs in ./WebSocketLogs
    }
});


// Setup Redis pub/sub.
// NOTE: You must create two Redis clients, as 
// the one that subscribes can't also publish.
logger.log(process.env);
logger.log(process.env.REDIS_ENDPOINT);
//socketConsole.time().log(process.env);
//socketConsole.time().log(process.env.REDIS_ENDPOINT);
var pub = redis.createClient(6379, process.env.REDIS_ENDPOINT,{});
var sub = redis.createClient(6379, process.env.REDIS_ENDPOINT,{});
var clients = [];
var map = new HashMap();
var txUrl = "http://172.31.5.202:7474/db/data/transaction/commit";

function cypher(query, params, cb) {
	console.log("Request: " + query);
	var statements = JSON.stringify({statements:[{statement:query, parameters:params}]});

	r.post({
		url:txUrl,
	    body: statements,
	    headers:{"Content-Type": "application/json"}
		}, 
	     	function(err,res,body) {
	         //console.log(res); 
	          cb(err,body);
	         })

}

// Listen for messages being published to this server.
sub.on('message', function(channel, msg) {
  // Broadcast the message to all connected clients on this server.


  		clients = map.get(channel);
		clients.forEach(function(conn){
		conn.write(msg);
		logger.log('message: ' + msg);
			
		});
    

});

// Setup our SockJS server.
var opts = {sockjs_url: "http://cdn.jsdelivr.net/sockjs/0.3.4/sockjs.min.js"};
var sockjs_echo = sockjs.createServer(opts);

sockjs_echo.on('connection', function(conn) {
	var userID = {};
/*	var t = new Date(Date.now());
	var formattedTime = t.format("dd.mm.yyyy hh:MM:ss");*/

    console.log('--------------------NEW CONNECTION--------------------');
	socketConsole.time().log(' Client: ' + conn + ' connected');
	logger.log(' Client: ' + conn + ' connected');


	map.forEach(function(value, key) {

		console.log(key + " : " + value);
	});


	conn.on('data', function(message) {
		var msg = JSON.parse(message);
		userID = msg.userid;

		console.log("User ID: " + userID);
		logger.log("User ID: " + userID);

		console.log("Smack: " + msg.smack);

		var query = "MATCH (user:`User`)-[:FOLLOWS]->(following) WHERE user.uid = {userid} RETURN following LIMIT 100";
		var params = {userid: userID};

		var cb = function(err,data) {

			var cypherResult = {};
			cypherResult = JSON.parse(data);
			console.log(JSON.stringify(cypherResult));
			logger.log(JSON.stringify(cypherResult));



			var userChannels = cypherResult.results[0].data;

			userChannels.forEach(function(channel) {

				console.log("Article ID: " + channel.row[0].a_id);
				logger.log("Article ID: " + channel.row[0].a_id);


				if(channel.row[0].a_id){
				var newChannel = "article." + channel.row[0].a_id;
				var clientList = [];

					if(map.has(newChannel)){
						clientList = map.get(newChannel);
						clientList.push(conn);
					}else{
						clientList.push(conn);						
						map.set(newChannel, clientList);			
						sub.subscribe(newChannel);

					}
				}
			});

			var articleChannel = 'ArticlePublish.' + userID;
			var uniqueConnection = [];
			uniqueConnection.push(conn);
			map.set(articleChannel, uniqueConnection);

			sub.subscribe(articleChannel);			

		}


		cypher(query,params,cb);
		 

	});




    conn.on('close', function() {
    	sub.unsubscribe('ArticlePublish.' + userID);
    	map.remove('ArticlePublish.' + userID);

    	map.forEach(function(value, key) {
    		value = value.splice(value.indexOf(conn),1);

	    	console.log(key + " : " + value);
		});

        console.log('close ' + conn);
       	logger.log('close ' + conn);

        console.log('--------------------CONNECTION CLOSED--------------------');

    });	


});

// 2. Static files server
//var static_directory = new node_static.Server(__dirname);

// 3. Usual http stuff
var server = http.createServer();
/*server.addListener('request', function(req, res) {
	static_directory.serve(req, res);
});
server.addListener('upgrade', function(req,res){
	res.end();
});*/

sockjs_echo.installHandlers(server, {prefix:'/notifications'});

console.log(' [*] Listening on 0.0.0.0:9500' );
server.listen(9500, '0.0.0.0');
