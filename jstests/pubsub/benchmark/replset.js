load("jstests/pubsub/benchmark/helpers.js")

var publish = function(message) {

    var rs = new ReplSetTest({ nodes : 3, name : "pubsubBenchmark" });
    var nodes = rs.startSet();
    rs.initiate();

    // TODO
    // use MongoRunner.runMongo to connect to one of the replset nodes
    // on connection, subscribe to readySignal to hear when the shells are ready


    for (var i=1; i<nodes.length; i++) {
        // TODO
        // start parallel shell on each other node to run the same benchrun test 
        // need to send startSignal through that connection to tell them to start
        startParallelShell("

    }
 
    var ops = [{op : "command",
                ns : "test",
                command : { publish : "A", message : message }
               }];
    var benchArgs = {ops : ops, seconds : timeSecs};
 
    for (var numClients = 1; numClients <= maxClients; numClients++) {
        benchArgs["parallel"] = numClients;
        benchArgs["host"] = nodes[numClients%3].getDB('test').getMongo().host;

        before = db.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = db.serverStatus()["opcounters"]["command"];                                         
        stats["server"]["" + numClients] = (after - before)/timeSecs;                               
    }                                                                                               
                                                                                                    
    printStats(stats);

}































