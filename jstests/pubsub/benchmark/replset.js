load("jstests/pubsub/benchmark/helpers.js");
load("src/mongo/shell/synchronized.js");

/*

var publish = function(messageSize) {

    var rs = new ReplSetTest({ nodes : 3, name : "pubsubBenchmark" });
    var nodes = rs.startSet();
    rs.initiate();

    // on one of the replsettest nodes,
    // subscribe to readySignal to hear when the shells are ready
    var rsdb = nodes[0].getDB('test');
    var rsps = rsdb.PS();
    var subReadySignal = rsps.subscribe("readySignal");

    var ops = [{op : "command",
                ns : "test",
                command : { publish : "A"}
               }];
    if (messageSize == "light")
        ops[0]["command"]["message"] = lightMessage;
    else if (messageSize == "heavy")
        ops[0]["command"]["message"] = heavyMessage;
    else{
        print("unknown message size " + messageSize);
        return;
    }

    var benchArgs = {ops: ops, host: rsdb.getMongo().host, seconds: timeSecs, parallel: 1 };

    print("\n\n\n\n\nstarting tests\n\n\n\n\n");
 
    for (var numClients = 1; numClients <= maxClients; numClients++) {
        var shells = [];

        print("starting parallel publish shells for " + numClients + " clients\n\n\n\n\n");
        for (var node = 0; node < nodes.length; node++) {
            // evenly distribute clients across each node
            var load = numClients/3;
            // add leftover clients to last node
            if (node == nodes.length - 1) 
                load += numClients%3;
            print("\n\n\nstarting parallel publish shell!\n");
            shells[node] = startPublishShell(messageSize, load, nodes[node].port);
        }


        print("\n\n\n\n\nwaiting for ready signal from shells\n\n\n\n\n");

        // wait for ready signals from all shells
        var readyCount = 0;
        while (readyCount < nodes.length) {
            var res = rsps.poll(subReadySignal, 1000 * 60 * 10);
            if (res["messages"][subReadySignal.str])
                readyCount += res["messages"][subReadySignal.str]["readySignal"].length;
        }

        print("sending start signal\n\n\n");

        // signal to all shells that they can start their benchrun tests
        rsps.publish("startSignal", { start : 1 });
       
        print("starting own benchrun test\n\n\n");
 
        // start our own benchRun test against one of the nodes
        before = rsdb.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = rsdb.serverStatus()["opcounters"]["command"];                                         
        stats["server"]["" + numClients] = (after - before)/timeSecs;                               

        print("finished our benchrun test, waiting for all shells to terminate\n\n\n");

        // wait for all shells to terminate before starting next batch of tests
        for (var i=0; i<nodes.length; i++)
            shells[i](); 

        print("all shells terminated\n\n\n");
        printStats(stats);
    }                                                                                               
                       
    rs.stopSet();
                                                                             
    printStats(stats);

}

*/

var poll = function(messageSize) {

    var rs = new ReplSetTest({ nodes : 3, name : "pubsubBenchmark" });
    var nodes = rs.startSet();
    rs.initiate();

    // on one of the replsettest nodes,
    // subscribe to readySignal to hear when the shells are ready
    var rsdb = nodes[0].getDB('test');
    var rsps = rsdb.PS();
    var subReadySignal = rsps.subscribe("readySignal");

    // on that node, subscribe to the channel it will poll from
    var subA = rsps.subscribe("A");

    var ops = [{op : "command",
                ns : "test",
                command : { poll : subA }
               }];

    var benchArgs = {ops: ops, host: rsdb.getMongo().host, seconds: timeSecs, parallel: 1 };

    print("\n\n\n\n\nstarting tests\n\n\n\n\n");
 
    for (var numClients = 1; numClients <= maxClients; numClients++) {
        var shells = [];

        print("starting parallel poll shells for " + numClients + " clients\n\n\n\n\n");
        for (var i=0; i<numClients; i++) {
            print("\n\n\nstarting parallel poll shell!\n");
            shells[i] = startPollShell(nodes[i%3].port);
        }

        print("\n\n\n\n\nwaiting for ready signal from shells\n\n\n\n\n");

        var numShells = numClients;

        // wait for ready signals from all shells
        var readyCount = 0;
        while (readyCount < numShells) {
            var res = rsps.poll(subReadySignal, 1000 * 60 * 10);
            if (res["messages"][subReadySignal.str])
                readyCount += res["messages"][subReadySignal.str]["readySignal"].length;
        }

        print("sending start signal\n\n\n");

        // signal to all shells that they can start their benchrun tests
        rsps.publish("startSignal", { start : 1 });
       
        print("starting own benchrun test\n\n\n");
 
        // start our own benchRun test against one of the nodes
        before = rsdb.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = rsdb.serverStatus()["opcounters"]["command"];                                         
        stats["server"]["" + numClients] = (after - before)/timeSecs;                               

        print("finished our benchrun test, waiting for all shells to terminate\n\n\n");

        // wait for all shells to terminate before starting next batch of tests
        for (var i=0; i<numShells; i++)
            shells[i](); 

        print("all shells terminated\n\n\n");
        printStats(stats);
    }                                                                                               
                       
    rs.stopSet();
                                                                             
    printStats(stats);

}

*/

