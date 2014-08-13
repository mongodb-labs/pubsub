load("jstests/pubsub/benchmark/helpers.js");




/**
 * Measure publishes/second with an increasing number of parallel threads
 **/ 
var publish = function(message) {
    var ops = [{op : "command",
                ns : "test",
                command : { publish : "A", message : message }
               }];
    var benchArgs = {ops : ops, host : db.getMongo().host, seconds : timeSecs};

    for (var numClients = 1; numClients <= maxClients; numClients++) {
        benchArgs["parallel"] = numClients;

        before = db.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = db.serverStatus()["opcounters"]["command"];
        stats["server"]["" + numClients] = (after - before)/timeSecs;
    }

    printStats(stats);
}






var startPublishShell = function(message) {
    if (message == "light") {
        return startParallelShell('var subStartSignal = ps.subscribe("startSignal");' +
               'load("jstests/pubsub/benchmark/helpers.js");' + 
               'var ops = [{ op:"command", ns:"test", command: { publish: "A", message: lightMessage }}];' +
                // signal that this shell is ready
               'ps.publish("readySignal", { ready : 1 });' +
                // wait for signal that all shells ready (signal might already be in buffer)
               'ps.poll(subStartSignal, 1000 * 60 * 10);' +
                // all shells ready; start benchRun
               'benchRun({ ops: ops, host: db.getMongo().host, seconds:' + timeSecs + ', parallel: 1});' +
               'ps.unsubscribeAll();' +
               'quit();' 
             );
    }
    else if (message == "heavy") {
        return startParallelShell('var subStartSignal = ps.subscribe("startSignal");' +
               'load("jstests/pubsub/benchmark/helpers.js");' + 
               'var ops = [{ op:"command", ns:"test", command: { publish: "A", message: heavyMessage }}];' +
                // signal that this shell is ready
               'ps.publish("readySignal", { ready : 1 });' +
                // wait for signal that all shells ready (signal might already be in buffer)
               'ps.poll(subStartSignal, 1000 * 60 * 10);' +
                // all shells ready; start benchRun
               'benchRun({ ops: ops, host: db.getMongo().host, seconds:' + timeSecs + ', parallel: 1});' +
               'ps.unsubscribeAll();' +
               'quit();' 
             );
    }
    else {
        print("Error: message size " + message + " not supported.");
        return null;
    }
}


/**
 * Measure polls/second with an increasing number of parallel threads
 **/ 
var poll = function(message) {

    var subA = ps.subscribe("A"); 
    var ops = [{op : "command",
                ns : "test",
                command : { poll : subA }
              }];
    var benchArgs = {ops : ops, host : db.getMongo().host, seconds : timeSecs, parallel : 1};
    var subReadySignal = ps.subscribe("readySignal"); 

    // set up parallel shells to be simultaneously polling
    // cannot be done through benchRun itself because requires
    // each polling client to use its own subscriptionId
    for (var numOtherClients = 0; numOtherClients < maxClients; numOtherClients++) {
        var shells = [];
        var otherClients = numOtherClients;
        for (var i=0; i<otherClients; i++) {
            shells[i] = startParallelShell('var subA = ps.subscribe("A");' +
                   'var subStartSignal = ps.subscribe("startSignal");' +
                   'var ops = [{ op:"command", ns:"test", command: { poll: subA }}];' +
                    // signal that this shell is ready
                   'ps.publish("readySignal", { ready : 1 });' +
                    // wait for signal that all shells ready (signal might already be in buffer)
                   'ps.poll(subStartSignal, 1000 * 60 * 10);' +
                    // all shells ready; start benchRun
                   'benchRun({ ops: ops, host: db.getMongo().host, seconds:' + timeSecs + ', parallel: 1});' +
                   'ps.unsubscribeAll();' +
                   'quit();' 
                 );
        }

        // if we want to be simultaneously publishing messages,
        // set up a parallel shell to do so
        if (message) {
            shells[otherClients] = startPublishShell(message);
            otherClients++;
        }

        // wait for ready signals from all shells
        var readyCount = 0;
        while (readyCount < otherClients) { 
            var res = ps.poll(subReadySignal, 1000 * 60 * 10);
            if (res["messages"][subReadySignal.str]) {
                readyCount += res["messages"][subReadySignal.str]["readySignal"].length;
            } 
        }

        // signal to all shells that they can start their benchrun tests
        ps.publish("startSignal", { start : 1 });

        // start our own benchRun test
        before = db.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numOtherClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = db.serverStatus()["opcounters"]["command"];
        stats["server"]["" + numOtherClients] = (after - before)/timeSecs;

        // wait for all shells to terminate before starting next batch of tests
        for (var i=0; i<otherClients; i++)
            shells[i]();
    }

    printStats(stats);
}
