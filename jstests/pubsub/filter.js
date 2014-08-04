// subscribe without filter
var subRegular = ps.subscribe("A");

// subscribe with filter
var subFilter = ps.subscribe("A", { count: { $gt: 3 } });

// publish
for(var i=0; i<6; i++){
    ps.publish("A", { body : "hello", count : i });
    // ensure that this test, which depends on in-order delivery,
    // works on systems with only millisecond granularity
    sleep(1);
}

// poll on all subscriptions as array
var arr = [subRegular, subFilter];
var res = ps.poll(arr);

// verify correct messages received
var resRegular = res["messages"][subRegular.str]["A"];
for(var i=0; i<6; i++)
    assert.eq(resRegular[i]["count"], i);

var resFilter = res["messages"][subFilter.str]["A"];
for(var i=0; i<2; i++)
    assert.eq(resFilter[i]["count"], i+4);
