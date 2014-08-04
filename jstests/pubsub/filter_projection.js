// subscribe without filter or projection
var subRegular = ps.subscribe("A");

// subscribe with filter and projection
var subBoth = ps.subscribe("A", { count: { $gt: 3 } }, { count: 1 });

// publish
for(var i=0; i<6; i++){
    ps.publish("A", { body : "hello", count : i });
    // ensure that this test, which depends on in-order delivery,
    // runs on systems with only millisecond granularity
    sleep(1);
}

// poll on all subscriptions as array
var arr = [subRegular, subBoth];
var res = ps.poll(arr);

// verify correct messages received
var resRegular = res["messages"][subRegular.str]["A"];
for(var i=0; i<6; i++)
    assert.eq(resRegular[i]["count"], i);

var resBoth = res["messages"][subBoth.str]["A"];
for(var i=0; i<2; i++){
    assert.eq(resBoth[i]["count"], i+4);
    assert.eq(resBoth[i]["body"], undefined);
}
