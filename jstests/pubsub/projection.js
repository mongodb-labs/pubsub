// subscribe without projection
var subRegular = ps.subscribe("A");

// subscribe with projection
var subProjection = ps.subscribe("A", null, { count: 1 });

// publish
for(var i=0; i<6; i++){
    ps.publish("A", { body : "hello", count : i });
    // ensure that this test, which depends on in-order delivery,
    // works on systems with only millisecond granularity
    sleep(1);
}

// poll on subscriptions as array
var arr = [subRegular, subProjection];
var res = ps.poll(arr);

// verify correct messages received
var resRegular = res["messages"][subRegular.str]["A"];
for(var i=0; i<6; i++)
    assert.eq(resRegular[i]["count"], i);

var resProjection = res["messages"][subProjection.str]["A"];
for(var i=0; i<6; i++){
    assert.eq(resProjection[i]["count"], i);
    assert.eq(resProjection[i]["body"], undefined);
}
