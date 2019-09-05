require('should')

it("#01. test pengurangan", function (done) {
    var result = 1 - 2;

    result.should.equal(-1);

    done();
});