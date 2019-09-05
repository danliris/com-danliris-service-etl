function test(name, path) {
    describe(name, function () {
        require(path);
    });
}

describe('#Azure Funtion', function (done) {
    test('INITIAL TEST', './initial-test-tests');
    test('ADE TEST', './ade-tests');
});