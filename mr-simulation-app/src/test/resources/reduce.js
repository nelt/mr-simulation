function reduce(values) {
    var result = {cnt: 0};

    for each (var value in values) {
        result.cnt += value.cnt;
    }

    return result;
}