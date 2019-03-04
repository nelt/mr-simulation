function reduce(values) {
    var result = {cnt: 0, avg: 0};

    for each (var value in values) {
        result.avg = (result.avg * result.cnt + value.avg * value.cnt) / (result.cnt + value.cnt);
        result.cnt += value.cnt;
    }

    return result;
}