# Map / Reduce Simulation Tool

This a simple tool to run map / reduce algorithm by providing :
* map and reduce functions in the Javascript language (see below for the specification of the flavor of JS tu use)
* a data set in the form of a file, one entry per line, each entry being a json object
* one can define the number of mapper to use and the number of reduce phase, thus simulating map / reduce distribution 
topology.

## Usage 

Pick up the last release all in one jar (on the release page), then execute :

```
java -jar mr-1.0.2.jar --map map.js --reduce reduce.js --data-set data-set.json
```
This executes the algorithm using the defaults : 4 mappers and 2 reduce phases. 

### Defining the number of mappers

You can set the number of mappers with the --mapper-count option (the default value is 4). Here's an example with 10 concurrent mappers :


```
java -jar mr-1.0.1.jar --mapper-count 10 --map map.js --reduce reduce.js --data-set data-set.json
```

### Defining the number of mappers

You can set the number of reduce phases with the --reduce-phases option. Here's an example with 10 concurrent mappers and 4 reduce phases :

```
java -jar mr-1.0.1.jar --mapper-count 10 --reduce-phases 4 --map map.js --reduce reduce.js --data-set data-set.json
```

## Map and Reduce functions syntax

Map and reduce function must be written in JS syntax to be executed by the nashorn engine (embedded in Java 8).

The map and reduce signature must be as follows.

### The map function

```
function map(value) {
    /* ... */
    emit(key, {some: 'object'});
    /* ... */
}
```

The emit function takes two parameters, (1) the key to emit for, (2) the value to be emitted.
* the key must be a string
* the value must be a javascript object that can be converted to / from json.

### The reduce function

```
function reduce(values) {
    /* ... */
    for each(var value in values) {
        /* ... */
    }
    /* ... */
    return {some: 'reduced object'}
}
```

Note the use of the 'for each' construct. This is a particular nashorn engine extension to ease iterating over lists.

More on nashorn extensions on [this wiki page](https://wiki.openjdk.java.net/display/Nashorn/Nashorn+extensions).

## The data set

Here is an example of the content of a data set file :


```json
{"date":"2019-03-08T02:04:54Z","region":"Cameroon","id_publicite":"cj-9919"}
{"date":"2019-03-07T20:49:27Z","region":"Germany","id_publicite":"yz-0223"}
{"date":"2019-04-24T03:01:26Z","region":"Indonesia","id_publicite":"bo-1924"}
{"date":"2019-01-02T16:57:44Z","region":"Argentina","id_publicite":"xu-1247"}
{"date":"2019-05-31T08:10:45Z","region":"Sri Lanka","id_publicite":"xq-1471"}
{"date":"2019-04-22T07:08:11Z","region":"Indonesia","id_publicite":"km-2723"}
{"date":"2019-05-05T19:55:49Z","region":"Guadeloupe","id_publicite":"cb-2839"}
```
## Functions accessible from the javascript code

### Date manipulation

For easing the use of dates, some functions are given to manipulate string representation of date (in the ISO 8601 format, i.e., 2019-02-04T05:00:59Z) as they where structured data.

#### Extracting date field

``` 
dateYear('2019-02-04T05:00:59Z') == 2019
dateMonth('2019-02-04T05:00:59Z') == 2
dateDay('2019-02-04T05:00:59Z') == 4
dateHour('2019-02-04T05:00:59Z') == 5
dateMinute('2019-02-04T05:00:59Z') == 0
dateSecond('2019-02-04T05:00:59Z') == 59
```

#### Formatting a date
``` 
dateFormat('2019-02-04T05:00:59Z', 'yyyy-MM') == '2019-02'
```

The format string must be a valid [java date / time formatter pattern](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns).

### Splitting a phrase into words
``` 
words('2019-02-04T05:00:59Z', 'yyyy-MM') == '2019-02'
var wordList = words('this is a phrase');
for each(var word in wordList) {
    emit(word, {cnt: 1});
}
```
The preceeding code will emit the four words of 'this is a phrase'.