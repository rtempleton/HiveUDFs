# Hive UDFs collection

### TimeMachine
TimeMachine is a UDF that assists in grouping records into "buckets" when performing roll-up aggregations. it also aids in generating timestamps for where clauses that help ensure the data being grouped lands on an aggregate time boundary. This allows a user to enter an arbitrary time (like the current time) and ensure that a rounded timestamp number is generated.

#### timemachine(timestamp input, int bucketSize , int bucketCount)
- input - The timestamp field/value for the basis of this function
- bucketSize - number of minutes of the hour this will round off to (must be an int value > 0)
- bucketCount - the number of buckets in the past or future this will round off to. A bucket count of 0 (zero) means the current bucket, positive/negative numbers will add/subtract the count*size minutes from the current bucket.

Examples:

select current_timestamp;
> 2017-04-17 16:26:13

select timemachine(current_timestamp, 5, 0); --get the 5 minute time bucket the "current time" falls in
> 2017-04-17 16:25:00

select timemachine(current_timestamp, 10, -1); --current 10 minute time bucket minus 10 minutes
> 2017-04-17 16:10:00

select timemachine(current_timestamp, 20, 3); --current 20 minute time bucket plus 60 minutes
> 2017-04-17 17:20:00

select timemachine(ts\_col, 15, 0) as ts, count(*) group by timemachine(ts\_col, 15, 0) order by ts desc;
>2017-04-17 16:15:00, 145 <br>
2017-04-17 16:00:00, 345 <br>
2017-04-17 15:45:00, 234 <br>
2017-04-17 15:30:00, 98 <br>
