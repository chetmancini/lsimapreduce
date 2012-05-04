
LSI MapReduce
===============================

* _Cornell University_
* CS5300
* Assignment 5

This project is to run MapReduce on Amazon Web Services.

Members
===============================
* Hugo Hache			hrh64
* Chester Mancini		cam479
* Sean Ogden			spo38

Overall Structure
==============================

Parameters:
  * size
  * columnWidth
  * defaultDensity

## Pass 1
### Map
  - Input: <LongWritable, Text>
  - Description:
  - Output: <IntWritable, IntIntWritableTuple>
 
### Reduce
  - Input: <IntWritable, IntIntWritableTuple>
  - Description:
  - Output: <IntWritable, IntIntWritableTuple>

## Pass 2
### Map
  - Input: <LongWritable, Text>
  - Description:
  - Output: <Text, IntIntIntWritableTuple>

### Reduce
  - Input: <Text, IntIntIntWritableTuple>
  - Description:
  - Output: <IntWritable, IntIntWritableTuple>

## Pass 3
### Map
  - Input: <LongWritable, Text>
  - Description:
  - Output: <IntWritable, IntIntWritableTuple>

### Reduce
  - Input: <IntWritable, IntIntWritableTuple>
  - Description:
  - Output: <IntWritable, IntWritable>

Output
==============================


Parameters
==============================
* NetID: cam479 "974"
* wMin:
* wLimit:
* Number of Grid Points Processed:
* Extra Credit Critical Density: 




