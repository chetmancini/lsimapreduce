
LSI MapReduce
===============================

* _Cornell University_
* CS5300
* Assignment 5

This project is to run MapReduce on Amazon Web Services.

Members
===============================
* Hugo Hache    hrh64
* Chester Mancini cam479
* Sean Ogden    spo38

Overall Structure
==============================

Parameters:
  * size
  * columnWidth
  * defaultDensity

## Pass 1
### Map
  - Input: [LongWritable, Text]
  - Description: Transmit only the cell containing a tree and compute their column id for each of them
  - Output: [IntWritable, IntIntWritableTuple] (<idcolumn;<localidcell,localidcell>>)
 
### Reduce
  - Input: [IntWritable, IntIntWritableTuple] (<idcolumn;<localidcell,localidcell>>)
  - Description: Compute the Union Find on one column group (including the 2 boundary columns)
  - Output: [IntIntWritableTuple, IntIntWritableTuple] (<<idColumnGp,localCellId>,<idColumGp,localParentId>>)

--> Generate counters: # vertices, # edges

## Pass 2
### Map
  - Input: [LongWritable, Text] (result first pass)
  - Description: Transmit only the cells in boundary columns to an unique reducer
  - Output: [Text, IntIntIntWritableTuple] <someCommonKeyForAll;<idcolumnGp,localidcell,localidparent>>

### Reduce
  - Input: [Text, IntIntIntWritableTuple] <someCommonKeyForAll;<idcolumnGp,localidcell,localidparent>>
  - Description: Compute the Union Find on the set of all the boundaries columns
  - Output: [IntWritable, IntIntWritableTuple]

## Pass 3
### Map
  - Input: [LongWritable, Text] (result first and second pass)
  - Description: Compute column group id and transmit all except only the left boundary column of each group (except if the last column group is perfectly ending, in this case transmit the right boundary column of this last group)
  - Output: [IntIntWritableTuple, IntIntWritableTuple] <<idColumngp,localidcell>,<idColumngp,localidparent>>

### Reduce
  - Input: [IntIntWritableTuple, IntIntWritableTuple] <<idColumngp,localidcell>,<idColumngp,localidparent>>
  - Description: Compute Union find on each column group
  - Output: [IntWritable, IntWritable] <parentGlobalIdofSingleCC,sizeOfSingleCC>

## Pass 4
### Map
  - Input: [LongWritable, Text] (output third pass)
  - Description: Group sizes by parentGlobalId
  - Output: [IntWritable, IntWritable] <parentGlobalIdofSingleCC,sizeOfSingleCC>

### Reduce
  - Input: [IntWritable, IntIntWritableTuple]
  - Description: Sum the sizes corresponding to the same root
  - Output: [IntWritable, IntWritable] <globalIdRoot,globalSizeConnectedComponent>
 
--> Generate counters : # single CC, sum size CC, sum square(size) CC

Output (non-diagonal)
==============================
 * number of vertices:      58997294                      
 * number of edges:         69608194
 * number of distinct CC:   2897988
 * avg size CC:             20.358019
 * (weighted) avg size CC:  7543.9851
 * average burn count:      4450.74708

Parameters
==============================
* NetID:                cam479 ==> "974"
* wMin:                 0.3896
* wLimit:               0.9796 
* Num Grid Points Used: 10k x 10k
* EC Critical Density:  0.4

Appendix (Data used to determine critical density)
==============================

    Density Nbr CC  Avg size CC
    0.1   64015   1.5593688
    0.2   72380   2.7579026
        
    0.3   47948   6.254046
    0.4   17446   22.91918
    0.33  38148   8.649235
    0.36  28463   12.64958
    0.28  54163   5.1685653
    0.38  22574   16.83286
    0.43  11444   37.56484
    0.46  7364    62.428165
    0.49  4641    105.563675
    0.52  2823    184.25789
    0.55  1728    318.365162037037
    0.58  1010    574.261386138614
    0.475 5874    80.8592100783112