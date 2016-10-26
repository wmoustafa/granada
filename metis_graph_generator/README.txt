**********************************
How to use command line arguments:
***********************************
1st argument is the filename in Snap format. The Snap format has rows of edges like v1 v2.
2nd argument is the probability of sampling edges (in case you want a smaller network)
3rd argument is the number of level 1 partitions
4th argument is the number of level 2 partitions (Use 1 for this parameter)
5th argument is the number of level 3 partitions
6th argument is the number of labels used to label vertices (every vertex has a label which 
we may use as part of a predicate on that vertex, e.g., red, green, etc). (Use 5 for this parameter)
7th argument is the weights on edges. That can be: unit, uniform, skewed_low, skewed_high.
8th argument denotes whether the generated graph should be a DAG
9th argument should always be true
***********************************

The total number of super-vertices is the result of multiplying all the levels of paritions. 
Then, the vertices each super-vertex contains is N/#super-vertices where N is the size of the input graph.

***********************************
Sample command:

java -cp fastutil-7.0.9.jar:gson-2.5.jar:. -Xms26g -Xmx26g io/SnapToPartitionedGiraph1 /home/vpapavas/giraph_paper/datasets/cit-Patents.txt 1 5 1 200 5 uniform false true

Note that the total number of super-vertices is: 5*200 = 1000

***********************************

Update of February 9: Use the command below

***********************************
java -cp fastutil-7.0.9.jar:gson-2.5.jar:. -Xms26g -Xmx26g io/SnapToPartitionedGiraph1 /home/vpapavas/giraph_paper/datasets/cit-Patents.txt 1 1000 1 1 5 uniform false true

***********************************

The generator produces two files, one to be used as input to the Datalog engine and the other for plain Giraph analytics. 
The files for the Datalog engine have the extension "datalog.txt" whereas for plain Giraph, they have the extension
"csv",
