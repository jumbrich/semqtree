#!/bin/bash

#input data [sorted spoc.idx]
INPUT=
HEADER
JAR=dist/semqtree.jar

DAT=
mkdir $DAT

LOG=$DAT/logs
mkdir $LOG

#QUERIES

BENCH=$DAT/bench
mkdir $BENCH

QTREE_OUT=$DAT/qtrees
mkdir $QTREE_OUT  
echo "Starting QTree benchmark"
echo "input data: $INPUT"

for HASH in prefix mixed
do
	echo "Selected hash function is: $HASH"

	for BUCKETS in 5000 10000 50000
	do 
		for FANOUT in 10 20 30 
		do
			for MAX_DIM in 10000 50000 100000
			do
				PREFIX=$BUCKETS.$FANOUT.$MAX_DIM.$HASH
				echo "Building QTree from $INPUT with $BUCKETS buckets, $FANOUT fanout and $MAX_DIM maxDim"
				echo "Serialise QTree to: $QTREE_OUT"
				BENCHMARK=$BENCH/$PREFIX
				mkdir $BENCHMARK
				echo "Benchmark at: $BENCHMARK"
				java -Xmx2G -jar $JAR Build -h $HASH -b $BUCKETS -f $FANOUT -m $MAX_DIM -o $QTREE_OUT -i $INPUT -if idx -bench $BENCHMARK 1>$LOG/$PREFIX.build.stdout 2>$LOG/$PREFIX.build.stderr
			done
		done
	done
done


echo "Generating sample queries"
QUERY_SIZE = 100
for depth in 0 1 2 3 
do
	echo "Sample query : Path depth: $depth size: $QUERY_SIZE"
	java -jar $JAR RWJG -d $depth -s $QUERY_SIZE -i $INPUT -o $QUERIES/path.d-$depth.$QUERY_SIZE.queries 1>$LOG/path.d-$depth.$QUERY_SIZE.queries.stdout 2>$LOG/path.d-$depth.$QUERY_SIZE.queries.stderr
	echo "Sample query : Star leafs: $depth size: $QUERY_SIZE"
	java -jar $JAR StarShapeQueryGeneration -l $depth -s $QUERY_SIZE -i $INPUT -o $QUERIES/star.d-$depth.$QUERY_SIZE.queries 1>$LOG/star.d-$depth.$QUERY_SIZE.queries.stdout 2>$LOG/star.d-$depth.$QUERY_SIZE.queries.stderr
done

for qtree in `ls -1 $QTREE_OUT`
do	
	for query in `ls -1 $QUERIES`
	do
   		mkdir $DAT/$qtree.$query
		java -Xmx2G  -jar $JAR Joins -i $QTREE_OUT/$qtree -j $QUERIES/$query.joinorder -l $QUERIES/$query -o $DAT/$qtree.$query -if idx 1>$LOG/$qtree.$query.query.stdout 2>$LOG/$qtree.$query.query.stderr
		 java -Xmx2G -jar $JAR QueryDW -sq $DAT/$qtree.$query/$qtree.$query.queries  -r $HEADER -j $QUERIES/$query.joinorder -l $QUERIES/$query -o $BENCHMARK/$qtree.$query -d $INPUT  -if idx 1>$LOG/$qtree.$query.eval.stdout 2>$LOG/$qtree.$query.eval.stderr 
	done
done



