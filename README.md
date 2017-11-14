# RecommenderItemCF
Recommender System based on item collaborative filtering running on Hadoop
#### This is a chain of five map-reduce jobs.
**Input sample:**  
user_id, movie_id, rating  
1488844,45,3  
822109,55,5  
885013,1,4  
30878,12,4  
823519,22,3  
893988,66,3  
124105,8,4  
1248029,7,3  
1842128,21,4  
2238063,13,3  
1503895,44,4  
2207774,7,5  
2590061,2,3  
2442,6,3  
….  
#### Map-reduce job 1: Divide data by user_id
**Input:**  
rows of user_id, movie_id, rating  
**Output:**  
user_id1\t< movie_A:rating, movie_B:rating, …>  
user_id2\t< movie_B:rating, movie_C:rating, …>  
user_id3\t< movie_A:rating, movie_C:rating, …>  
…
#### Map-reduce job2: Build Co-occurrence Matrix
**Input:**  
rows of user_id \t movie_A:rating, movie_B:rating, movie_C:rating, …  
**Output:**  
movie_A:movie_B \t relation (number of co-occurrences)  
movie_A:movie_C \t relation    
movie_B:movie_C \t relation    
…
#### Map-reduce job 3: Normalize co-occurrence matrix
**Input:**  
rows of movie_id:movie_id \t relation  
**Output:**  
movie_A \t movie_B=relation_normed  
movie_A \t movie_C=relation_normed  
movie_B \t movie_C=relation_normed  
…
#### Map-reduce job 4: Multiply normalized co-occurrence matrix and rating matrix
**Mapper1 input:**  
rows of moive_id \t moive_id=relation_normed  
**Mapper 2 input:**  
rows of user_id, movie_id, rating  
**Output:**  
user_id1:movie_A \t rating_partial  
user_id2:movie_A \t rating_partial  
user_id1:movie_B \t rating_partial  
user_id1:movie_A \t rating_partial  
user_id2:movie_C \t rating_partial  
user_id2:movie_A \t rating_partial  
…
#### Map-reduce job 5: Sum up partial ratings
**Input:**  
rows of user_id:movie_id \t rating_partial  
**Output:**  
user_id1:movie_A \t rating  
user_id1:movie_B \t rating  
user_id1:movie_C \t rating  
user_id2:movie_A \t rating  
user_id2:movie_B \t rating
…  
#### Run
hdfs dfs -mkdir /input  
hdfs dfs -put input/* /input  
hadoop com.sun.tools.javac.Main *.java  
jar cf recommender.jar *.class  
hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /normalize /multiplication /sum  
hdfs dfs -cat /sum/*  
// args0: original input directory  
// args1 – args5: output directories for map-reduce jobs 1-5  

#### Maven version
https://github.com/fanchi/RecommenderItemCFMaven