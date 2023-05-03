/**
* Illustrates a wordcount in Java
*/
package com.geekcap.javaworld.sparkexample;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

public class K_Means {
	static List<DataPoint> centroids;


	public static void main(String[] args) throws Exception {

		String inputFile = args[0];
		String initialCentroidsOutputFile = args[1];
		String clusterOutputFile = args[2];
		String centroidsOutputFile = args[3];
		String noIterationsFile = args[4];

		int number_of_centroids = 3;
		double equalityThresh = 1e-15;
		int no_iterations=0;
		long start = System.currentTimeMillis();
		long finish = 0;
		int max_iterations=300;


		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setMaster("local").setAppName("KMeans");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("ERROR");
		Logger.getRootLogger().setLevel(Level.toLevel("ERROR"));
		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);
		// Split up into Points.
		JavaRDD<DataPoint> points = input.map(new Function<String, DataPoint>() {
			public DataPoint call(String line) {
				return new DataPoint(line,1);
			}
		});


		centroids = points.takeSample(false,number_of_centroids);

		JavaRDD<DataPoint> centroidsRDD = sc.parallelize(centroids);
		centroidsRDD.saveAsTextFile(initialCentroidsOutputFile);

		for(int i=0;i<max_iterations;i++) {


			no_iterations++;



            /*Compute the distance between each DataPoint and all the centroids and assign each point to
			its corresponding centroid.
			It returns a pair in which the key is the centroid and the value a point that belongs to the cluster
			of that centroid*/
			JavaPairRDD<DataPoint,DataPoint> centroidToPointInCluster = points.mapToPair(new PairFunction<DataPoint, DataPoint, DataPoint>() {
				public Tuple2<DataPoint,DataPoint> call(DataPoint point) {
					//Computes the distance between the point and each centroid.
					double minDistance = Double.MAX_VALUE;
					DataPoint minDistanceCentroid = null;
					for (DataPoint centroid : centroids) {
						double distance = Utilities.getDistance(point, centroid);
						if(distance < minDistance){
							minDistance = distance;
							minDistanceCentroid = centroid;
						}
					}
					return new Tuple2<>(minDistanceCentroid,point);
				}
			});


			//Compute the sum of the points in the cluster.
			JavaPairRDD<DataPoint,DataPoint> centroidToSumOfPointsInCluster = centroidToPointInCluster.reduceByKey(new Function2<DataPoint, DataPoint,DataPoint>() {
				public DataPoint call(DataPoint dp1,DataPoint dp2) {
					return Utilities.sumDataPoints(dp1,dp2);
				}
			});


			//Count the number of points in each cluster.
			Map<String,Object> centroidToNumberOfPointsInCluster = centroidToPointInCluster.mapToPair(new PairFunction<Tuple2<DataPoint,DataPoint>, String,Integer>() {
				public Tuple2<String,Integer> call(Tuple2<DataPoint,DataPoint> centroidToPoint) {
					return new Tuple2<>(Utilities.convertDataPointToString(centroidToPoint._1),1);
				}
			}).countByKey();


			//Get new centroids and the distance between them and old centroids.
			JavaPairRDD<DataPoint,Double>  newCentroidWithDistanceToOldCentroid = centroidToSumOfPointsInCluster.mapToPair(new PairFunction<Tuple2<DataPoint,DataPoint>, DataPoint,Double>() {
				public Tuple2<DataPoint,Double> call(Tuple2<DataPoint,DataPoint> oldCentroidToSumOfPoints) {
					DataPoint newCentroid = Utilities.computeAverage(oldCentroidToSumOfPoints._2,(Long)centroidToNumberOfPointsInCluster.get(Utilities.convertDataPointToString(oldCentroidToSumOfPoints._1)));
					return new Tuple2<>(newCentroid,Utilities.getDistance(newCentroid,oldCentroidToSumOfPoints._1));
				}
			});


			//Get the new centroids without the distances between them and the old centroids.
			JavaRDD<DataPoint> newCentroids = newCentroidWithDistanceToOldCentroid.map(new Function<Tuple2<DataPoint, Double>, DataPoint>() {
				public DataPoint call(Tuple2<DataPoint, Double> newCentroidWithDistance)  {
					return newCentroidWithDistance._1;
				}
			});

			Double minDistanceBetweenCentroids= newCentroidWithDistanceToOldCentroid.min(new RDDComparator())._2;

			centroids = newCentroids.collect();


			if(minDistanceBetweenCentroids.compareTo(equalityThresh)<0){
				JavaPairRDD<String, String> centroidToStringPoint = centroidToPointInCluster.mapToPair(new PairFunction<Tuple2<DataPoint, DataPoint>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<DataPoint, DataPoint> centroidToPoint) throws Exception {
						return new Tuple2<>(Utilities.convertDataPointToString(centroidToPoint._1)+"#",Utilities.convertDataPointToString(centroidToPoint._2));
					}
				});

				JavaPairRDD<String, String> clusters = centroidToStringPoint.reduceByKey(new Function2<String, String, String>(){
					public String call(String p1, String p2){
						return p1 + "#" + p2;
					}
				});

				//Finish doesn't include the time needed to write the result to disk.
				finish = System.currentTimeMillis();
				clusters.saveAsTextFile(clusterOutputFile);
				newCentroids.saveAsTextFile(centroidsOutputFile);
				break;
			}

		}

		//This time doesn't include the time to write the results to disk.
		long timeElapsed = finish - start;
		BufferedWriter writer = new BufferedWriter(new FileWriter(noIterationsFile));
		writer.write(no_iterations+" Iterations took "+timeElapsed/1000+" seconds" );
		writer.close();
	}
}
