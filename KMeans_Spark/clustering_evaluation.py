# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
import sys
from sklearn.cluster import KMeans


ground_truth_file_path = sys.argv[1]
initial_centroids_path = sys.argv[2]
final_clusters_path = sys.argv[3]


def read_ground_truth(ground_truth_file_path="/content/iris.data"):
  ground_truth_by_label={"Iris-setosa":[],"Iris-virginica":[],"Iris-versicolor":[]}
  ground_truth_by_datapoint={}
  with open(ground_truth_file_path)as f:
    for datapoint in f :
      ground_truth_by_label[datapoint[datapoint.rfind(',')+1:-1]].append(datapoint[:datapoint.rfind(',')])
      ground_truth_by_datapoint[datapoint[:datapoint.rfind(',')]]=(datapoint[datapoint.rfind(',')+1:-1])

  return ground_truth_by_label,ground_truth_by_datapoint
  



def read_clusters(clusters_file_path="/content/part-r-00000",number_of_clusters=3):
  clusters_by_label={}
  clusters_by_datapoint={}
  for i in range(number_of_clusters):
    clusters_by_label["Cluster_"+str(i)]=[]
  centroids=[]
  with open(clusters_file_path)as f:
    cluster_number=0
    for datapoint in f :
      datapoint = datapoint[1:-2]
      centroid=datapoint.split('#,')[0]
      cluster_points=datapoint.split('#,')[1].split('#')
      centroids.append(centroid)
      for i in range(len(cluster_points)):
        clusters_by_label["Cluster_"+str(cluster_number)].append(cluster_points[i])
        clusters_by_datapoint[cluster_points[i]]="Cluster_"+str(cluster_number)
      cluster_number+=1
  return clusters_by_label,clusters_by_datapoint,centroids


def assign_clusters_and_compute_metrics(clusters_by_label,clusters_by_datapoint,ground_truth_by_label,ground_truth_by_datapoint,number_of_clusters=3):
  clusters_assignment={}
  clusters_precision_recall={}
  for i in range(number_of_clusters):
    clusters_assignment["Cluster_"+str(i)]=""
  
  for i in range(number_of_clusters):
    clusters_precision_recall["Cluster_"+str(i)]=[]

  for i in range(number_of_clusters):
    counts={"Iris-setosa":0,"Iris-virginica":0,"Iris-versicolor":0} #Will hold the number of points from the cluster that correspond to each groundtruth label
    datapoints_in_cluster=clusters_by_label["Cluster_"+str(i)]
    for datapoint in datapoints_in_cluster:
        counts[ground_truth_by_datapoint[datapoint]]+=1
    
    total=counts["Iris-setosa"]+counts["Iris-virginica"]+counts["Iris-versicolor"]
    max_count=max(counts["Iris-setosa"],counts["Iris-virginica"],counts["Iris-versicolor"])
    
    if(max_count==counts["Iris-virginica"]):
      clusters_assignment["Cluster_"+str(i)]="Iris-virginica"
    elif(max_count==counts["Iris-versicolor"]):
      clusters_assignment["Cluster_"+str(i)]="Iris-versicolor"
    else:
      clusters_assignment["Cluster_"+str(i)]="Iris-setosa"
    
    precision=max_count/total
    clusters_precision_recall["Cluster_"+str(i)].append(precision)
  

  for i in range(number_of_clusters):
    datapoints_in_cluster=clusters_by_label["Cluster_"+str(i)]
    correctly_clustered=0
    for datapoint in datapoints_in_cluster:
      if(ground_truth_by_datapoint[datapoint]==clusters_assignment["Cluster_"+str(i)]):
        correctly_clustered+=1
    
    recall=correctly_clustered/len(ground_truth_by_label[clusters_assignment["Cluster_"+str(i)]])
    clusters_precision_recall["Cluster_"+str(i)].append(recall)
    
  return clusters_assignment,clusters_precision_recall




def compute_f_score(precision,recall):
  return (2*precision*recall/(precision+recall))

def print_results(clusters_assignment,clusters_precision_recall,number_of_clusters=3):
  for i in range(number_of_clusters):
    cluster_name="Cluster_"+str(i)
    space="         " #5tabs
    print(cluster_name+" : "+clusters_assignment[cluster_name]+space+
          "Precision : "+str(clusters_precision_recall[cluster_name][0])+space+"Recall : "+str(clusters_precision_recall[cluster_name][1])
          +space+"F_Score : "+str(compute_f_score(clusters_precision_recall[cluster_name][0],clusters_precision_recall[cluster_name][1])))




"""# Unparalleled KMeans

### Reference https://medium.com/analytics-vidhya/clustering-on-iris-dataset-in-python-using-k-means-4735b181affe
"""

def unparalleled_kmeans(data,initial_centroids):
  kmeans=KMeans(n_clusters=initial_centroids.shape[0],init=initial_centroids,n_init=1,max_iter=100)
  return kmeans,kmeans.fit_predict(data)
  




def read_initial_centroids(initial_centroids_path):
	initial_centroids = []
	with open(initial_centroids_path) as f :
		for centroid in f :
			initial_centroids.append([float(coordinate) for coordinate in centroid.split(',')])
	return np.array(initial_centroids)
	


def convert_list_to_string(ls):
  return ''.join(str(ls[i])+"," for i in range(len(ls)))[:-1]


def get_clusters_from_unparalleled(data,clustering_result,number_of_clusters=3):
  clusters_by_label_unparalleled={}
  clusters_by_datapoint_unparalleled={}
  for i in range(number_of_clusters):
    clusters_by_label_unparalleled["Cluster_"+str(i)]=[]

  for i in range(len(data)):
    clusters_by_label_unparalleled["Cluster_"+str(clustering_result[i])].append(convert_list_to_string(data[i]))
    clusters_by_datapoint_unparalleled[convert_list_to_string(data[i])]="Cluster_"+str(clustering_result[i])
  return clusters_by_label_unparalleled,clusters_by_datapoint_unparalleled


#Paralleled
ground_truth_by_label,ground_truth_by_datapoint=read_ground_truth(ground_truth_file_path)
clusters_by_label,clusters_by_datapoint,centroids=read_clusters(final_clusters_path)
clusters_assignment,clusters_precision_recall=assign_clusters_and_compute_metrics(clusters_by_label,clusters_by_datapoint,ground_truth_by_label,ground_truth_by_datapoint)
print("PARALLELED KMEANS")
print()
print_results(clusters_assignment,clusters_precision_recall)
print()
#Unparalleled
df_iris=pd.read_csv(ground_truth_file_path,names=["Dim1","Dim2","Dim3","Dim4","Label"])
data=df_iris.iloc[:,[0,1,2,3]].values
initial_centroids=read_initial_centroids(initial_centroids_path)
kmeans,clustering_result=unparalleled_kmeans(data,initial_centroids)
clusters_by_label_unparalleled,clusters_by_datapoint_unparalleled=get_clusters_from_unparalleled(data,clustering_result)
clusters_assignment_unparalleled,clusters_precision_recall_unparalleled=assign_clusters_and_compute_metrics(clusters_by_label_unparalleled,clusters_by_datapoint_unparalleled,ground_truth_by_label,ground_truth_by_datapoint)
print("UNPARALLELED KMEANS")
print()
print_results(clusters_assignment_unparalleled,clusters_precision_recall_unparalleled)
print()
clusters_by_label_unparalleled['Cluster_0'].sort()
clusters_by_label['Cluster_0'].sort()
clusters_by_label_unparalleled['Cluster_1'].sort()
clusters_by_label['Cluster_1'].sort()
clusters_by_label_unparalleled['Cluster_2'].sort()
clusters_by_label['Cluster_2'].sort()
print(clusters_by_label_unparalleled)
print()
print(clusters_by_label)
print()
"""# Measure Running Time of Unparalleled KMeans"""
import time
sum=0
no_iterations=10
for i in range(no_iterations):
  start=time.time()
  unparalleled_kmeans(data,initial_centroids)
  end=time.time()
  sum+=end-start

print("The average running time of the unparalleled KMeans is "+str(sum/no_iterations)+" seconds")
