package com.geekcap.javaworld.sparkexample;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class RDDComparator implements Comparator<Tuple2<DataPoint, Double>>, Serializable {
@Override
    public int compare(Tuple2<DataPoint, Double> centroidWithDistance1, Tuple2<DataPoint, Double> centroidWithDistance2) {
            return centroidWithDistance1._2.compareTo(centroidWithDistance2._2);
    }
}
