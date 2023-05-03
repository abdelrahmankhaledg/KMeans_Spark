package com.geekcap.javaworld.sparkexample;


import java.util.ArrayList;


public class Utilities {

    public static Double getDistance(DataPoint d1,DataPoint d2){
        double distance=  0;
        ArrayList<Double> firstCoordinates=d1.getCoordinates();
        ArrayList<Double> secondCoordinates=d2.getCoordinates();
        for(int i=0;i<firstCoordinates.size();i++){
            distance+= (firstCoordinates.get(i)- secondCoordinates.get(i))*(firstCoordinates.get(i)- secondCoordinates.get(i));
        }
        return Math.sqrt(distance);
    }

    public static DataPoint sumDataPoints(DataPoint d1,DataPoint d2){
        DataPoint result = new DataPoint();
        ArrayList<Double> firstCoordinates=d1.getCoordinates();
        ArrayList<Double> secondCoordinates=d2.getCoordinates();
        ArrayList<Double> resultCoordinates = new ArrayList<>();
        for(int i=0;i<firstCoordinates.size();i++){
            resultCoordinates.add(firstCoordinates.get(i)+secondCoordinates.get(i));
        }

        result.setCoordinates(resultCoordinates);
        return result;
    }
    public static DataPoint computeAverage(DataPoint dp,Long count){
        ArrayList<Double> coordinates = dp.getCoordinates();
        coordinates.replaceAll(aDouble -> aDouble / count);
        return dp;
    }
    public static String convertDataPointToString(DataPoint dp){
        StringBuilder dataPointBuilder=new StringBuilder();
        ArrayList<Double> coordinates=dp.getCoordinates();
        for(int i=0;i<coordinates.size();i++){
            dataPointBuilder.append(coordinates.get(i));
            if(i!= coordinates.size()-1)
                dataPointBuilder.append(",");
        }
        return dataPointBuilder.toString();
    }
}

