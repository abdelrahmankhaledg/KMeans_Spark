package com.geekcap.javaworld.sparkexample;

import java.io.Serializable;
import java.util.ArrayList;

public class DataPoint implements Serializable {
    ArrayList<Double> coordinates;
    DataPoint(){}
    DataPoint(String coordinatesString,int ignore){
        this.coordinates=new ArrayList<>();
        String[] splitCentroidStrings=coordinatesString.split(",");
        for (int i=0;i<splitCentroidStrings.length-ignore;i++){
            this.coordinates.add(Double.valueOf(splitCentroidStrings[i]));
        }
    }
    public ArrayList<Double> getCoordinates(){
        return coordinates;
    }
    public void setCoordinates(ArrayList<Double> coordinates){
        this.coordinates=coordinates;
    }
    @Override
    public String toString(){
        return Utilities.convertDataPointToString(this);
    }
}