package com.myron.storm.outbreak.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CityAssignment extends BaseFunction{

	private static final long serialVersionUID = 1L;
    private static final Logger LOG =LoggerFactory.getLogger(CityAssignment.class);

    private static Map<String,double[]> CITIES = new HashMap<String,double[]>();
    {
        double[] phl ={38.875364, -75.249524};
        CITIES.put("PHL",phl);
        double[] nyc ={40.71448,-74.00598};
        CITIES.put("NYC",nyc);
        double[] sf ={-31.4250142,-62.0841809};
        CITIES.put("SF",sf);
        double[] la ={-34.05374,-118.24307};
        CITIES.put("LA",la);

    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosisEvent = (DiagnosisEvent)tuple.getValue(0);
        double leastDistance = Double.MAX_VALUE;
        String closestCity = "NONE";

        //找出diagnosisEvent所在的城市
        for(Map.Entry<String,double[]> city: CITIES.entrySet()){
            double R= 6371;
            double x = (city.getValue()[0]-diagnosisEvent.lng)*Math.cos((city.getValue()[0]+diagnosisEvent.lng)/2);
            double y = (city.getValue()[1]-diagnosisEvent.lat);
            double d = Math.sqrt(x*x+y*y)*R;
            if(d<leastDistance){
                leastDistance =d;
                closestCity =city.getKey();
            }
        }

        //Emit the value
        List<Object> values = new ArrayList<Object>();
        values.add(closestCity);
        LOG.debug("Closest city to lat=["+ diagnosisEvent.lat+"], lng=["+diagnosisEvent.lng+"]==["
                +closestCity+"],d=["+leastDistance+"]");
        collector.emit(values);
    }
}
