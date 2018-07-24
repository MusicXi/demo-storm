package com.myron.storm.outbreak.topology;

import java.io.Serializable;

/**
 * 诊断事件对象
 * @author Administrator
 *
 */
public class DiagnosisEvent implements Serializable {
    private static final long serialVersionUID =1L;
    public double lat ;
    public double lng ;
    public long time ;
    public String diagnosisCode ;

    public DiagnosisEvent(double lat ,double lng ,long time, String diagnosisCode){
        super();
        this.time=time;
        this.lat = lat ;
        this.lng = lng ;
        this.diagnosisCode = diagnosisCode;
    }
}