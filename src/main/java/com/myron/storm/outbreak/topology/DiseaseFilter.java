package com.myron.storm.outbreak.topology;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 过滤不需要的tuple
 * @author Administrator
 *
 */
public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);


    @Override
    public boolean isKeep(TridentTuple tuple) {
        DiagnosisEvent diagnosis = (DiagnosisEvent)tuple.getValue(0);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        //过滤出 code小于322的疾病
        if(code.intValue() <=322){
            LOG.debug("Emitting disease ["+diagnosis.diagnosisCode+"]");
            return true;
        }else{
            LOG.debug("Filtering disease ["+diagnosis.diagnosisCode+"]");
            return false;
        }
    }
}