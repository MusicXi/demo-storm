package com.myron.storm.wordcount2.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


/**
 * 打印接受的数据的Bolt
 * @author sxf
 *
 */
@SuppressWarnings("serial")
public class PrintBolt extends BaseBasicBolt {

    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector arg1) {
        //接收数据date
        try {
            String mesg=tuple.getString(0);
            if(mesg!=null){
                System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date())+"===>"+mesg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    /**
     * 此方法，用于声明当前Bolt类发射什么字段的元组，当前bolt只是打印，不再发射元组，所以不用定义
     * 为该拓扑的所有流声明输出模式
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    
    

    
}