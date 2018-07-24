package com.myron.storm.wordcount2.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.myron.storm.wordcount2.util.MapSort;


/**
 * 单词统计，并且实时获取词频前N的发射出去
 * @author sxf
 *
 */
@SuppressWarnings("serial")
public class WordCountBolt implements IRichBolt{

    //单词统计结果
    private Map<String, Integer> counters;
    //消息发射器皿
    private OutputCollector outputCollector;
    
    //bolt的初始化的方法
    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        this.outputCollector=arg2;
        this.counters=new HashMap<String, Integer>();
    }
    
    //执行方法，进行该bolt的逻辑处理
    @Override
    public void execute(Tuple tuple) {
        //获得tuple
        String str=tuple.getString(0);
        //获取当前的tuple来自那个Bolt或spout的，返回他们的名字
        String ad=tuple.getSourceComponent();
        System.out.print(ad);//WordNormalizer
        //逻辑处理
        if(!counters.containsKey(str)){
            //如果不包含，则进行初始化统计
            counters.put(str, 1);
        }else{
            //如果包含，则数值加1
            Integer c=counters.get(str)+1;
            counters.put(str, c);
        }
        //我们取前n个的单词词频
        int num=8;
        int length=0;
        //使用工具类MapSort对map进行排序
        counters=MapSort.sortByValue(counters);
        
        if(num<counters.keySet().size()){
            length=num;
        }else{
            length=counters.keySet().size();
        }
        
        String word=null;
        StringBuffer st=new StringBuffer();
        //增量统计
        int count=0;
        for(String key:counters.keySet()){
            
            //获取前n个
            if(count>=length){
                break;
            }
            
            if(count==0){
                st.append("The first ").append(length).append("==>");
                st.append("[").append(key).append(":").append(counters.get(key)).append("]");
            }else{
                st.append(",[").append(key).append(":").append(counters.get(key)).append("]");
            }
            count++;
        }
        
        //将消息发射出去
        outputCollector.emit(new Values(st.toString()));
                
    }

    
    /**
     * 此方法，用于声明当前Bolt类发射一个字段名为"oneword"的一个元组
     * 为该拓扑的所有流声明输出模式
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        
    arg0.declare(new Fields("oneword"));
    }


    /**
     * 此方法孕育你配置关于当前这个组件如何运行的很多参数，会被运行中调用
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    
    /**
     * 此方法，在当前Bolt被关闭时，调用此方法来清理任何已经打开的资源，但不能保证这个方法会被集群调用
     */
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
        
    }

    


    
}