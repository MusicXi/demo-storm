package com.myron.storm.wordcount2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.myron.storm.wordcount2.bolt.PrintBolt;
import com.myron.storm.wordcount2.bolt.WordCountBolt;
import com.myron.storm.wordcount2.bolt.WordNormalizerBolt;
import com.myron.storm.wordcount2.spout.RandomSentenceSpout;



public class WordCountTopology {

    private static TopologyBuilder builder=new TopologyBuilder();

    public static void main(String[] args) throws InterruptedException {
        
        
        Config config=new Config();
        
        builder.setSpout("RandomSentence", new RandomSentenceSpout(),2);//即对应两个executor线程
        builder.setBolt("WordNormalizer", new WordNormalizerBolt(),2).shuffleGrouping("RandomSentence");
        builder.setBolt("WordCount", new WordCountBolt(),2).fieldsGrouping("WordNormalizer", new Fields("wordd"));
        builder.setBolt("Print", new PrintBolt(),1).shuffleGrouping("WordCount");
        
        config.setDebug(false);//调试模式,会把所有的log都打印出来
        
        //通过是否有参数来判断是否启动集群，或者本地模式执行
        if(args!=null&&args.length>0){
            System.out.print("集群模式------------------------------------------------");
            try {
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                // TODO: handle exception
            }
        }else{
            System.out.print("本地模式------------------------------------------------");
            //本地模式
            config.setMaxTaskParallelism(1);
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("wordcount",config,builder.createTopology() );
            
            Thread.sleep(50000L);
            //关闭本地集群
            cluster.shutdown();
        }
    }
}