package com.myron.storm.wordcount2.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * 消息预处理的bolt,将消息按单词切分
 * @author sxf
 *Bolt的生命周期如下：
 *在客户端主机上创建IBolt对象，IBolt被序列化到拓扑并提交到集群的主控节点（Nimbus）然后Nimbus启动工作进程（Worker）反序列化对象，
 *调用对象上的prepare（）方法，然后开始处理元组。
 *如果你希望参数化一个IBolt，应该通过其构造函数设置参数并作为实例变量保存参数化状态。然后，实例变量会序列化，
 *并发送给跨集群的每个任务来执行这个Bolt
 *如果使用java来定义Bolt,应该使用IRichBolt接口，IRichBolt接口添加了使用java TopologyBuilder API的必要方法
 *
 * 
 * IBasicBolt与IRichBolt具有一样的同名方法，唯一不同，IBasicBolt的execute（）方法会自动处理Acking机制，
 * 如果在execute中想让元组失败，可以显示抛出一个FailedException异常
 */
@SuppressWarnings("serial")
public class WordNormalizerBolt implements IRichBolt {

    //发射消息
    private OutputCollector outputCollector;
    
    /**
     * bolt的初始化方法
     *(配置的参数，上下文，发送器)
     *【1】IBolt接口的prepare()方法
     *在该组建的一个任务在集群的工作进程内被初始化时被调用，提供了Bolt执行的所需环境
     *Map参数：是这个Bolt的Storm配置，提供给拓扑与这台主机上的集群配置一起进行合并。
     *TopologyContext参数：可以用来获取关于这个任务在拓扑中的位置信息，比如任务的id,该任务的组件id，输入输出信息等。
     *OutputCollector参数：是收集器皿，用于从这个Bolt发射元组。元组随时被发射，包括prepare()和cleanup()方法。
     *收集器是线程安全，应该作为这个Bolt对象的实例变量进行保存。
     */
    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        this.outputCollector=arg2;
    }
    
    
    /**
     * 执行订阅的Tuple逻辑过程的方法
     * 【2】IBolt接口的execute()方法
     * 用于处理一个输入元组，元组对象包含元组来自哪个组件/流/任务的元数据。
     * 元组的值可以使用tuple.getValue（）进行访问。Ibolt没有立即处理元组，而是完全地捕获一个元组在以后进行处理。
     * 元组应该使用prepare方法提供的OutputCollector进行发射。使用OutputCollector在某种程度上要求所有输入元组是ack或者fail
     * 否则Storm将无法确定来自Spout的元组什么时候处理完成。
     * 常见做法是：在execute()方法结束时对输入的元组调用ack方法，而IBasicBolt会自动处理该部分。
     * Tuple参数：为被处理的输入元组
     * 
     */
    @Override
    public void execute(Tuple tuple) {
        //获取订阅的tuple的内容
        String sentence=tuple.getString(0);
        //获取元组来自那个bolt或spout。返回它们的名字
        String ad=tuple.getSourceComponent();
        System.out.print(ad);//RandomSentence
        //进行单词分割
        String[] words=sentence.split(" ");
        //将单词发送出去
        for(String word:words){
            outputCollector.emit(new Values(word));
        }
    }
    
    /**
     * 此方法，在当前Bolt被关闭时，调用此方法来清理任何已经打开的资源，但不能保证这个方法会被集群调用
     * 【2】IBolt接口的cleanup（）方法
     * 当一个Bolt即将关闭时被调用。不能保证cleanup()方法一定会被调用，
     * 因为Supervisor可以对集群的工作进程使用Kill -9命令强制杀死进程命令
     * 如果在本地模式下运行storm，当拓扑被杀死时一定会调用该方法
     *
     */
    @Override
    public void cleanup() {
        
    }

    
    /**
     * 此方法，用于声明当前Bolt类发射一个字段名为"wordd"的一个元组
     * 为该拓扑的所有流声明输出模式
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("wordd"));
    }

    /**
     * 此方法孕育你配置关于当前这个组件如何运行的很多参数，会被运行中调用
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}