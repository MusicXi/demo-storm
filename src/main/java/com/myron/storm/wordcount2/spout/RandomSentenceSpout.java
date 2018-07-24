package com.myron.storm.wordcount2.spout;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


/**
 * 内存中随机选取待定的英文语句，作为数据源发射出去
 * @author sxf
 *随机发送一条内置消息，该spout继承BaseRichSpout/IRichSpout
 *
 *Storm的两个主要抽象是Spout和Bolt，Storm的第三个更强大的抽象是StateSpout
 */
@SuppressWarnings("serial")
public class RandomSentenceSpout extends BaseRichSpout {
    //发射消息
    SpoutOutputCollector spoutOutputCollector;
    Random random;
    
    /**
     * 【1】IComponent接口中的open（）方法
     * 进行spout的一些初始化工作，包括参数传递。open（）方法在该组件的一个任务在集群的工作进程内被初始化时被调用。提供了Spout执行的所需的环境。
     * Map:是这个Spout的Storm配置，提供给拓扑与这台主机上的集群配置一起进行合并。
     * TopologyContext：可以用来获取关于这个任务在拓扑中的位置信息，包括该任务的id,该任务的组件id，输入和输出信息等。
     * SpoutOutputCollector：是收集器，用于从这个Spout发射元组，元组可以随时被发射，包括open()和colse()方法。收集器是线程安全的，应该作为这个Spout对象的实例变量进行保存
     * 
     * 
     * 【2】IComponent接口中的colse（）方法
     * 当一个Ispout即将关闭时被调用。不能保证colse（）方法一定会被调用。因为Supervisor可以对集群的工作进程使用Kill -9命令强制杀死进程命令
     * 本地模式，当拓扑被杀死事，一定调用colse()方法
     * 
     * 【3】IComponent接口中的activate（）方法
     * Activate（）方法当Spout已经从失效模式中激活时被调用。该Spout的nextTuple()方法很快就会被调用。当使用Storm客户端操作拓扑时，Spout可以在失效状态之后变成激活模式。
     * 
     * 【4】
     */
    @SuppressWarnings("rawtypes")
	@Override
    public void open(Map arg0, TopologyContext arg01, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random=new Random();
    }
    
    /**
     * 进行Tuple处理的主要方法
     * 【4】IComponent的nextTuple()方法
     * 当调用nextTuple()方法时，Storm要求Spout发射元组到输出收集器（OutputCollector）
     * nextTuple()方法应该是非阻塞的，所以，如果Spout没有元组可以发射，该方法应该返回。
     * nextTuple(),ack()和fail()方法都在Spout任务的单一线程内紧密循环被调用。当没有元组可以发射时，
     * 可以让nextTuple()去sleep很短时间，例如1毫秒，这样不会浪费太多cpu资源。
     */
    @Override
    public void nextTuple() {
        //每两秒种发送一条消息,方便查看中间结果
        Utils.sleep(2000);
        //自定义内置数组
        String[] sentences=new String[]{
                "or 420 million US dollars",
                "What happened is that a group",
                "fight lasted hours overnight between",
                "the air according to the residents",
                "told me that one Malian soldier",
                "military spokesman says security forces",
                "that thousands of people who prayed",
                "continuing to receive treatment for",
                "freezing temperatures currently gripping",
                "Central African Republic Michel Djotodia",
                "freezing temperatures currently gripping",
                "former opposition will make up most",
                "The Syrian government has accused",
                "Doctors in South Africa reporting",
                "military spokesman says security forces",
                "Late on Monday, Ms Yingluck invoked special powers allowing officials to impose curfews",
                "Those who took up exercise were three times more likely to remain healthy over the next eight",
                "The space dream, a source of national pride and inspiration",
                "There was no time to launch the lifeboats because the ferry capsized with such alarming speed"
                };
        
        //从sentences数组中，随机获取一条语句，作为这次spout发送的消息
        String sentence=sentences[random.nextInt(sentences.length)];
        //使用emit方法进行Tuple发布会，参数用Values申明
        spoutOutputCollector.emit(new Values(sentence.trim().toLowerCase()));
    }

    
    
    /**
     * 【5】IComponent的ack()方法
     * Storm已经断定该Spout发射的标识符为msgId的元组已经被完全处理时，会调用ack方法。
     * 通常情况下，ack()方法会将该消息移除队列以防止它被重发
     */
    @Override
    public void ack(Object msgId) {
        
    }

    
    /**
     * 【6】IComponent接口的fail()方法
     * 该Spout发射的标识为msgId的元组未能被完全处理时，会调用fail()方法。
     * 通常情况下，fail方法会将消息放回队列中，并在稍后重发消息
     */
    @Override
    public void fail(Object msgId) {
        
    }

    //字段声明
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("wordd"));
    }

    
}