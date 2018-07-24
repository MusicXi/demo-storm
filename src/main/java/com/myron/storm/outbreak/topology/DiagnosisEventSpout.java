package com.myron.storm.outbreak.topology;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

/**
 * diagnosis 诊断
 * Trident的是spout/bolt的高级抽象
 * Trident中,spout没有真正的发射tuple,而是把工作分解给BatchCoordinator和Emitter
 * 
 * @author Administrator
 *
 */
public class DiagnosisEventSpout implements ITridentSpout<Long>{

    private static final long serialVersionUID = 1L;
    SpoutOutputCollector collector;
    //MasterBatchCoordinator会调用用户定义的BatchCoordinator的isReady()方法,如果返回true的话
    //则会发送一个id为 batch的消息流，从而开始一个数据流转
    //BatchCoordinator负责管理批次和元数据
    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    //消息发送节点会接收协调spout的$batch和$success流。
    Emitter<Long> emitter = new DiagnosisEventEmitter();

    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
       return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }
}