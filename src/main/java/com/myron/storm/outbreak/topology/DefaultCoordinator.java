package com.myron.storm.outbreak.topology;

import java.io.Serializable;

import org.apache.storm.trident.spout.ITridentSpout;
import org.slf4j.Logger;

public class DefaultCoordinator implements ITridentSpout.BatchCoordinator<Long>,Serializable {

    private static final long serialVersionUID =1l;
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(DefaultCoordinator.class);


    //initializeTransaction方法返回一个用户定义的事务元数据。X是用户自定义的与事务相关的数据类型，返回的数据会存储到zk中。
    //其中txid为事务序列号，prevMetadata是前一个事务所对应的元数据。若当前事务为第一个事务，则其为空。currMetadata是当前事务的元数据，
    // 如果是当前事务的第一次尝试，则为空，否则为事务上一次尝试所产生的元数据。
    //TridentSpoutCoordinator接到MBC的 batch流后，会调用BatchCoordinator的initialTransaction()初始化一个消息，并继续向外发送
    //batch流。TridentSpoutExecutor接到 batch流后，会调用用户代码中的TridentSpoutExecutor#emitBatch()方法，开始发送实际的业务数据。
    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
        LOG.info("Initializing Transaction ["+txid+"]");
        return null;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful Transaction ["+txid+"]");
    }

    //isReady方法用于判断事务所对应的数据是否已经准备好，当为true时，表示可以开始一个新事务。其参数是当前的事务号。
    //BatchCoordinator中实现的方法会被部署到多个节点中运行，
    //其中isReady是在真正的Spout(MasterBatchCoordinator)中执行的，其余方法在TridentSpoutCoordinator中执行。
    @Override
    public boolean isReady(long txid) {
        return true;
    }

    @Override
    public void close() {

    }
}