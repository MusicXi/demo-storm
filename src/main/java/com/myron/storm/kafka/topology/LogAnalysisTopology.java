package com.myron.storm.kafka.topology;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class LogAnalysisTopology {
	private static final String ZK_HOSTS = "";
	private static final String TOPIC = "";
	public static StormTopology buildTopology() {
		//Kafka Trident Spout
		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts(ZK_HOSTS);
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, TOPIC);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		
		Stream spoutStream = topology.newStream("kafka-stream", spout);
		Fields jsonFields = new Fields("level", "timestamp", "message", "logger");
		//Steam parseStream = spoutStream.each(jsonFields, filter);
		return topology.build();
	}
}
