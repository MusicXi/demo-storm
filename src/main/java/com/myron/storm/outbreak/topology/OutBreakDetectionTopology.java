package com.myron.storm.outbreak.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

/**
 * 疾病爆发探测拓扑
 * 
 * @author Administrator
 *
 */
public class OutBreakDetectionTopology {

	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		//提供到BatchCoordinator和Emitter访问方法
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		//newStream时使用的入参spout会裂变成两个bolt，一是TridentSpoutCoordinator,另一个是TridentSpoutExecutor。
		Stream inputStream = topology.newStream("event", spout);
		
		inputStream.each(new Fields("event"),new DiseaseFilter())
        //声明需要CityAssignment对数据流中的每个tuple执行操作
        //在每个tuple中，CityAssignment会在event字段上运算并且增加一个叫做city的新字段
        //这个字段会附在tuple中向后发射
        .each(new Fields("event"),new CityAssignment(),new Fields("city"))
        //在每一个tuple中，HourAssignment会进行运算，并且增加hour和cityDiseaseHour新字段
        .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
        //根据 cityDiseaseHour进行分组，cityDiseaseHour是 城市:疾病代码:小时
        .groupBy(new Fields("cityDiseaseHour"))
        //统计并且持久化
        //partitionPersist 是一个接收 Trident 聚合器作为参数并对 state 数据源进行更新的方法
        //Trident 需要你提供一个实现 MapState 接口的 state。被分组的域就是 state 中的 key，
        // 而聚合的结果就是 state 中的 value
        .persistentAggregate(new OutbreakTrendFactory(),
        					 new Count(),
        					 new Fields("count")).newValuesStream()
        .each(new Fields("cityDiseaseHour", "count"),
        		new OutbreakDetector(), new Fields("alert"))
        .each(new Fields("alert"), new DispatchAlert(),
        		new Fields());
		return topology.build();
	}
	
	public static void main(String[] args) throws InterruptedException {
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cdc", conf, buildTopology());
		Thread.sleep(200000);
		cluster.shutdown();
	}

}
