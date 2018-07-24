package com.myron.storm.trident.wordcount.function;

import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TestTridentTopology {

	public static void main(String[] args) {
		
		//This spout cycles through that set of sentences over and over to produce the sentence stream
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
	               new Values("the cow jumped over the moon"),
	               new Values("the man went to the store and bought some candy"),
	               new Values("four score and seven years ago"),
	               new Values("how many apples can you eat"));
		spout.setCycle(true);
		
		//Here's the code to do the streaming word count part of the computation:
		TridentTopology topology = new TridentTopology();     
		
		//Input sources can also be queue brokers like Kestrel or Kafka.
		TridentState wordCounts = 
		     topology.newStream("spout1", spout)
		       .each(new Fields("sentence"), new Split(), new Fields("word"))
		       .groupBy(new Fields("word"))
		       .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
		       .parallelismHint(6);

	}

}
