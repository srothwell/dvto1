package org.dvto.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {


	static final String TOPOLOGY_NAME = "dvto";
	static final String TWITTER_CONSUMER_TOKEN = "";
	static final String TWITTER_CONSUMER_SECRET = "";
	static final String TWITTER_ACCESS_TOKEN = "";
	static final String TWITTER_ACCESS_SECRET = "";
	
	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		
		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("twitterStream", new TwitterSampleSpout(TWITTER_CONSUMER_TOKEN, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET));
		b.setBolt("logBolt", new LogBolt()).shuffleGrouping("twitterStream");
		
		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
		
		Runtime.getRuntime().addShutdownHook(new Thread()	{
			@Override
			public void run()	{
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});
		

	}

}


