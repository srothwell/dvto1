package org.dvto.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class LogBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private static final Logger logger = LoggerFactory.getLogger(LogBolt.class);
	long retweetThreshold;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger.info("Prepared LogBolt");
		retweetThreshold = 50L;
	}

	@Override
	public void execute(Tuple input) {
		//Status tweet = (Status) input.getValueByField("tweet");
		String author = input.getStringByField("author");
		String text = input.getStringByField("text");
		Long retweetCount = input.getLongByField("retweetCount");
		
		if(retweetCount > retweetThreshold) {
			retweetThreshold = retweetCount;
			logger.info("@" + author + " got " + String.valueOf(retweetCount) + " retweets for : " + text);
		}
		
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
