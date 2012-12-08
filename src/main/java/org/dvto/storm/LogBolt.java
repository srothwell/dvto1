package org.dvto.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class LogBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	private static final Logger logger = LoggerFactory.getLogger(LogBolt.class);
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger.info("Prepared LogBolt");
	}

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		logger.info("@" + tweet.getUser().getScreenName() + " -> " + tweet.getText());
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
