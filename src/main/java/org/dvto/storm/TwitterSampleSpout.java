/**
 * Taken from the storm-starter project on GitHub
 * https://github.com/nathanmarz/storm-starter/ 
 */
package org.dvto.storm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.auth.AccessToken;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.base.Preconditions;

@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String _consumerKey;
	String _consumerSecret;
	String _accessToken;
	String _accessTokenSecret;

	public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
		Preconditions.checkArgument(!consumerKey.equals(""));
		Preconditions.checkArgument(!consumerSecret.equals(""));
		Preconditions.checkArgument(!accessToken.equals(""));
		Preconditions.checkArgument(!accessTokenSecret.equals(""));
		_consumerKey = consumerKey;
		_consumerSecret = consumerSecret;
		_accessToken = accessToken;
		_accessTokenSecret = accessTokenSecret;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override public void onDeletionNotice(StatusDeletionNotice sdn) {}
			@Override public void onTrackLimitationNotice(int i) {}
			@Override public void onScrubGeo(long l, long l1) {}
			@Override public void onException(Exception e) {}
			@Override public void onStallWarning(StallWarning sw) {}
		};
		
		TwitterStreamFactory fact = new TwitterStreamFactory();
		_twitterStream = fact.getInstance();
		_twitterStream.setOAuthConsumer(_consumerKey, _consumerSecret);
    		_twitterStream.setOAuthAccessToken(new AccessToken(_accessToken, _accessTokenSecret));
		_twitterStream.addListener(listener);
		_twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		
		if (ret == null) {
			Utils.sleep(50);
		}
		else {
			if(ret.isRetweet()) {
				Status retweet = ret.getRetweetedStatus();
				_collector.emit(new Values(retweet.getUser().getScreenName(), retweet.getText(), retweet.getRetweetCount()));
			}
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {}

	@Override
	public void fail(Object id) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("author", "text", "retweetCount"));
	}

}
