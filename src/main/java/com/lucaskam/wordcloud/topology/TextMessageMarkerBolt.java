package com.lucaskam.wordcloud.topology;

import org.pmw.tinylog.Logger;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TextMessageMarkerBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private GmailServiceProvider gmailServiceProvider;
    private transient GmailService gmailService;

    public TextMessageMarkerBolt(GmailServiceProvider gmailServiceProvider) {
        this.gmailServiceProvider = gmailServiceProvider;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text-message"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        gmailService = gmailServiceProvider.provide();
    }

    @Override
    public void execute(Tuple input) {
        TextMessage textMessage;
        try {
            textMessage = (TextMessage) input.getValueByField("text-message");
            gmailService.markTextMessage(textMessage);
            outputCollector.emit(new Values(textMessage));
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.error(e, "Unable to mark text message as processed: {}", input);
        } finally {
            outputCollector.ack(input);
        }
    }
}
