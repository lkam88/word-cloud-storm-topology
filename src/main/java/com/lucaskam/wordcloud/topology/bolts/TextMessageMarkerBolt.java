package com.lucaskam.wordcloud.topology.bolts;

import com.lucaskam.wordcloud.topology.services.GmailService;
import com.lucaskam.wordcloud.topology.services.providers.GmailServiceProvider;
import com.lucaskam.wordcloud.topology.models.TextMessage;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.pmw.tinylog.Logger;

import java.util.Map;

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
            Logger.debug("Marked text message as processed: {}", textMessage);
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.error(e, "Unable to mark text message as processed: {}", input);
        } finally {
            outputCollector.ack(input);
        }
    }
}
