package com.lucaskam.wordcloud.topology;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;

import org.pmw.tinylog.Logger;

import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class GmailSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private GmailServiceProvider gmailServiceProvider;
    private int minutesInBetweenQueries;
    
    private transient GmailService gmailService;
    
    public GmailSpout(GmailServiceProvider gmailServiceProvider, int minutesInBetweenQueries) {
        this.gmailServiceProvider = gmailServiceProvider;
        this.minutesInBetweenQueries = minutesInBetweenQueries;
    }
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text-message"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        gmailService = gmailServiceProvider.provide();
    }

    @Override
    public void nextTuple() {
        try {
            ListMessagesResponse listMessagesResponse = gmailService.getMessages();

            while (listMessagesResponse.getNextPageToken() != null) {
                String pageToken = listMessagesResponse.getNextPageToken();

                for (Message message : listMessagesResponse.getMessages()) {

                    TextMessage textMessage = new TextMessage(message.getId());

                    spoutOutputCollector.emit(new Values(textMessage));
                }

                Thread.sleep(1000);
                listMessagesResponse = gmailService.getMessages(pageToken);
            }

        } catch (GoogleJsonResponseException ignored) {
            try {
                Logger.debug("We exceeded API rate limits, so we're gonna chill for 10 seconds");
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Logger.error(e, "Unable to get all emails from Gmail");
            }
        } catch (IOException | InterruptedException e) {
            Logger.error(e);
        }
        try {
            Thread.sleep(minutesInBetweenQueries * 1000 * 60);
            Logger.debug("Got as many emails as we could, now we're gonna sleep for {} minutes", minutesInBetweenQueries);
        } catch (InterruptedException ignored) {
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }
    
}
