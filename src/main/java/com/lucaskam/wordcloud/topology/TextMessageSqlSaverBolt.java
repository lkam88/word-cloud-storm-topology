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

public class TextMessageSqlSaverBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private TextMessageDaoProvider textMessageDaoProvider;
    private transient TextMessageDao textMessageDao;

    public TextMessageSqlSaverBolt(TextMessageDaoProvider textMessageDaoProvider) {
        this.textMessageDaoProvider = textMessageDaoProvider;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text-message"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.textMessageDao = textMessageDaoProvider.provide();
    }

    @Override
    public void execute(Tuple input) {
        TextMessage textMessage = null;
        try {
            textMessage = (TextMessage) input.getValueByField("text-message");
            textMessageDao.save(textMessage);
            outputCollector.emit(new Values(textMessage));
            Logger.debug("Saved to database: {}", textMessage);
        } catch (Exception e) {
            Logger.error(e, "Unable to save text message to database: {}", textMessage);
        } finally {
            outputCollector.ack(input);
        }
    }
}
