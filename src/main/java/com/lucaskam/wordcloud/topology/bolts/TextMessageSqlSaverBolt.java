package com.lucaskam.wordcloud.topology.bolts;

import com.lucaskam.wordcloud.topology.daos.TextMessageDao;
import com.lucaskam.wordcloud.topology.daos.providers.TextMessageDaoProvider;
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
