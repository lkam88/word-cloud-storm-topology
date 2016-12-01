package com.lucaskam.wordcloud.topology.bolts;

import com.google.api.services.gmail.model.Message;

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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;



public class TextMessagePopulatorBolt extends BaseRichBolt {
    public final String myEmailAddress;
    public final String myPhoneNumber;

    private OutputCollector outputCollector;
    private GmailServiceProvider gmailServiceProvider;
    private transient GmailService gmailService;

    public TextMessagePopulatorBolt(GmailServiceProvider gmailServiceProvider, String myEmailAddress, String myPhoneNumber) {
        this.gmailServiceProvider = gmailServiceProvider;
        this.myEmailAddress = myEmailAddress;
        this.myPhoneNumber = myPhoneNumber;
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
        TextMessage skeletonTextMessage = null;
        try {
            skeletonTextMessage = (TextMessage) input.getValueByField("text-message");

            Message gmailMessage = gmailService.getMessage(skeletonTextMessage.getId());

            TextMessage textMessage = transformToTextMessage(gmailMessage);
            outputCollector.emit(new Values(textMessage));
            Logger.debug("Populated text message: {}", textMessage);
            Thread.sleep(1000);
        } catch (Exception e) {
            Logger.error(e, "Unable to populate text message: {}", skeletonTextMessage);
        } finally {
            outputCollector.ack(input);
        }
    }

    private TextMessage transformToTextMessage(Message actualMessage) throws ParseException {
        String toAddress = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("To")).findFirst().get().getValue();
        String fromAddress = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("From")).findFirst().get().getValue();
        String date = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("Date")).findFirst().get().getValue();

        String toPhoneNumber = extractPhoneNumberFromEmailAddress(toAddress);
        String fromPhoneNumber = extractPhoneNumberFromEmailAddress(fromAddress);

        byte[] messageBody = null;
        try {
            messageBody = actualMessage.getPayload().getBody().decodeData();
        } catch (Exception e) {
            Logger.error(e, "Unable to decode message body {}", actualMessage);
        }

        return new TextMessage(actualMessage.getId(), toPhoneNumber, fromPhoneNumber, new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US).parse
            (date).getTime(), messageBody);
    }

    private String extractPhoneNumberFromEmailAddress(String address) {
        String phoneNumber = address.equals(myEmailAddress) ? myPhoneNumber : address.split("<")[1].replaceAll("[^0-9]", "");
        Logger.trace("original address: {} Extracted to: {}", address, phoneNumber);
        return phoneNumber;
    }
}
