package com.lucaskam.wordcloud.topology;

import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;
import org.pmw.tinylog.labelers.TimestampLabeler;
import org.pmw.tinylog.writers.RollingFileWriter;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class GmailTopology {
    public static void main(String[] args) throws Exception {
        InputStream inputStream = new FileInputStream(args[0]);

        Properties properties = new Properties();

        properties.load(inputStream);

        String applicationName = properties.getProperty("applicationName");
        String gmailSearchQuery = properties.getProperty("gmailSearchQuery");
        String processedLabelId = properties.getProperty("processedLabelId");
        String myEmailAddress = properties.getProperty("myEmailAddress");
        String myPhoneNumber = properties.getProperty("myPhoneNumber");
        String databaseUrl = properties.getProperty("databaseUrl");
        String logLevel = properties.getProperty("logLevel");
        String gmailCredentialsFilePath = properties.getProperty("gmailCredentialsFilePath");

        Configurator.currentConfig()
                    .writer(new RollingFileWriter("log.txt", 30, new TimestampLabeler()))
                    .level(Level.TRACE)
                    .activate();

        Logger.info("Successfully loaded properties.");

        Logger.info("Building topology");
        GmailServiceProvider gmailServiceProvider = new GmailServiceProvider(applicationName, gmailSearchQuery, processedLabelId,
                                                                             gmailCredentialsFilePath);
        TextMessageDaoProvider textMessageDaoProvider = new TextMessageDaoProvider(databaseUrl);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("gmail-spout", new GmailSpout(gmailServiceProvider,
                                                       5));
        builder.setBolt("skeleton-text", new TextMessagePopulatorBolt(gmailServiceProvider, myEmailAddress, myPhoneNumber)).shuffleGrouping("gmail-spout");
        builder.setBolt("full-text", new TextMessageSqlSaverBolt(textMessageDaoProvider)).shuffleGrouping("skeleton-text");
        builder.setBolt("marked-text", new TextMessageMarkerBolt(gmailServiceProvider)).shuffleGrouping("full-text");

        Logger.info("Starting topology");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("gmail", new Config(), builder.createTopology());
    }
}
