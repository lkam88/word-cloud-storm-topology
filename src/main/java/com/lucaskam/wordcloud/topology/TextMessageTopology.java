package com.lucaskam.wordcloud.topology;

import com.lucaskam.wordcloud.topology.bolts.TextMessageMarkerBolt;
import com.lucaskam.wordcloud.topology.bolts.TextMessagePopulatorBolt;
import com.lucaskam.wordcloud.topology.bolts.TextMessageSqlSaverBolt;
import com.lucaskam.wordcloud.topology.daos.providers.TextMessageDaoProvider;
import com.lucaskam.wordcloud.topology.models.RuntimeConfigurations;
import com.lucaskam.wordcloud.topology.services.providers.GmailServiceProvider;
import com.lucaskam.wordcloud.topology.spouts.GmailSpout;

import org.pmw.tinylog.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TextMessageTopology {
    private final String applicationName;
    private final String gmailSearchQuery;
    private final String processedLabelId;
    private final String gmailCredentialsFilePath;
    private final String databaseUrl;
    private final int minutesInBetweenQueries;
    private final String myEmailAddress;
    private final String myPhoneNumber;


    public TextMessageTopology(RuntimeConfigurations runtimeConfigurations) {
        this.applicationName = runtimeConfigurations.getApplicationName();
        this.gmailSearchQuery = runtimeConfigurations.getGmailSearchQuery();
        this.processedLabelId = runtimeConfigurations.getProcessedLabelId();
        this.gmailCredentialsFilePath = runtimeConfigurations.getGmailCredentialsFilePath();
        this.databaseUrl = runtimeConfigurations.getDatabaseUrl();
        this.minutesInBetweenQueries = runtimeConfigurations.getMinutesInBetweenQueries();
        this.myEmailAddress = runtimeConfigurations.getMyEmailAddress();
        this.myPhoneNumber = runtimeConfigurations.getMyPhoneNumber();
    }

    public void buildTopology() {
        Logger.info("Building topology");

        GmailServiceProvider gmailServiceProvider = new GmailServiceProvider(applicationName, gmailSearchQuery, processedLabelId,
                                                                             gmailCredentialsFilePath);
        TextMessageDaoProvider textMessageDaoProvider = new TextMessageDaoProvider(databaseUrl);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("gmail-spout", new GmailSpout(gmailServiceProvider,
                                                       minutesInBetweenQueries));
        builder.setBolt("skeleton-text", new TextMessagePopulatorBolt(gmailServiceProvider, myEmailAddress, myPhoneNumber)).shuffleGrouping("gmail-spout");
        builder.setBolt("full-text", new TextMessageSqlSaverBolt(textMessageDaoProvider)).shuffleGrouping("skeleton-text");
        builder.setBolt("marked-text", new TextMessageMarkerBolt(gmailServiceProvider)).shuffleGrouping("full-text");

        Logger.info("Starting topology");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("gmail", new Config(), builder.createTopology());
    }
}
