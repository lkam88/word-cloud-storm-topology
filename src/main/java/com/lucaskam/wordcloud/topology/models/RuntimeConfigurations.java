package com.lucaskam.wordcloud.topology.models;

import java.util.Properties;

public class RuntimeConfigurations {
    private final String applicationName;
    private final String gmailSearchQuery;
    private final String processedLabelId;
    private final String gmailCredentialsFilePath;
    private final String databaseUrl;
    private final int minutesInBetweenQueries;
    private final String myEmailAddress;
    private final String myPhoneNumber;
    private final String logLevel;
    private final String logFilesLocation;

    public RuntimeConfigurations(Properties properties) {
        this.applicationName = properties.getProperty("applicationName");
        this.gmailSearchQuery = properties.getProperty("gmailSearchQuery");
        this.processedLabelId = properties.getProperty("processedLabelId");
        this.myEmailAddress = properties.getProperty("myEmailAddress");
        this.myPhoneNumber = properties.getProperty("myPhoneNumber");
        this.databaseUrl = properties.getProperty("databaseUrl");
        this.logLevel = properties.getProperty("logLevel");
        this.gmailCredentialsFilePath = properties.getProperty("gmailCredentialsFilePath");
        this.logFilesLocation = properties.getProperty("logFilesLocation");
        this.minutesInBetweenQueries = Integer.valueOf(properties.getProperty("minutesInBetweenQueries"));
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getGmailSearchQuery() {
        return gmailSearchQuery;
    }

    public String getProcessedLabelId() {
        return processedLabelId;
    }

    public String getGmailCredentialsFilePath() {
        return gmailCredentialsFilePath;
    }

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public int getMinutesInBetweenQueries() {
        return minutesInBetweenQueries;
    }

    public String getMyEmailAddress() {
        return myEmailAddress;
    }

    public String getMyPhoneNumber() {
        return myPhoneNumber;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public String getLogFilesLocation() {
        return logFilesLocation;
    }
}
