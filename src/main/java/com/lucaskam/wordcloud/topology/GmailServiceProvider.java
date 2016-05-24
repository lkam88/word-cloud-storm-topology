package com.lucaskam.wordcloud.topology;

import java.io.Serializable;

public class GmailServiceProvider  implements Serializable{
    private String applicationName;
    private String gmailSearchQuery;
    private String processedLabelId;
    private String gmailCredentialsFilePath;

    public GmailServiceProvider(String applicationName, String gmailSearchQuery, String processedLabelId, String gmailCredentialsFilePath) {
        this.applicationName = applicationName;
        this.gmailSearchQuery = gmailSearchQuery;
        this.processedLabelId = processedLabelId;
        this.gmailCredentialsFilePath = gmailCredentialsFilePath;
    }

    public GmailService provide()
    {
        return new GmailService(applicationName, gmailSearchQuery, processedLabelId, gmailCredentialsFilePath);
    }
}
