package com.lucaskam.wordcloud.topology;

import java.io.Serializable;

public class TextMessageDaoProvider implements Serializable {
    private String databaseUrl;

    public TextMessageDaoProvider(String databaseUrl) {
        this.databaseUrl = databaseUrl;
    }
    
    public TextMessageDao provide() {
        return new TextMessageDao(databaseUrl);
    }
}
