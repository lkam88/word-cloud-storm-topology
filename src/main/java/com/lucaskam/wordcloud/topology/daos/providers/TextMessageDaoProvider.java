package com.lucaskam.wordcloud.topology.daos.providers;

import com.lucaskam.wordcloud.topology.daos.TextMessageDao;

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
