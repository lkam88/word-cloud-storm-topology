package com.lucaskam.wordcloud.topology;

import java.io.Serializable;
import java.util.Arrays;

public class TextMessage implements Serializable {
    
    private String id;
    private String from;
    private String to;
    private Long timestamp;
    private byte[] messageBody;

    public TextMessage(String id, String from, String to, Long timestamp, byte[] messageBody) {
        this.id = id;
        this.from = from;
        this.to = to;
        this.timestamp = timestamp;
        this.messageBody = messageBody;
    }

    public TextMessage(String id) {
       this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(byte[] messageBody) {
        this.messageBody = messageBody;
    }

    public String getText() {
        return messageBody != null ? new String(messageBody) : null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TextMessage{");
        sb.append("id='").append(id).append('\'');
        sb.append(", from='").append(from).append('\'');
        sb.append(", to='").append(to).append('\'');
        sb.append(", timestamp=").append(timestamp);
        sb.append(", messageBody=\"").append(getText()).append("\"");
        sb.append('}');
        return sb.toString();
    }
}
