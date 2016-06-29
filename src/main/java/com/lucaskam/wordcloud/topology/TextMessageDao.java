package com.lucaskam.wordcloud.topology;

import org.pmw.tinylog.Logger;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TextMessageDao implements Serializable {
    public static final String INSERT_QUERY = "INSERT IGNORE INTO `text_message`(`id`,`from`,`to`,`timestamp`,`message_body`)" +
                                              "VALUES(?,?,?,?,?)";
    public static final String SELECT_BY_FROM_QUERY = "SELECT * FROM text_message WHERE `from` = ?";
    public static final String SELECT_BY_TO_QUERY = "SELECT * FROM text_message WHERE `to` = ?";
    private Connection databaseConnection;

    public TextMessageDao(String databaseUrl) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            databaseConnection =
                DriverManager.getConnection(databaseUrl);
        } catch (Exception e) {
            Logger.error(e, "Unable to connect to database.");
        }
    }

    public void save(TextMessage textMessage) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement(INSERT_QUERY);

        //"EEE, d MMM yyyy HH:mm:ss Z (z)"
        statement.setString(1, textMessage.getId());
        statement.setString(2, textMessage.getFrom());
        statement.setString(3, textMessage.getTo());
        statement.setTimestamp(4, new Timestamp(textMessage.getTimestamp()));
        statement.setBlob(5, textMessage.getMessageBody() != null ? new ByteArrayInputStream(textMessage.getMessageBody()) : null);

        statement.execute();

    }

    public List<TextMessage> getTextMessageByFrom(String from) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement(SELECT_BY_FROM_QUERY);
        statement.setString(1, from);
        ResultSet resultSet = statement.executeQuery();

        List<TextMessage> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(bindTextMessage(resultSet));
        }

        return results;
    }

    public List<TextMessage> getTextMessageByTo(String to) throws SQLException {
        PreparedStatement statement = databaseConnection.prepareStatement(SELECT_BY_TO_QUERY);
        statement.setString(1, to);
        ResultSet resultSet = statement.executeQuery();

        List<TextMessage> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(bindTextMessage(resultSet));
        }

        return results;
    }

    private TextMessage bindTextMessage(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String from = resultSet.getString("from");
        String to = resultSet.getString("to");
        Long timestamp = resultSet.getLong("timestamp");
        byte[] messageBody = resultSet.getBytes("message_body");

        return new TextMessage(id, from, to, timestamp, messageBody);
    }
}
