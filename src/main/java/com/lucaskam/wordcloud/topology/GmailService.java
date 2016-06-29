package com.lucaskam.wordcloud.topology;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.Gmail;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.ListMessagesResponse;
import com.google.api.services.gmail.model.Message;
import com.google.api.services.gmail.model.ModifyMessageRequest;

import org.pmw.tinylog.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.util.Arrays;

public class GmailService implements Serializable {
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final int MAX_RETRIES = 5;

    private final String applicationName;
    private final String gmailSearchQuery;
    private final String processedLabelId;

    private FileDataStoreFactory dataStoreFactory;
    private HttpTransport httpTransport;
    private String gmailCredentialsFilePath;

    public GmailService(String applicationName, String gmailSearchQuery, String processedLabelId, String gmailCredentialsFilePath) {
        this.applicationName = applicationName;
        this.gmailSearchQuery = gmailSearchQuery;
        this.processedLabelId = processedLabelId;
        this.gmailCredentialsFilePath = gmailCredentialsFilePath;
        try {
            httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            dataStoreFactory = new FileDataStoreFactory(new java.io.File(
                System.getProperty("user.home"), ".credentials/gmail-java-quickstart.json"));
        } catch (Exception e) {
            Logger.error(e, "unable to connect to GmailService.  Exiting...");
            System.exit(1);
        }
    }

    public Gmail getGmailService() throws IOException {
        Credential credential = authorize(gmailCredentialsFilePath);
        return new Gmail.Builder(httpTransport, JSON_FACTORY, credential)
            .setApplicationName(applicationName)
            .build();
    }

    public Credential authorize(String clientSecretFilePath) throws IOException {
        File file = new File(clientSecretFilePath);
        GoogleClientSecrets clientSecrets =
            GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(new FileInputStream(file)));


        GoogleAuthorizationCodeFlow flow =
            new GoogleAuthorizationCodeFlow.Builder(
                httpTransport, JSON_FACTORY, clientSecrets, GmailScopes.all())
                .setDataStoreFactory(dataStoreFactory)
                .setAccessType("online")
                .build();
        return new AuthorizationCodeInstalledApp(
            flow, new LocalServerReceiver()).authorize("user");
    }

    private String user = "me";

    // TODO: Make retry logic less horrible somehow.
    public ListMessagesResponse getMessages() throws IOException {
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                Gmail gmail = getGmailService();
                return gmail.users().messages().list(user).setQ(gmailSearchQuery).execute();
            } catch (SocketTimeoutException e) {
                Logger.error(e, "Gmail timed out. retries: {}", retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (GoogleJsonResponseException e) {
                int statusCode = e.getStatusCode();

                Logger.error(e, "Gmail returned a {}. retries: {}", statusCode, retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            } catch (Exception e)
            {
                Logger.error(e, "Something bad happened while getting a messages");
            }
        }

        throw new RuntimeException("Gmail timed out or something...");
    }

    public ListMessagesResponse getMessages(String pageToken) throws IOException {
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                Gmail gmail = getGmailService();
                return gmail.users().messages().list(user).setQ(gmailSearchQuery).setPageToken(pageToken).execute();

            } catch (SocketTimeoutException e) {
                Logger.debug("GMail timed out. retries: {}", retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (GoogleJsonResponseException e) {
                int statusCode = e.getStatusCode();

                Logger.error(e, "Gmail returned a {}. retries: {}", statusCode, retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (Exception e)
            {
                Logger.error(e, "Something bad happened while getting a messages");
            }
        }

        throw new RuntimeException("Gmail timed out or something...");
    }

    public Message getMessage(String gmailId) throws IOException {
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                Gmail gmail = getGmailService();
                return gmail.users().messages().get(user, gmailId).execute();
            } catch (SocketTimeoutException e) {
                Logger.debug("GMail timed out. retries: {}", retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (GoogleJsonResponseException e) {
                int statusCode = e.getStatusCode();

                Logger.error(e, "Gmail returned a {}. retries: {}", statusCode, retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (Exception e)
            {
                Logger.error(e, "Something bad happened while getting a Message");
            }
        }

        throw new RuntimeException("Gmail timed out or something...");
    }

    public void markTextMessage(TextMessage textMessage) throws IOException {
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            try {
                Gmail gmail = getGmailService();
                ModifyMessageRequest modifyMessageRequest = new ModifyMessageRequest().setAddLabelIds(Arrays.asList(processedLabelId));
                gmail.users().messages().modify(user, textMessage.getId(), modifyMessageRequest).execute();
                return;
            } catch (SocketTimeoutException e) {
                Logger.debug("GMail timed out. retries: {}", retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (GoogleJsonResponseException e) {
                int statusCode = e.getStatusCode();

                Logger.error(e, "Gmail returned a {}. retries: {}", statusCode, retries);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (Exception e)
            {
                Logger.error(e, "Something bad happened while marking a message");
            }
        }

        throw new RuntimeException("Gmail timed out or something...");
    }
}