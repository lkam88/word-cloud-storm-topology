package com.lucaskam.wordcloud.topology;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Arrays;

public class GmailService implements Serializable {
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    
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
            Logger.error(e, "unable GmailService.  Exiting...");
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
        // Load client secrets.
//        InputStream in =
//            GmailService.class.getResourceAsStream(clientSecretFilePath);
        File file = new File(clientSecretFilePath);
        GoogleClientSecrets clientSecrets =
            GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(new FileInputStream(file)));

        // Build flow and trigger user authorization request.
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

    public ListMessagesResponse getMessages() throws IOException {
        Gmail gmail = getGmailService();
        return gmail.users().messages().list(user).setQ(gmailSearchQuery).execute();
    }

    public ListMessagesResponse getMessages(String pageToken) throws IOException {
        Gmail gmail = getGmailService();
        return gmail.users().messages().list(user).setQ(gmailSearchQuery).setPageToken(pageToken).execute();
    }

    public Message getTextMessage(String gmailId) throws IOException, InterruptedException, ParseException {
        Gmail gmail = getGmailService();
        return gmail.users().messages().get(user, gmailId).execute();
    }

    public void markTextMessage(TextMessage textMessage) throws IOException, InterruptedException {
        Gmail gmail = getGmailService();
        ModifyMessageRequest modifyMessageRequest = new ModifyMessageRequest().setAddLabelIds(Arrays.asList(processedLabelId));
        gmail.users().messages().modify(user, textMessage.getId(), modifyMessageRequest).execute();
    }

}