import com.google.api.services.gmail.model.Message;

import com.lucaskam.wordcloud.topology.GmailService;
import com.lucaskam.wordcloud.topology.TextMessage;
import com.lucaskam.wordcloud.topology.TextMessageDao;

import org.pmw.tinylog.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

public class GetMessageById {
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
        String logFilesLocation = properties.getProperty("logFilesLocation");
        int minutesInBetweenQueries = Integer.valueOf(properties.getProperty("minutesInBetweenQueries"));
        
        
        GmailService  gmailService= new GmailService(applicationName, gmailSearchQuery, processedLabelId, gmailCredentialsFilePath);

        Message message = gmailService.getMessage("1559e96ed77b060d");

        System.out.println(message);

        TextMessage textMessage = transformToTextMessage(message);
        System.out.println(textMessage);

        TextMessageDao textMessageDao = new TextMessageDao(databaseUrl);
//        textMessageDao.save(textMessage);
    }

    private static TextMessage transformToTextMessage(Message actualMessage) throws ParseException {
        String toAddress = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("To")).findFirst().get().getValue();
        String fromAddress = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("From")).findFirst().get().getValue();
        String date = actualMessage.getPayload().getHeaders().stream().filter(header -> header.getName().equals("Date")).findFirst().get().getValue();

        String toPhoneNumber = extractPhoneNumberFromEmailAddress(toAddress);
        String fromPhoneNumber = extractPhoneNumberFromEmailAddress(fromAddress);

        byte[] messageBody = null;
        try {
            messageBody = actualMessage.getPayload().getBody().decodeData();
        } catch (Exception e) {
            Logger.error(e, "Unable to decode message body {}", actualMessage);
        }

        return new TextMessage(actualMessage.getId(), toPhoneNumber, fromPhoneNumber, new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US).parse
            (date).getTime(), messageBody);
    }

    private static String extractPhoneNumberFromEmailAddress(String address) {
        String phoneNumber = address.equals("lkam88@gmail.com") ? "7145956204" : address.split("<")[1].replaceAll("[^0-9]", "");
        Logger.trace("original address: {} Extracted to: {}", address, phoneNumber);
        return phoneNumber;
    }
}
