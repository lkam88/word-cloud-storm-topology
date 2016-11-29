package com.lucaskam.wordcloud.topology;

import com.lucaskam.wordcloud.topology.models.RuntimeConfigurations;

import org.pmw.tinylog.Configurator;
import org.pmw.tinylog.Level;
import org.pmw.tinylog.Logger;
import org.pmw.tinylog.labelers.TimestampLabeler;
import org.pmw.tinylog.writers.RollingFileWriter;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws Exception {
        InputStream configurationFilePath = new FileInputStream(args[0]);

        Properties properties = new Properties();
        properties.load(configurationFilePath);
        RuntimeConfigurations runtimeConfigurations = new RuntimeConfigurations(properties);

        Configurator.currentConfig()
                    .writer(new RollingFileWriter(runtimeConfigurations.getLogFilesLocation(), 30, new TimestampLabeler()))
                    .level(Level.valueOf(runtimeConfigurations.getLogLevel()))
                    .activate();

        Logger.info("Successfully loaded properties.");

        TextMessageTopology textMessageTopology = new TextMessageTopology(runtimeConfigurations);
        
        textMessageTopology.buildTopology();
    }

}
