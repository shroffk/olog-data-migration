package org.phoebus.old.olog;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class OldOlogConfig {

    Properties properties = new Properties();

    public OldOlogConfig() {
        String configPath = System.getProperty("old_olog.properties");
        if (configPath == null) {
            try (InputStream input = OldOlogConfig.class.getResourceAsStream("/old_olog.properties")) {
                if (input == null) {
                    System.out.println("Sorry, unable to find config.properties");
                    return;
                }
                properties.load(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try (InputStream input = OldOlogConfig.class.getResourceAsStream("/old_olog.properties")) {
                if (input == null) {
                    System.err.println("Could not find /old_olog.properties in classpath");
                    return;
                }
                properties.load(input);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load default classpath properties", e);
            }
        }
    }

    public String getPropertyValue(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }


}
