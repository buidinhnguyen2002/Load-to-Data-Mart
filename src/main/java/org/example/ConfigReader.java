package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {
    private Properties properties;
    public enum ConfigurationProperty {
        STAGING_CONTROL_URL("StagingControl.url"),
        STAGING_CONTROL_USERNAME("StagingControl.username"),
        STAGING_CONTROL_PASSWORD("StagingControl.password"),
        DATA_WAREHOUSE_URL("DataWarehouse.url"),
        DATA_WAREHOUSE_USERNAME("DataWarehouse.username"),
        DATA_WAREHOUSE_PASSWORD("DataWarehouse.password"),
        DATA_MART_URL("DataMart.url"),
        DATA_MART_USERNAME("DataMart.username"),
        DATA_MART_PASSWORD("DataMart.password"),
        MODULE_LOAD_TO_DATA_MART("Module.LoadToDataMart"),
        MODULE_COLUMNS_NEW_ARTICLES("Module.Columns.NewArticles"),
        MODULE_FILE_LOGS_ERROR("Module.FileLogsError"),
        MODULE_PREVIOUS_MODULE("Module.PreviousModule"),
        MODULE_CONFIG_ID("Module.ConfigId");

        private final String propertyName;

        ConfigurationProperty(String propertyName) {
            this.propertyName = propertyName;
        }

        public String getPropertyName() {
            return propertyName;
        }
    }

    public ConfigReader() {
        properties = new Properties();
        try {
            properties.load(new FileInputStream("./Config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }

    public static void main(String[] args) {
        String propertyName = ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName();
        System.out.println("Property name: " + propertyName);
    }
}
