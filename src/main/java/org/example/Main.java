package org.example;

import java.sql.*;

public class Main {
    private ConfigReader configReader;
    private ConnectDB connectDBControl;
    String urlControl;
    String userControl;
    String passControl;

    String urlDW;
    String userDW;
    String passDW;

    String urlDM;
    String userDM;
    String passDM;
    String moduleName;
    String configId;
    String columns;
    String filePathLogs;
    String previousModule;
    int idLog;
    public Main(ConfigReader configReader) {
        this.configReader = configReader;
        loadConfig();
    }
    // load config
    public void loadConfig() {
        // load config dbControl
        urlControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_URL.getPropertyName());
        userControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_USERNAME.getPropertyName());
        passControl = configReader.getProperty(ConfigReader.ConfigurationProperty.STAGING_CONTROL_PASSWORD.getPropertyName());
        moduleName = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_LOAD_TO_DATA_MART.getPropertyName());
        configId = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_CONFIG_ID.getPropertyName());
        columns = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_COLUMNS_NEW_ARTICLES.getPropertyName());
        filePathLogs = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_FILE_LOGS_ERROR.getPropertyName());
        previousModule = configReader.getProperty(ConfigReader.ConfigurationProperty.MODULE_PREVIOUS_MODULE.getPropertyName());

        // load config db DW
        urlDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_URL.getPropertyName());
        userDW = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_USERNAME.getPropertyName());
        passDW= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_WAREHOUSE_PASSWORD.getPropertyName());

        // load config db DM
        urlDM = configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_URL.getPropertyName());
        userDM= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_USERNAME.getPropertyName());
        passDM= configReader.getProperty(ConfigReader.ConfigurationProperty.DATA_MART_PASSWORD.getPropertyName());
    }
    public boolean checkPreviousProgress(){
        boolean result = false;
        // connect database control
        connectDBControl = new ConnectDB(urlControl,userControl, passControl, filePathLogs);
        // ghi log vào file nếu kết nối thất bại
        if(connectDBControl.getConnection() == null){
            connectDBControl.writeLogToFile(filePathLogs, "fail", "connect control failed");
            return false;
        }
        String queryPreviousProcess = "SELECT * FROM logs where event='" + previousModule + "' AND DATE(create_at) = CURDATE() AND status='successful'";
        Connection connectionControl = connectDBControl.getConnection();
                try {
            Statement stmtControl = connectionControl.createStatement();
            ResultSet rs = stmtControl.executeQuery(queryPreviousProcess);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            if(rs.next()){
                result = true;
                String test = "";
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String data = rs.getString(columnName);
                    test += columnName + " " + data + "\t";
                }
                System.out.println(test);
            }else{
                result = false;
            }
            //
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public void executeApp() {
        if (!checkPreviousProgress()) {
            return;
        }
        // insert logs
        insertLogsProcess();
        // connect DW
        ConnectDB connectDW = new ConnectDB(urlDW, userDW, passDW, filePathLogs, idLog, connectDBControl.getConnection());
        try {
            // Kết nối đến Data Warehouse
            Connection connectionDW = connectDW.getConnection();
            if(connectionDW == null) {
                connectDW.writeLogs();
                return;
            }
            // Kết nối đến Data Mart
            // Truy vấn SQL để lấy dữ liệu từ bảng trong Data Warehouse
            String sqlSelect = "SELECT * FROM news_articles";
            Statement stmtDW = connectionDW.createStatement();
            ResultSet rs = stmtDW.executeQuery(sqlSelect);

            // connect data mart
            ConnectDB connectDM = new ConnectDB(urlDM, userDM, passDM, filePathLogs, idLog, connectDBControl.getConnection());
            Connection connectionDM = connectDM.getConnection();
            if(connectionDM == null) return;
            // PreparedStatement để chèn dữ liệu vào Data Mart
            String sqlInsert = createQueryInsertToDataMart("news_articles_temp");
            PreparedStatement pstmtDM = connectionDM.prepareStatement(sqlInsert);
            String[] columnsArr = columns.split(",");
            // Duyệt qua kết quả từ Data Warehouse và chèn vào Data Mart
            while (rs.next()) {
                // Lấy dữ liệu từ kết quả truy vấn DW và chèn vào DM
                for(int i=0; i< columnsArr.length; i++){
                    String column = columnsArr[i];
                    pstmtDM.setString(i+1, rs.getString(column));
                }
                // Thực hiện chèn dữ liệu vào Data Mart
                pstmtDM.executeUpdate();
            }
            // Rename table news_articles_temp to present_news_articles
            renameTable(connectionDM);
            // update logs process to successful
            updateStatusProcess("successful");
            // Đóng các kết nối
            rs.close();
            stmtDW.close();
            pstmtDM.close();
            connectionDW.close();
            connectionDM.close();
            System.out.println("Data transfer from DW to DM completed.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void updateStatusProcess(String status) {
        String sqlUpdate = "UPDATE logs SET status=? WHERE id=?";
        Connection connectionControl = connectDBControl.getConnection();
        try {
            PreparedStatement pstmtControl = connectionControl.prepareStatement(sqlUpdate);
            pstmtControl.setString(1, status);
            pstmtControl.setInt(2, idLog);
            pstmtControl.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void insertLogsProcess() {
        Connection connection = connectDBControl.getConnection();
        String sqlInsert = "INSERT INTO logs(event, status) VALUES (?, ?)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sqlInsert,Statement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, moduleName);
            preparedStatement.setString(2, "in process");
            preparedStatement.executeUpdate();
            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                idLog = generatedKeys.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void renameTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        // Đổi tên bảng present_news_articles => articles_temp
            String queryRenameToTemp = "RENAME TABLE present_news_articles TO articles_temp";
        statement.executeUpdate(queryRenameToTemp);
        // Đổi tên bảng new_articles_temp => present_news_articles
        String queryRenameToPresent = "RENAME TABLE news_articles_temp TO present_news_articles";
        statement.executeUpdate(queryRenameToPresent);
        // Đổi tên bảng articles_temp => news_articles_temp
        String queryRenameToArticleTemp = "RENAME TABLE articles_temp TO news_articles_temp";
        statement.executeUpdate(queryRenameToArticleTemp);
        // Xóa dữ liệu bảng new_articles_temp
        String truncateQuery = "TRUNCATE TABLE news_articles_temp";
        statement.executeUpdate(truncateQuery);
        System.out.println("Đã đổi tên bảng thành công!");
    }

    private String createQueryInsertToDataMart(String tableName) {
        String valuesField = "";
        String values = "";
        String[] columnsArr = columns.split(",");
        for(int i=0; i< columnsArr.length - 1; i++){
            valuesField += "`"+columnsArr[i]+"`,";
            values += "?,";
        }
        valuesField += "`"+columnsArr[columnsArr.length-1]+"`";
        values += "?";
        String result = "INSERT INTO "+ tableName +"("+valuesField+") VALUES ("+values+")" ;
        return result;
    }

    public static void main(String[] args) {
        String configPath = "";
        if (args.length > 0) {
            configPath = args[0];
        }
        System.out.println();
        System.out.println(configPath);
        ConfigReader configReader = new ConfigReader();
        Main main = new Main(configReader);
        main.executeApp();
    }
}