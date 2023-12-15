package org.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ConnectDB {
    private String url;
    private String user;
    private String password;
    private Connection connection;
    private String filePathLogs;
    private int idLog;

    private Connection connectionControl;


    public ConnectDB(String url, String user, String password, String filePathLogs) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.filePathLogs = filePathLogs;
        connectDB();
    }

    public ConnectDB(String url, String user, String password, String filePathLogs, int idLog, Connection connectionControl) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.filePathLogs = filePathLogs;
        this.idLog = idLog;
        this.connectionControl = connectionControl;
        connectDB();
    }

    public void connectDB() {
        System.out.println("URL " + url);
        try {
            // Kết nối đến cơ sở dữ liệu
            connection = DriverManager.getConnection(url, user, password);
            System.out.println("Kết nối thành công! " + url);
        } catch (SQLException e) {
//            String dbName = getDBName();
//            if(dbName.equals("control")){
//                writeLog(filePathLogs, "fail", "connect control failed");
//            }else{
//                String sqlUpdate = "UPDATE logs SET status='fail',note='connect "+dbName+" failed' WHERE id=?";
//                try {
//                    PreparedStatement pstmtControl = connectionControl.prepareStatement(sqlUpdate);
//                    pstmtControl.setInt(1, idLog);
//                    pstmtControl.executeUpdate();
//                } catch (SQLException ex) {
//                    throw new RuntimeException(ex);
//                }
//
//            }
        }
    }

    public String getDBName() {
        String databaseName = "";
        int index = url.lastIndexOf("/") + 1;
        if (index > 0 && index < url.length()) {
            databaseName = url.substring(index);
        }
        return databaseName;
    }

    public void writeLogs(){
        String dbName = getDBName();
        String sqlUpdate = "UPDATE logs SET status='fail',note='connect " + dbName + " failed' WHERE id=?";
        try {
            PreparedStatement pstmtControl = connectionControl.prepareStatement(sqlUpdate);
            pstmtControl.setInt(1, idLog);
            pstmtControl.executeUpdate();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void writeLogToFile(String filePath, String status, String errorMessage) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = currentDateTime.format(formatter);


        String logData = String.format("date: %s, module: Load to data mart, status=\"%s\", message_error: %s%n",
                formattedDateTime, status, errorMessage);

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(filePath, true));
            writer.write(logData);
            System.out.println("Log has been written successfully.");
        } catch (IOException e) {
            System.err.println("Error writing to the log file: " + e.getMessage());
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                System.err.println("Error closing the log file: " + e.getMessage());
            }
        }
    }


    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public static void main(String[] args) {
//        writeLog("D:\\Storage NLU data\\Data warehouse\\MiddleTest\\logs.txt", "fail", "connect control failed");
    }
}
