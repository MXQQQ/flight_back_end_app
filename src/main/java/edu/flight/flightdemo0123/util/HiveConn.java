package edu.flight.flightdemo0123.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class HiveConn {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.2.2:10000/flights";
    private static String user = "hadoop";
    private static String pass = "123";

    // 打开连接
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, pass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    // 关闭连接
    public static void close(Statement statement, Connection connection) {
        try {
            if (statement != null) {
                statement.close();
                statement = null;
            }
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
