package edu.flight.flightdemo0123.hive;

import java.sql.*;

public class HiveJDBC {

    private final String driverName = "org.apache.hive.jdbc.HiveDriver";

    private final String url = "jdbc:hive2://192.168.2.2:10000/flights";

    private Connection conn;

    private PreparedStatement ps;

    private ResultSet rs;

    public Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url, "hadoop", "123");
        return conn;
    }

    public PreparedStatement prepare(Connection conn, String sql) throws SQLException {
        return conn.prepareStatement(sql);
    }

    public void getAll(String tableName) throws SQLException, ClassNotFoundException {
        this.conn = getConnection();
        String sql = "select * from " + tableName;
        System.out.println(sql);
        ps = prepare(conn, sql);
        rs = ps.executeQuery();
        int columns = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= columns; i++) {
                System.out.print(rs.getString(i));
                System.out.print("\t\t");
            }
            System.out.println();
        }
    }


}
