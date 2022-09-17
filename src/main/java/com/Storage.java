package com;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Storage {
    private static String url = "";
    private static final String user = "user";
    private static final String password = "password";


    public Logger logger;


    public Storage(String port, Logger logger) {
        url = "jdbc:postgresql://localhost:" + port + "/postgres";
        this.logger = logger;
    }


    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.info(e.getMessage());
        }
        return conn;
    }


//    public static void main(String[] args) {
//        Storage storage = new Storage("5430", Server.getLogger("test"));
////        storage.put("khiar", "green");
////        storage.put("apple", "yellow");
////        storage.put("yegear", "white");
//        HashMap<String, String > table = new HashMap<>();
//        table.put("coffee", "black");
//        table.put("tee", "brown");
//        storage.putAll(table);
//        System.out.println(storage.get("khiar"));
//        System.out.println(storage.get("apple"));
//        System.out.println(storage.getAll());
//
//    }

    public String get(String key) {
        String SQL = "SELECT value FROM data WHERE key = ?";
        String value = null;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next())
                value = rs.getString("value");

        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }

        return value;
    }

    public Boolean containsKey(String key) {
        String SQL = "SELECT value FROM data WHERE key = ?";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next())
                return true;

        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }

        return false;
    }

    public void put(String key, String value) {
        String insertSQL = "INSERT INTO data (key, value) " +
                "VALUES (?,?)" +
                "ON CONFLICT (key) DO UPDATE " +
                "    SET value = excluded.value; ";
        String updateSQL = "UPDATE data SET value = ? WHERE key = ?";
//        String SQL = containsKey(key) ? updateSQL : insertSQL;
        String SQL = insertSQL;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);
            pstmt.setString(2, value);

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }
    }


    public void remove(String key) {

        String SQL = "DELETE FROM data WHERE key = ?";;

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.setString(1, key);

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }

    }

    public HashMap<String, String> getAll() {
        String SQL = "SELECT * FROM data";
        HashMap<String, String> table = new HashMap<>();
        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                table.put(rs.getString("key"), rs.getString("value"));
            }

        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }
        return table;
    }


    public void putAll(Map<String, String> table) {
        String insertSQL = "INSERT INTO data (key, value) " +
                "VALUES (?,?)" +
                "ON CONFLICT (key) DO UPDATE " +
                "    SET value = excluded.value; ";
        String SQL = insertSQL;

        try{
            Connection conn = connect();

            PreparedStatement pstmt = conn.prepareStatement(SQL);
            for(Map.Entry<String, String> entry : table.entrySet()) {
                pstmt.setString(1, entry.getKey());
                pstmt.setString(2, entry.getValue());
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.close();
        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }
    }

    public void clear() {
        String SQL = "DELETE FROM data";

        try (Connection conn = connect();
             PreparedStatement pstmt = conn.prepareStatement(SQL)) {

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            logger.info(ex.getMessage());
        }

    }
}
