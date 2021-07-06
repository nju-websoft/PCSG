package PCSG.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {

    /** settings to your local database, i.e., url, name, user, password
     * We use MySQL Community Server --version 8.0.15 */

    public static String url = "jdbc:mysql://xxx.xxx.xxx.xxx:3306/your_database_name"
    			+ "useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&autoReconnect=true";
    public static String name = "com.mysql.cj.jdbc.Driver";
    public static String user = "username";
    public static String password = "password";

    public Connection conn = null;
  
    public DBUtil() {
        try {
            Class.forName(name);
            conn = DriverManager.getConnection(url, user, password);
//            System.out.println("~~~~Succeed in connecting the database~~~~");
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
