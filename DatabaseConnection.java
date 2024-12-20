import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DatabaseConnection {
    private static Scanner scanner = new Scanner(System.in);

    private static Connection conn;

    public static void main(String[] args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String url = "jdbc:oracle:thin:@db18.cse.cuhk.edu.hk:1521/oradb.cse.cuhk.edu.hk";
            Connection connection = DriverManager.getConnection(
                    url,
                    "h102",
                    "NospIddO");
            System.out.println("Connected to the database!");

            conn = connection;

            // stmt.executeUpdate("CREATE TABLE b " +
            // "(UserID VARCHAR(10), " +
            // "Password VARCHAR(8))");

            callsql("select * from user_tables");
            System.out.println("Welcome to sales system! \n\n");
            start();
            scanner.close();
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e);
        }
    }

    private static String scanInput(String s) {
        System.out.print(s);
        String input = scanner.nextLine();
        return input;
    }

    private static void callsql(String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        return;
    }

    private static ResultSet getsqlResult(String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return rs;
    }

    private static void start() throws SQLException {
        while (true) {
            System.out.println("-----Main menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. Operations for administator\n" +
                    "2. Operations for salesperson\n" +
                    "3. Operations for manager\n" +
                    "4. Exit this program\n");

            switch (scanInput("Enter Your Choice: ")) {
                case "1":
                    administrator();
                    break;
                case "2":
                    salesperson();
                    break;
                case "3":
                    manager();
                    break;
                case "4":
                    return;
                default:
                    System.out.println("Usage: 1/2/3/4");
                    continue;
            }
        }
    }

    private static void administrator() {
        while (true) {
            System.out.println("-----Operations for administrator menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. Create all tables\n" +
                    "2. Delete all tables\n" +
                    "3. Load from datafile\n" +
                    "4. Show content of a table\n" +
                    "5. Return to the main menu\n");

            switch (scanInput("Enter Your Choice: ")) {
                case "1":
                    createTables();
                    break;
                case "2":
                    deleteTables();
                    break;
                case "3":
                    loadDatafile();
                    break;
                case "4":
                    showContent();
                    break;
                case "5":
                    return;
                default:
                    System.out.println("Usage: 1/2/3/4/5");
                    continue;
            }
        }
    }

    private static void createTables() {

        return;
    }

    private static void deleteTables() {
        return;
    }

    private static void loadDatafile() {
        return;
    }

    private static void showContent() {
        return;
    }

    private static void salesperson() throws SQLException {
        while (true) {
            System.out.println("-----Operations for salesperson menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. Search for parts\n" +
                    "2. Sell a part\n" +
                    "3. Return to the main menu\n");
            switch (scanInput("Enter Your Choice: ")) {
                case "1":
                    searchPart();
                    break;
                case "2":
                    sellPart();
                    break;
                case "3":
                    return;
                default:
                    System.out.println("Usage: 1/2/3");
            }
        }
    }

    private static void searchPart() throws SQLException {
        while (true) {
            System.out.println("Choose the search criterion\n" +
                    "1. Part Name\n" +
                    "2. Manufacturer Name\n");
            switch (scanInput("Choose the search criterion: ")) {
                case "1":
                    // part name
                    String keyword = scanInput("Type in Search Keyword: ");
                    String ordering = scanInput("Choose ordering:\n" +
                            "1. By price, ascending order\n" +
                            "2. By price, desending order\n");

                    String sql = "SELECT * FROM part" +
                            "Where pName LIKE '%" + keyword + "%'" +
                            "ORDER BY pPrice " + (ordering.equals("1") ? "ASC" : "DESC");

                    callsql(sql);

                    return;
                case "2":
                    String keyword2 = scanInput("Type in Search Keyword: ");
                    String ordering2 = scanInput("Choose ordering:\n" +
                            "1. By price, ascending order\n" +
                            "2. By price, desending order\n");

                    String sql2 = "SELECT p.*, m.mName " +
                            "FROM part p " +
                            "JOIN manufacturer m ON p.mID = m.mID " +
                            "WHERE m.mName LIKE '%" + keyword2 + "%' " +
                            "ORDER BY p.pPrice " + (ordering2.equals("1") ? "ASC" : "DESC");

                    callsql(sql2);

                    return;
                default:
                    System.out.println("Usage: 1/2");
            }
        }
    }

    private static void sellPart() throws SQLException {
        String part = scanInput("Enter the Part ID : ");
        String salesperson = scanInput("Enter the Salesperson ID : ");

        String updatePartSql = "UPDATE part SET pAvailableQuantity = pAvailableQuantity - 1 " +
                "WHERE pID = " + part + " AND pAvailableQuantity > 0";

        String insertTransactionSql = "INSERT INTO transaction (pID, sID, tDate) " +
                "VALUES (" + part + ", " + salesperson + ", CURRENT_DATE)";

        String selectPartSql = "SELECT pName, pAvailableQuantity FROM part WHERE pID = " + part;

        callsql(updatePartSql);
        callsql(insertTransactionSql);

        ResultSet rs = getsqlResult(selectPartSql);
        if (rs.next()) {
            String productName = rs.getString("pName");
            int remainingQuantity = rs.getInt("pAvailableQuantity");
            System.out.println("Product: " + productName + ", Remaining Quantity: " + remainingQuantity);
        }

        return;
    }

    private static void manager() {
        while (true) {
            System.out.println("-----Operations for manager menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. List all salespersons\n" +
                    "2. Count the no. of sales records of each salesperson under a specific range on years of experience\n"
                    +
                    "3. Show the total sales valur of each manufacturer\n" +
                    "4.Show the N most popular part\n" +
                    "5. Return to the main menu\n");
            switch (scanInput("Enter Your Choice: ")) {
                case "1":
                    listSalesPersons();
                    break;
                case "2":
                    countSalesRecord();
                    break;
                case "3":
                    showManufacturer();
                    break;
                case "4":
                    showNMostpopular();
                    break;
                case "5":
                    return;
                default:
                    System.out.println("Usage: 1/2/3/4/5");
            }
        }
    }

    private static void listSalesPersons() {
        return;
    }

    private static void countSalesRecord() {
        return;
    }

    private static void showManufacturer() {
        return;
    }

    private static void showNMostpopular() {
        return;
    }
}