import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class DatabaseConnection {
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String url = "jdbc:oracle:thin:@db18.cse.cuhk.edu.hk:1521/oradb.cse.cuhk.edu.hk";
            Connection conn = DriverManager.getConnection(
                    url,
                    "h102",
                    "NospIddO");
            System.out.println("Connected to the database!");

            Statement stmt = conn.createStatement();
        // stmt.executeUpdate("CREATE TABLE b " +
        //         "(UserID VARCHAR(10), " +
        //         "Password VARCHAR(8))");

        ResultSet rs = stmt.executeQuery("select * from user_tables");
        while (rs.next()){
            System.out.println(rs.getString(1));
            }
        


            start();
            scanner.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e);
        }
    }

    private static String scanInput() {
        System.out.print("Enter Your Choice: ");
        String input = scanner.nextLine();
        return input;
    }

    private static void start() {
        while (true) {
            System.out.println("-----Main menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. Operations for administator\n" +
                    "2. Operations for salesperson\n" +
                    "3. Operations for manager\n" +
                    "4. Exit this program\n");

            switch (scanInput()) {
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
                    "4. Show content of a table\n");

            switch (scanInput()) {
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

    private static void salesperson() {
        return;
    }

    private static void manager() {
        return;
    }
}