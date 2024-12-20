import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
            try {
                System.out.println("Getting all columns from tables");
                callsql("select * from user_tables");
                System.out.println("Done getting all columns from tables");
            } catch (SQLException e) {
                System.out.println(e);
            }

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
        System.out.println("Processing...");
        try {
            java.io.InputStream inputStream = DatabaseConnection.class.getResourceAsStream("/Data/create_table");
            if (inputStream == null) {
                System.out.println("Could not find the file /Data/create_table.");
                return;
            }

            java.util.Scanner fileScanner = new java.util.Scanner(inputStream).useDelimiter(";");
            while (fileScanner.hasNext()) {
                String sql = fileScanner.next().trim();
                if (!sql.isEmpty()) {
                    callsql(sql);
                }
            }
            fileScanner.close();
            System.out.println("Done! Database is initialized");
        } catch (Exception e) {
            System.out.println("Error occurred while creating tables: " + e.getMessage());
        }
    }

    private static void deleteTables() {
        try {
            String[] tables = { "transaction", "salesperson", "part", "category", "manufacturer" };
            System.out.println("Processing...");
            for (String table : tables) {
                try {
                    callsql("DROP TABLE " + table + " CASCADE CONSTRAINTS");
                    System.out.println("Table " + table + " dropped successfully.");
                } catch (SQLException e) {
                    System.out.println("Could not drop table " + table + ": " + e.getMessage());
                }
            }
            System.out.print("Done! Database is removed!");
        } catch (Exception e) {
            System.out.println("Error occurred while dropping tables: " + e.getMessage());
        }
    }

    private static void loadDatafile() {
        String folderPath = scanInput("Type in the Source Data Folder Path: ").trim();
        System.out.println("Processing...");
        try {
            conn.setAutoCommit(false);
            try (BufferedReader br = new BufferedReader(new FileReader(folderPath + "/category.txt"))) {
                String sql = "INSERT INTO category (cID, cName) VALUES (?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.executeUpdate();
                }
                System.out.println("Loaded data from category.txt successfully.");
            }
    
            // Load manufacturer.txt
            try (BufferedReader br = new BufferedReader(new FileReader(folderPath + "/manufacturer.txt"))) {
                String sql = "INSERT INTO manufacturer (mID, mName, mAddress, mPhoneNumber) VALUES (?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.setString(3, parts[2].trim());
                    pstmt.setLong(4, Long.parseLong(parts[3].trim()));
                    pstmt.executeUpdate();
                }
                System.out.println("Loaded data from manufacturer.txt successfully.");
            }
    
            // Load part.txt
            try (BufferedReader br = new BufferedReader(new FileReader(folderPath + "/part.txt"))) {
                String sql = "INSERT INTO part (pID, pName, pPrice, mID, cID, pWarrantyPeriod, pAvailableQuantity) "
                           + "VALUES (?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.setInt(3, Integer.parseInt(parts[2].trim()));
                    pstmt.setInt(4, Integer.parseInt(parts[3].trim()));
                    pstmt.setInt(5, Integer.parseInt(parts[4].trim()));
                    pstmt.setInt(6, Integer.parseInt(parts[5].trim()));
                    pstmt.setInt(7, Integer.parseInt(parts[6].trim()));
                    pstmt.executeUpdate();
                }
                System.out.println("Loaded data from part.txt successfully.");
            }
    
            // Load salesperson.txt
            try (BufferedReader br = new BufferedReader(new FileReader(folderPath + "/salesperson.txt"))) {
                String sql = "INSERT INTO salesperson (sID, sName, sAddress, sPhoneNumber, sExperience) "
                           + "VALUES (?, ?, ?, ?, ?)";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setString(2, parts[1].trim());
                    pstmt.setString(3, parts[2].trim());
                    pstmt.setLong(4, Long.parseLong(parts[3].trim()));
                    pstmt.setInt(5, Integer.parseInt(parts[4].trim()));
                    pstmt.executeUpdate();
                }
                System.out.println("Loaded data from salesperson.txt successfully.");
            }
    
            // Load transaction.txt
            try (BufferedReader br = new BufferedReader(new FileReader(folderPath + "/transaction.txt"))) {
                String sql = "INSERT INTO transaction (tID, pID, sID, tDate) VALUES (?, ?, ?, TO_DATE(?, 'DD/MM/YYYY'))";
                PreparedStatement pstmt = conn.prepareStatement(sql);
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    pstmt.setInt(1, Integer.parseInt(parts[0].trim()));
                    pstmt.setInt(2, Integer.parseInt(parts[1].trim()));
                    pstmt.setInt(3, Integer.parseInt(parts[2].trim()));
                    pstmt.setString(4, parts[3].trim()); // Date remains in DD/MM/YYYY format
                    pstmt.executeUpdate();
                }
                System.out.println("Loaded data from transaction.txt successfully.");
            }
    
            conn.commit(); // Commit transaction
            System.out.println("Data is inputted to the database!");
    
        } catch (IOException e) {
            System.out.println("File I/O Error: " + e.getMessage());
            rollbackTransaction();
        } catch (SQLException e) {
            System.out.println("SQL Error: " + e.getMessage());
            rollbackTransaction();
        } catch (NumberFormatException e) {
            System.out.println("Data Format Error: " + e.getMessage());
            rollbackTransaction();
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Error re-enabling auto-commit: " + e.getMessage());
            }
        }
    }
    
    private static void rollbackTransaction() {
        try {
            conn.rollback();
            System.out.println("Transaction rolled back due to error.");
        } catch (SQLException e) {
            System.out.println("Error during rollback: " + e.getMessage());
        }
    }
    private static void showContent() {
        String tableName = scanInput("Which table would you like to show: ").trim().toUpperCase();
    
        // Optional: Validate if the table name is one of the expected tables
        String[] validTables = { "CATEGORY", "MANUFACTURER", "PART", "SALESPERSON", "TRANSACTION" };
        boolean isValidTable = false;
        for (String validTable : validTables) {
            if (validTable.equalsIgnoreCase(tableName)) {
                isValidTable = true;
                break;
            }
        }
    
        if (!isValidTable) {
            System.out.println("Invalid table name. Please choose from the following tables:");
            for (String validTable : validTables) {
                System.out.println("- " + validTable);
            }
            return;
        }
    
        String sql = "SELECT * FROM " + tableName;
    
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
    
            // Get metadata about the table
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            System.out.println("Content of table "+tableName+":");

            // Print column headers
            for (int i = 1; i <= columnCount; i++) {
                System.out.print("| " + rsmd.getColumnName(i) + " ");
            }
            System.out.println("|"); // Close the header line

            // Print rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    if (value == null) {
                        value = "NULL"; // Handle null values
                    }
                    System.out.print("| " + value + " ");
                }
                System.out.println("|");
            }
    
        } catch (SQLException e) {
            System.out.println("Error retrieving data from table '" + tableName + "': " + e.getMessage());
        }
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