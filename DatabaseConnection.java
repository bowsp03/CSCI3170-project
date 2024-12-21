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

    // private static void callsql(String sql) throws SQLException {
    //     Statement stmt = conn.createStatement();
    //     ResultSet rs = stmt.executeQuery(sql);
    //     while (rs.next()) {
    //         System.out.println(rs.getString(1));
    //     }
    //     return;
    // }

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
                case "custom":
                    executeCustomSQL();
                    break;
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
                    "5. Return to the main menu");

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
                    getsqlResult(sql);
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
                    getsqlResult("DROP TABLE " + table + " CASCADE CONSTRAINTS");
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

            System.out.println("Content of table " + tableName + ":");

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
                    "3. Return to the main menu");
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

                ResultSet rs;
            switch (scanInput("Choose the search criterion: ")) {
                case "1":
                    // part name
                    String keyword = scanInput("Type in Search Keyword: ");
                    String ordering = scanInput("Choose ordering:\n" +
                            "1. By price, ascending order\n" +
                            "2. By price, desending order\n" + 
                            "Choose the search criterion: ");

                            String sql = "SELECT p.pID, p.pName, m.mName, c.cName, p.pAvailableQuantity, " +
                            "p.pWarrantyPeriod, p.pPrice " +
                            "FROM part p " +
                            "JOIN manufacturer m ON p.mID = m.mID " +
                            "JOIN category c ON p.cID = c.cID " +
                            "WHERE p.pName LIKE '%" + keyword + "%' " +
                            "ORDER BY p.pPrice " + (ordering.equals("1") ? "ASC" : "DESC");
               

                    //System.out.println(sql);

                    rs = getsqlResult(sql);

                    System.out.println("| ID | Name | Manufacturer | Category | Quantity | Warranty | Price |");
                    while (rs.next()) {
                        int id = rs.getInt("pID");
                        String name = rs.getString("pName");
                        String manufacturer = rs.getString("mName");
                        String category = rs.getString("cName");
                        int quantity = rs.getInt("pAvailableQuantity");
                        int warranty = rs.getInt("pWarrantyPeriod");
                        int price = rs.getInt("pPrice");

                        System.out.printf("| %d | %s | %s | %s | %d | %d | %d |\n",
                                id, name, manufacturer, category, quantity, warranty, price);
                    }

                    System.out.println("End of Query\n\n");
                    return;

                case "2":
                    String keyword2 = scanInput("Type in Search Keyword: ");
                    String ordering2 = scanInput("Choose ordering:\n" +
                            "1. By price, ascending order\n" +
                            "2. By price, desending order\n"+ 
                            "Choose the search criterion: ");

                    String sql2 = "SELECT p.pID, p.pName, m.mName, c.cName, p.pAvailableQuantity, " +
                            "p.pWarrantyPeriod, p.pPrice " +
                            "FROM part p " +
                            "JOIN manufacturer m ON p.mID = m.mID " +
                            "JOIN category c ON p.cID = c.cID " +
                            "WHERE m.mName LIKE '%" + keyword2 + "%' " +
                            "ORDER BY p.pPrice " + (ordering2.equals("1") ? "ASC" : "DESC");

                    rs = getsqlResult(sql2);

                    System.out.println("| ID | Name | Manufacturer | Category | Quantity | Warranty | Price |");

                    while (rs.next()) {
                        int id = rs.getInt("pID");
                        String name = rs.getString("pName");
                        String manufacturer = rs.getString("mName");
                        String category = rs.getString("cName");
                        int quantity = rs.getInt("pAvailableQuantity");
                        int warranty = rs.getInt("pWarrantyPeriod");
                        int price = rs.getInt("pPrice");

                        System.out.printf("| %d | %s | %s | %s | %d | %d | %d |\n",
                                id, name, manufacturer, category, quantity, warranty, price);
                    }

                    System.out.println("End of Query\n\n");

                    return;
                default:
                    System.out.println("Incorrct input, please try again.");
            }
        }
    }

    private static void sellPart() throws SQLException {
        String part = scanInput("Enter the Part ID: ");
        String salesperson = scanInput("Enter the Salesperson ID: ");
        int tid = generateTID();
    
        // Update part quantity
        String updatePartSql = "UPDATE part SET pAvailableQuantity = pAvailableQuantity - 1 " +
                "WHERE pID = " + part + " AND pAvailableQuantity > 0";
    
        // Insert transaction
        String insertTransactionSql = "INSERT INTO transaction (tID, pID, sID, tDate) " +
                "VALUES (" + tid + ", " + part + ", " + salesperson + ", CURRENT_DATE)";
    
        // Fetch part details after update
        String selectPartSql = "SELECT pName, pAvailableQuantity FROM part WHERE pID = " + part;
    
        try {
            // Execute update query
            getsqlResult(updatePartSql);
    
            // Execute insert query
            getsqlResult(insertTransactionSql);
    
            // Retrieve and display part details
            ResultSet rs = getsqlResult(selectPartSql);
            if (rs.next()) {
                String productName = rs.getString("pName");
                int remainingQuantity = rs.getInt("pAvailableQuantity");
                System.out.println("Product: " + productName + ", Remaining Quantity: " + remainingQuantity);
            } else {
                System.out.println("No product found with the specified Part ID.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return;
    }

    private static int generateTID() {
        int nextTid = 1; // Default for the first transaction
        String sql = "SELECT MAX(tID) AS maxTid FROM transaction";
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                nextTid = rs.getInt("maxTid") + 1; // Increment the largest `tID`
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return nextTid;
    }

    private static void manager() throws SQLException {
        while (true) {
            System.out.println("-----Operations for manager menu-----\n" +
                    "What kinds of operation would you like to perform?\n" +
                    "1. List all salespersons\n" +
                    "2. Count the no. of sales records of each salesperson under a specific range on years of experience\n" +
                    "3. Show the total sales valur of each manufacturer\n" +
                    "4. Show the N most popular part\n" +
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

    private static void listSalesPersons() throws SQLException {
        
        String ordering = scanInput("Choose ordering:\n" +
                "1. By ascending order\n" +
                "2. By desending order\n" + 
                "Choose the search criterion: ");

        String sql = "SELECT * FROM salesperson ORDER BY sExperience " + (ordering.equals("1") ? "ASC" : "DESC");
    
        ResultSet rs = getsqlResult(sql);

        System.out.println("| ID | Name | Mobile Phone | Year of Experience |");
        while (rs.next()) {
            int id = rs.getInt("sID");
            String name = rs.getString("sName");
            String phone = rs.getString("sPhoneNumber");
            int yoa = rs.getInt("sExperience");

            System.out.printf("| %d | %s | %s | %d |\n",
                    id, name, phone, yoa);
        }

        System.out.println("End of Query\n\n");
        return; 
    }

    private static void countSalesRecord() throws SQLException {
        String lb = scanInput("Type in the lower bound for years of experience: ");
        String ub = scanInput("Type in the upper bound for years of experience: ");
    
        String sql = "SELECT s.sID, s.sName, s.sExperience, COUNT(t.tID) AS numOfTransactions " +
                     "FROM salesperson s " +
                     "LEFT JOIN transaction t ON s.sID = t.sID " +
                     "WHERE s.sExperience BETWEEN " + lb + " AND " + ub + " " +
                     "GROUP BY s.sID, s.sName, s.sExperience " +
                     "ORDER BY s.sID DESC";
    
        ResultSet rs = getsqlResult(sql);
    
        System.out.println("| ID | Name | Year of Experience | Number of Transactions |");
    
        while (rs.next()) {
            int id = rs.getInt("sID");
            String name = rs.getString("sName");
            int exp = rs.getInt("sExperience");
            int numOfTransactions = rs.getInt("numOfTransactions");
    
            System.out.printf("| %d | %s | %d | %d |\n",
                    id, name, exp, numOfTransactions);
        }
    
        System.out.println("End of Query\n\n");
    }
    

    private static void showManufacturer() throws SQLException {
        String sql = "SELECT m.mID, m.mName, SUM(p.pPrice) AS totalsalesvalue " +
                     "FROM manufacturer m " +
                     "JOIN part p ON m.mID = p.mID " +
                     "JOIN transaction t ON p.pID = t.pID " +
                     "GROUP BY m.mID, m.mName " +
                     "ORDER BY totalsalesvalue DESC";
    
        ResultSet rs = getsqlResult(sql);
    
        System.out.println("| Manufacturer ID | Manufacturer Name | Total Sales Value |");
        while (rs.next()) {
            int id = rs.getInt("mID");
            String name = rs.getString("mName");
            int tsv = rs.getInt("totalsalesvalue");
    
            System.out.printf("| %d | %s | %d |\n",
                    id, name, tsv);
        }
    
        System.out.println("End of Query\n\n");
    }
    

    private static void showNMostpopular() throws SQLException {
        String nop = scanInput("Type in the number of parts: ");
    
        String sql = "SELECT p.pID, p.pName, COUNT(t.tID) AS tr " +
        "FROM part p " +
        "LEFT JOIN transaction t ON p.pID = t.pID " +
        "GROUP BY p.pID, p.pName " +
        "ORDER BY tr DESC " +
        "FETCH FIRST " + nop + " ROWS ONLY";
    
        ResultSet rs = getsqlResult(sql);
    
        System.out.println("| Part ID | Part Name | No. of Transaction |");
        while (rs.next()) {
            int id = rs.getInt("pID");
            String name = rs.getString("pName");
            int not = rs.getInt("tr");
    
            System.out.printf("| %d | %s | %d |\n", id, name, not);
        }
    
        System.out.println("End of Query\n\n");
    }
    

    private static void executeCustomSQL() {
        Scanner scanner = new Scanner(System.in);
    
        try {
            System.out.print("Enter your SQL query: ");
            String sql = scanner.nextLine();
    
            Statement stmt = conn.createStatement();
    
            if (sql.trim().toUpperCase().startsWith("SELECT")) {
                ResultSet rs = stmt.executeQuery(sql);
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
    
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnLabel(i) + "\t");
                }
                System.out.println();
    
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getString(i) + "\t");
                    }
                    System.out.println();
                }
    
                rs.close();
            } else {
                int rowsAffected = stmt.executeUpdate(sql);
                System.out.println("Query executed successfully. Rows affected: " + rowsAffected);
            }
    
            stmt.close();
        } catch (SQLException e) {
            System.err.println("SQL Error: " + e.getMessage());
        }
    }
    
}