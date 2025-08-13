
import java.sql.DriverManager;
import java.sql.SQLException;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

// import com.unityjdbc.mongodb.MongoDBTranslator; // Adjust import as needed
import mongodb.jdbc.MongoConnection;
import mongodb.jdbc.MongoStatement;
import unity.annotation.GlobalSchema;
import unity.operators.Operator;
import unity.query.GlobalQuery;

public class TextNoSQLTranslate {

     // Use demo tpch Mongo instance. Queries that can be directly done using MongoDB can refer to collections not in this instance.
    // Queries that will involve UnityJDBC need a schema and a valid instance.
    // private static String url = "jdbc:mongo://localhost/tpch?debug=false&authDB=admin";
    private static String url = "jdbc:mongo://localhost/cre_Doc_Template_Mgt?debug=false&authDB=admin";

    // Mongo JDBC connection
    private static MongoConnection con = null;

    public static void main(String[] args) {

        try
        {
            // A query that can be translated and executed directly on MongoDB
            // String sql = "SELECT r_regionkey, r_name FROM region WHERE r_regionkey < 3;";
            String sql = "SELECT sum(enr) FROM college WHERE cName NOT IN (SELECT cName FROM tryout WHERE pPos = \"goalie\")";
            sql = "SELECT Airline , Abbreviation FROM AIRLINES WHERE Country = 'USA'";
            sql = "SELECT name FROM web_client_accelerator WHERE name LIKE '%Opera%'";
            sql = "SELECT count(*) FROM Documents AS T1 JOIN Templates AS T2 ON T1.Template_ID = T2.Template_ID WHERE T2.Template_Type_Code = 'PPT'";
            // sql = "SELECT Time_of_purchase , age , address FROM member ORDER BY Time_of_purchase";
            // sql = "SELECT name , Level_of_membership FROM visitor WHERE Level_of_membership > 4 ORDER BY age DESC";
            // translate(sql, con);

            // // A query that can be translated and executed directly on MongoDB (even though the given collection does not exist in the sample Mongo instance)
            // sql = "SELECT * FROM my_collection WHERE value < 3 AND value2 >= 'abc';";
            // translate(sql, con);

            // A query that can be executed by MongoDB directly which is translated into a query plan involving queries to MongoDB and operators done by UnityJDBC
            // This example REQUIRES a connection to the data source
            System.out.println("\nGetting connection:  " + url);
            con = (MongoConnection) DriverManager.getConnection(url, "admin", "ubco25");
            System.out.println("\nConnection successful for " + url);

            // sql = "SELECT r_regionkey, r_name, n_name, n_regionkey, n_nationkey FROM region R INNER JOIN nation N ON R.r_regionkey = N.n_regionkey WHERE r_regionkey < 3;";
            // sql = "SELECT r_regionkey, r_name, n_name, n_regionkey, n_nationkey FROM region JOIN nation ON r_regionkey = n_regionkey";
            // sql = "SELECT Airline , Abbreviation FROM AIRLINES WHERE Country = 'USA'";
            translate(sql, con);            
        }
        catch (SQLException ex)
        {
            System.out.println("Exception: " + ex);
        }
        finally
        {
            if (con != null)
            {
                try
                {	// Close the connection
                    con.close();
                    System.out.println("Connection closed.");
                }
                catch (SQLException ex)
                {
                    System.out.println("SQLException: " + ex);
                }
            }
        }
        System.exit(1);
        // String jsonFilePath = "test/convert/train_SLM_subset.json";
        // try (FileReader reader = new FileReader(jsonFilePath)) {
        //     JsonParser parser = new JsonParser();
        //     JsonElement root = parser.parse(reader);
        //     if (root.isJsonArray()) {
        //         JsonArray records = root.getAsJsonArray();
        //         for (JsonElement elem : records) {
        //             JsonObject obj = elem.getAsJsonObject();
        //             String refSql = obj.get("ref_sql").getAsString();
        //             //String mongoQuery = MongoDBTranslator.translateSQL(refSql);
        //             String mongoQuery = "Translated MongoDB Query"; // Placeholder for actual translation logic
        //             System.out.println("SQL: " + refSql);
        //             System.out.println("MongoDB Query: " + mongoQuery);
        //             System.out.println("-----");
        //         }
        //     } else {
        //         System.out.println("JSON file does not contain an array.");
        //     }
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }
    }
    /**
     * Translates a SQL query to MongoDB (if possible).
     * 
     * @param sql
     *            SQL query to translate
     * @param connection
     *            MongoConnection or null if translating without a connection
     * @throws SQLException
     *             if a database or translation error occurs
     */
    public static void translate(String sql, MongoConnection connection)
            throws SQLException
    {
        MongoStatement stmt;
        GlobalQuery gq;
        GlobalSchema schema = null;

        if (connection != null)
        {   // Translate using a connection
            schema = connection.getGlobalSchema();
            stmt = (MongoStatement) connection.createStatement();
        }
        else
        {   // Translate without a connection
            stmt = new MongoStatement();
        }

        boolean schemaValidation = false;                // This will allow a query to be executed without a schema (or a Mongo connection)
        gq = stmt.translateQuery(sql, schemaValidation, schema);

        System.out.println("\n\nTranslating SQL query: \n" + sql + '\n');
        String mongoQuery = stmt.getQueryString();
        if (mongoQuery.equals(""))
        {    // Query could not be executed by Mongo, output Unity execution plan
            System.out.println("SQL query cannot be directly executed by UnityJDBC.  Here is UnityJDBC logical query tree: ");
            gq.printTree();
            System.out.println("\nExecution plan: ");
            Operator.printTree(gq.getExecutionTree(), 1);
        }
        else
        {
            System.out.println("To Mongo query: \n" + mongoQuery);
        }
        if (connection != null)
            stmt.close();
    }
}