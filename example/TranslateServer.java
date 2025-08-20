// File: TranslateServer.java
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import mongodb.jdbc.MongoConnection;
import mongodb.jdbc.MongoStatement;
import unity.annotation.GlobalSchema;
import unity.operators.Operator;
import unity.query.GlobalQuery;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.Executors;

import java.sql.DriverManager;
import java.util.HashMap;

/**
 * Example usage: http://localhost:8082/translate?db=tpch&sql=SELECT%20*%20FROM%20nation%20WHERE%20n_regionkey%3C3
 */
public class TranslateServer {
    
    public static class Translator {

        private static HashMap <String, MongoConnection> connections = new HashMap<>();

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
        public static String translate(String sql, String databaseName)
                throws SQLException
        {
            MongoStatement stmt;
            GlobalQuery gq;
            GlobalSchema schema = null;

            // Lookup or create a connection for the given database
            MongoConnection connection = connections.get(databaseName);
            if (connection == null) {
                System.out.println("Creating new connection for database: " + databaseName);
                String url = "jdbc:mongo://localhost/"+databaseName+"?schema=example/schema/mongo_"+databaseName+".xml&debug=false";
                connection = (MongoConnection) DriverManager.getConnection(url);
                connections.put(databaseName, connection);
            }

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
            
            return mongoQuery;
        }
    }

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8082), 0);
        server.createContext("/translate", new TranslateHandler());
        server.setExecutor(Executors.newFixedThreadPool(4));
        System.out.println("Listening on http://localhost:8082/translate?db=...&sql=...");
        server.start();
    }

    static class TranslateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, Map.of("error", "Only GET is supported"));
                return;
            }

            Map<String, String> params = queryToMap(exchange.getRequestURI().getRawQuery());
            String db = params.get("db");
            String sql = params.get("sql");

            if (db == null || sql == null) {
                sendJson(exchange, 400, Map.of("error", "Missing required parameters 'db' and 'sql'"));
                return;
            }

            String mongo = "Error";
            
            try {
                mongo = Translator.translate(sql, db);
            } catch (SQLException e) {
                // mongo = e.toString();
                // // Might have to change to return error
                // return;
                sendJson(exchange, 500, Map.of(
                        "db", db,
                        "sql", sql,
                        "error", e.toString()
                ));
            }
            
            sendJson(exchange, 200, Map.of(
                    "db", db,
                    "sql", sql,
                    "mongo", mongo+";"
            ));
        }

        private static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
            String json = toJson(body);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        // Very simple JSON builder (for demo only — replace with Jackson if you want robust handling)
        private static String toJson(Object body) 
        {
            if (body instanceof Map<?, ?> map) {
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                for (var entry : map.entrySet()) {
                    if (!first) sb.append(",");
                    sb.append("\"").append(entry.getKey()).append("\":\"")
                    .append(entry.getValue().toString().replace("\\", "\\\\").replace("\"", "\\\""))
                    .append("\"");
                    first = false;
                }
                sb.append("}");
                return sb.toString();
            }
            return "{}";
        }

        private static Map<String, String> queryToMap(String query) {
            Map<String, String> result = new HashMap<>();
            if (query == null || query.isEmpty()) return result;
            for (String param : query.split("&")) {
                String[] pair = param.split("=", 2);
                if (pair.length > 0) {
                    String key = urlDecode(pair[0]);
                    String value = pair.length > 1 ? urlDecode(pair[1]) : "";
                    result.put(key, value);
                }
            }
            return result;
        }

        private static String urlDecode(String s) {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }
    }
}