// File: TranslateServer.java
// Build: javac -cp .:mongodb_unityjdbc_full.jar TranslateServerVer.java
// Run  : java  -cp .:mongodb_unityjdbc_full.jar TranslateServerVer
//
// Example call:
//   http://localhost:8082/translate?db=tpch&sql=SELECT%20*%20FROM%20nation%20WHERE%20n_regionkey%3C3

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
import java.io.File;

public class TranslateServerVer {
    
    public static class Translator {

        // (kept) simple cache; OK for light use. For heavy parallelism switch to ConcurrentHashMap
        private static HashMap<String, MongoConnection> connections = new HashMap<>();

        /**
         * Translates a SQL query to MongoDB (if possible).
         * 
         * NOTE (minimal change):
         *  - Ensure a schema XML exists on first connection (avoids "Non-existing table" errors).
         *  - Then open the normal connection using that schema and cache it.
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

                // ---- MINIMAL FIX: ensure schema exists once before normal connect ----
                // Keep your original folder "example/schema/..." to avoid big changes.
                String schemaPath = "example/schema/mongo_" + databaseName + ".xml";
                String baseUrl    = "jdbc:mongo://localhost/" + databaseName + "?debug=false";

                // Make sure the schema directory exists
                File schemaFile = new File(schemaPath);
                File parentDir  = schemaFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }

                // If schema file is missing, build it (safe to call even if it exists)
                // This is the key to avoid "Non-existing table referenced" when schema was never generated.
                String buildUrl = baseUrl + "&schema=" + schemaPath + "&rebuildSchema=true&generate";
                System.out.println("Connecting (schema build): " + buildUrl);
                try (MongoConnection buildCon = (MongoConnection) DriverManager.getConnection(buildUrl)) {
                    // closing buildCon flushes schema XML to disk
                } catch (SQLException e) {
                    System.out.println("Schema build FAILED for '" + databaseName + "': " + e);
                    throw e; // bubble up as 500 to the client
                }
                System.out.println("Schema build done for '" + databaseName + "'.");

                // Normal cached connection using the schema file
                String url = baseUrl + "&schema=" + schemaPath + "&debug=false";
                System.out.println("Connecting (normal): " + url);
                connection = (MongoConnection) DriverManager.getConnection(url);
                connections.put(databaseName, connection);
            }

            // Translate using a connection (unchanged)
            if (connection != null) {
                schema = connection.getGlobalSchema();
                stmt = (MongoStatement) connection.createStatement();
            } else {
                // Translate without a connection (shouldn't happen)
                stmt = new MongoStatement();
            }

            boolean schemaValidation = false;  // allow translation even if schema not strict
            gq = stmt.translateQuery(sql, schemaValidation, schema);

            System.out.println("\n\nTranslating SQL query: \n" + sql + '\n');
            String mongoQuery = stmt.getQueryString();
            if (mongoQuery.equals("")) {
                System.out.println("SQL query cannot be directly executed by UnityJDBC.  Here is UnityJDBC logical query tree: ");
                gq.printTree();
                System.out.println("\nExecution plan: ");
                Operator.printTree(gq.getExecutionTree(), 1);
            } else {
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
                return; // IMPORTANT: stop after sending error
            }

            Map<String, String> params = queryToMap(exchange.getRequestURI().getRawQuery());
            String db = params.get("db");
            String sql = params.get("sql");

            if (db == null || sql == null) {
                sendJson(exchange, 400, Map.of("error", "Missing required parameters 'db' and 'sql'"));
                return; // IMPORTANT: stop after sending error
            }

            try {
                String mongo = Translator.translate(sql, db);
                sendJson(exchange, 200, Map.of(
                        "db", db,
                        "sql", sql,
                        "mongo", mongo
                ));
                return; // IMPORTANT: stop after sending success
            } catch (SQLException e) {
                // MINIMAL FIX: always respond once with error JSON, then return
                sendJson(exchange, 500, Map.of(
                        "db", db,
                        "sql", sql,
                        "error", e.toString()
                ));
                return; // IMPORTANT: prevent a second response
            }
        }

        private static void sendJson(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
            String json = toJson(body);             // minimal JSON builder with escaping below
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        // MINIMAL ESCAPING: prevent broken JSON if SQL/error contain quotes or newlines
        private static String escape(String s) {
            return s
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
        }

        // Keep your light-weight JSON (no extra deps), but escape values
        private static String toJson(Map<String, ?> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (var entry : map.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(escape(String.valueOf(entry.getKey()))).append("\":");
                Object v = entry.getValue();
                if (v == null) {
                    sb.append("null");
                } else {
                    sb.append("\"").append(escape(String.valueOf(v))).append("\"");
                }
                first = false;
            }
            sb.append("}");
            return sb.toString();
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
