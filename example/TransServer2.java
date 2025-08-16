// File: TransServer2.java  

// Run the follwing in terminal:

// javac -cp .:mongodb_unityjdbc_full.jar TransServer2.java
// java  -cp .:mongodb_unityjdbc_full.jar TransServer2


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import mongodb.jdbc.MongoConnection;
import mongodb.jdbc.MongoStatement;
import mongodb.query.MongoQuery;
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

// EDIT: new imports for schema mkdirs + thread-safe cache
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

// EDIT: imports needed to EXECUTE the Mongo query
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;

/**
 * Example usage: http://localhost:8082/translate?db=tpch&sql=SELECT%20*%20FROM%20nation%20WHERE%20n_regionkey%3C3
 */
public class TransServer2 {  // EDIT: class name matches file name

    public static class Translator {

        // EDIT: use ConcurrentHashMap for thread-safe connection cache
        private static final ConcurrentHashMap<String, MongoConnection> connections = new ConcurrentHashMap<>();

        // EDIT: helper to close all connections (used by shutdown hook)
        public static void closeAll() {
            for (MongoConnection c : connections.values()) {
                try { c.close(); } catch (Exception ignore) {}
            }
        }

        /**
         * Translates a SQL query to MongoDB (if possible).
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

                // EDIT: make sure schema directory exists & add auto-generate/flush logic on first connection
                String schemaPath = "schema/mongo_" + databaseName + ".xml";
                File schemaFile = new File(schemaPath);
                File parentDir = schemaFile.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    if (parentDir.mkdirs()) {
                        System.out.println("Created schema directory: " + parentDir.getPath());
                    }
                }

                // Base URL (no auth; your mongod is no-auth)
                String base = "jdbc:mongo://localhost/" + databaseName + "?debug=false";

                if (!schemaFile.exists()) {
                    // EDIT: FIRST TIME — build schema with a TEMP connection and CLOSE it to flush XML
                    String buildUrl = base + "&schema=" + schemaPath + "&rebuildSchema=true&generate";
                    System.out.println("Connecting (schema build) with URL: " + buildUrl);
                    try (MongoConnection buildCon = (MongoConnection) DriverManager.getConnection(buildUrl)) {
                        // optional: touch the connection/metadata here if needed
                    } catch (SQLException e) {
                        System.out.println("Schema build connection FAILED for DB '" + databaseName + "': " + e);
                        throw e;
                    }
                    System.out.println("Schema build finished for DB '" + databaseName + "'.");
                }

                // EDIT: NORMAL connection using the existing schema file; this one we cache
                String url = base + "&schema=" + schemaPath;
                System.out.println("Connecting (normal) with URL: " + url);
                try {
                    connection = (MongoConnection) DriverManager.getConnection(url);
                    connections.put(databaseName, connection); // cache after success
                } catch (SQLException e) {
                    System.out.println("Connection FAILED for DB '" + databaseName + "': " + e);
                    throw e; // let handler return JSON error
                }
            }

            // Translate using a connection
            if (connection != null) {
                schema = connection.getGlobalSchema();
                stmt = (MongoStatement) connection.createStatement();
            } else {
                // Shouldn't happen, but keep fallback
                stmt = new MongoStatement();
            }

            boolean schemaValidation = false; // allow translation without strict schema validation
            gq = stmt.translateQuery(sql, schemaValidation, schema);

            System.out.println("\n\nTranslating SQL query: \n" + sql + '\n');
            String mongoQuery = stmt.getQueryString();
            if (mongoQuery.equals("")) {
                System.out.println("SQL query cannot be directly executed by MongoDB. Logical query tree:");
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
        // EDIT: bump thread pool for small parallelism
        server.setExecutor(Executors.newFixedThreadPool(8));
        System.out.println("Listening on http://localhost:8082/translate?db=...&sql=...");

        // EDIT: add shutdown hook to flush schemas by closing cached connections
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down. Closing Mongo connections...");
            try { Translator.closeAll(); } catch (Exception ignore) {}
        }));

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

            try {
                String mongo = Translator.translate(sql, db);
                sendJson(exchange, 200, Map.of(
                        "db", db,
                        "sql", sql,
                        "mongo", mongo
                ));
                return; // EDIT: ensure we don't fall through
            } catch (SQLException e) {
                // EDIT: always return JSON on errors (prevents client timeouts)
                sendJson(exchange, 500, Map.of(
                        "db", db,
                        "sql", sql,
                        "error", e.toString()
                ));
                return;
            }
        }

        private static void sendJson(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
            String json = toJson(body);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        // EDIT: add minimal escaping for quotes, backslashes, and newlines
        private static String escape(String s) {
            return s
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
        }

        // EDIT: simple JSON builder with escaping
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
