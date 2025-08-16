// File: TransServer3.java
//
// Compile (macOS/Linux):
//   javac -cp .:mongodb_unityjdbc_full.jar TransServer2.java
// Run:
//   java  -cp .:mongodb_unityjdbc_full.jar TransServer2
//
// Endpoints:
//   GET /translate?db=<db>&sql=<URL-ENCODED-SQL>
//   GET /warmup?db=<db1,db2,...>    // proactively build schema & cache connections (no timeout)

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

// Thread-safe cache for connections
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class TransServer3 {

    // ---- Translator: merges your version + supervisor logic, no timeouts ----
    public static class Translator {

        private static final ConcurrentHashMap<String, MongoConnection> connections = new ConcurrentHashMap<>();

        public static void closeAll() {
            for (MongoConnection c : connections.values()) {
                try { c.close(); } catch (Exception ignore) {}
            }
        }

        /** Ensure a cached connection for a DB; build schema if missing. */
        public static MongoConnection ensureConnection(String databaseName, boolean rebuildIfMissing) throws SQLException {
            MongoConnection cached = connections.get(databaseName);
            if (cached != null) return cached;

            System.out.println("Creating new connection for database: " + databaseName);

            // You used "schema/..." (your supervisor used "example/schema/...").
            // Keep your path to match where you’ve been writing XMLs.
            String schemaPath = "schema/mongo_" + databaseName + ".xml";
            File schemaFile = new File(schemaPath);
            File parentDir = schemaFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                if (parentDir.mkdirs()) {
                    System.out.println("Created schema directory: " + parentDir.getPath());
                }
            }

            // Base URL; no auth & no explicit timeouts => no artificial restrictions here.
            String base = "jdbc:mongo://localhost/" + databaseName + "?debug=false";

            // If schema file missing and allowed to build, do a generate/flush connection
            if (!schemaFile.exists() && rebuildIfMissing) {
                String buildUrl = base + "&schema=" + schemaPath + "&rebuildSchema=true&generate";
                System.out.println("Connecting (schema build) with URL: " + buildUrl);
                try (MongoConnection buildCon = (MongoConnection) DriverManager.getConnection(buildUrl)) {
                    // touch if needed; closing will flush schema XML
                } catch (SQLException e) {
                    System.out.println("Schema build connection FAILED for DB '" + databaseName + "': " + e);
                    throw e;
                }
                System.out.println("Schema build finished for DB '" + databaseName + "'.");
            }

            String url = base + "&schema=" + schemaPath;
            System.out.println("Connecting (normal) with URL: " + url);
            MongoConnection con = (MongoConnection) DriverManager.getConnection(url);
            connections.put(databaseName, con);
            return con;
        }

        /** Translate SQL -> MongoDB; prints plan if not directly executable (supervisor logic preserved). */
        public static String translate(String sql, String databaseName) throws SQLException {
            MongoConnection connection = ensureConnection(databaseName, true);
            GlobalSchema schema = connection.getGlobalSchema();

            MongoStatement stmt = (MongoStatement) connection.createStatement();

            // Allow translation without strict schema validation (from supervisor code)
            boolean schemaValidation = false;
            GlobalQuery gq = stmt.translateQuery(sql, schemaValidation, schema);

            System.out.println("\n\nTranslating SQL query: \n" + sql + '\n');
            String mongoQuery = stmt.getQueryString();

            if (mongoQuery.equals("")) {
                // If not directly executable in Mongo, output UnityJDBC logical tree + execution plan
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

    // ---- Server bootstrap (no timeouts configured) ----
    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8082), 0);
        server.createContext("/translate", new TranslateHandler());
        server.createContext("/warmup", new WarmupHandler()); // new: proactively build schemas
        server.setExecutor(Executors.newFixedThreadPool(8));  // small parallelism; no request time limits
        System.out.println("Listening on http://localhost:8082/translate?db=...&sql=...");
        System.out.println("Warmup endpoint: http://localhost:8082/warmup?db=db1,db2,...");

        // Close cached connections on shutdown so schemas are flushed to disk
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down. Closing Mongo connections...");
            try { Translator.closeAll(); } catch (Exception ignore) {}
        }));

        server.start();
    }

    // ---- /translate handler: mirrors supervisor behavior, always JSON ----
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
            } catch (SQLException e) {
                // Always JSON error (prevents client timeouts)
                sendJson(exchange, 500, Map.of(
                        "db", db,
                        "sql", sql,
                        "error", e.toString()
                ));
            }
        }
    }

    // ---- /warmup handler: proactively build schemas & cache connections ----
    static class WarmupHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, Map.of("error", "Only GET is supported"));
                return;
            }
            Map<String, String> params = queryToMap(exchange.getRequestURI().getRawQuery());
            String dbParam = params.get("db");
            if (dbParam == null || dbParam.isBlank()) {
                sendJson(exchange, 400, Map.of("error", "Missing required parameter 'db' (comma-separated list allowed)"));
                return;
            }

            String[] dbs = dbParam.split(",");
            StringBuilder ok = new StringBuilder();
            StringBuilder failed = new StringBuilder();

            for (String raw : dbs) {
                String db = raw.trim();
                if (db.isEmpty()) continue;
                try {
                    Translator.ensureConnection(db, true);
                    if (ok.length() > 0) ok.append(", ");
                    ok.append(db);
                } catch (SQLException e) {
                    if (failed.length() > 0) failed.append(", ");
                    failed.append(db).append(" (").append(e.toString()).append(")");
                }
            }

            sendJson(exchange, 200, Map.of(
                    "requested", dbParam,
                    "initialized", ok.toString(),
                    "failed", failed.toString()
            ));
        }
    }

    // ---- Minimal JSON + query parsing helpers (like your version) ----
    private static void sendJson(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
        String json = toJson(body);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String escape(String s) {
        return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

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
