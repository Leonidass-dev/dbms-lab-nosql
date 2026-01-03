package app;

import app.model.Student;
import app.store.HazelcastStore;
import app.store.MongoStore;
import app.store.RedisStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

public class Main {

    private static final int PORT = 8080;

    // README örneğine uygun: 2025000001’den başlatıp 10.000 kayıt
    private static final long STUDENT_NO_START = 2025000001L;
    private static final int SEED_COUNT = 10_000;

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // Konfigürasyon (env ile değiştirilebilir)
        String redisHost = env("REDIS_HOST", "localhost");
        int redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));

        String mongoUri = env("MONGO_URI", "mongodb://localhost:27017");
        String mongoDb = env("MONGO_DB", "nosql_lab");
        String mongoCol = env("MONGO_COLLECTION", "students");

        String hzMode = env("HZ_MODE", "embedded"); // embedded | client
        String hzAddr = env("HZ_ADDR", "localhost:5701");

        List<Student> seed = generateStudents(SEED_COUNT);

        RedisStore redisStore = new RedisStore(redisHost, redisPort, mapper);
        HazelcastStore hazelcastStore = new HazelcastStore(hzMode, hzAddr);
        MongoStore mongoStore = new MongoStore(mongoUri, mongoDb, mongoCol);

        // Seed
        redisStore.seedIfEmpty(seed);
        hazelcastStore.seedIfEmpty(seed);
        mongoStore.seedIfEmpty(seed);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { redisStore.close(); } catch (Exception ignored) {}
            try { hazelcastStore.close(); } catch (Exception ignored) {}
            try { mongoStore.close(); } catch (Exception ignored) {}
        }));

        // HTTP Server
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.setExecutor(Executors.newFixedThreadPool(Math.max(8, Runtime.getRuntime().availableProcessors() * 2)));

        // /nosql-lab-rd/student_no=2025000001   (veya /nosql-lab-rd?student_no=...)
        server.createContext("/nosql-lab-rd", exchange -> handle(exchange, mapper, (studentNo) -> redisStore.getByStudentNo(studentNo)));

        // /nosql-lab-hz/student_no=2025000001
        server.createContext("/nosql-lab-hz", exchange -> handle(exchange, mapper, (studentNo) -> hazelcastStore.getByStudentNo(studentNo)));

        // /nosql-lab-mon/student_no=2025000001
        server.createContext("/nosql-lab-mon", exchange -> handle(exchange, mapper, (studentNo) -> mongoStore.getByStudentNo(studentNo)));

        // basit health
        server.createContext("/health", exchange -> {
            byte[] body = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });

        server.start();
        System.out.println("Server started on http://localhost:" + PORT);
        System.out.println("Examples:");
        System.out.println("  http://localhost:8080/nosql-lab-rd/student_no=2025000001");
        System.out.println("  http://localhost:8080/nosql-lab-hz/student_no=2025000001");
        System.out.println("  http://localhost:8080/nosql-lab-mon/student_no=2025000001");
    }

    @FunctionalInterface
    interface StudentFetcher {
        Student fetch(String studentNo);
    }

    private static void handle(HttpExchange exchange, ObjectMapper mapper, StudentFetcher fetcher) throws IOException {
        try {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, mapper, Map.of("error", "Method Not Allowed"));
                return;
            }

            String studentNo = extractStudentNo(exchange);
            if (studentNo == null || studentNo.isBlank()) {
                sendJson(exchange, 400, mapper, Map.of("error", "student_no parameter is required"));
                return;
            }

            Student s = fetcher.fetch(studentNo);
            if (s == null) {
                sendJson(exchange, 404, mapper, Map.of("error", "student not found", "student_no", studentNo));
                return;
            }

            sendJson(exchange, 200, mapper, s);

        } catch (Exception e) {
            sendJson(exchange, 500, mapper, Map.of("error", "internal_error", "message", e.getMessage()));
        }
    }

    /**
     * README’deki path formatı: /nosql-lab-rd/student_no=2025000001
     * Ek olarak query formatını da destekler: /nosql-lab-rd?student_no=2025000001
     */
    private static String extractStudentNo(HttpExchange exchange) {
        // 1) path segmentten yakala
        String path = exchange.getRequestURI().getPath(); // /nosql-lab-rd/student_no=2025000001
        int idx = path.indexOf("student_no=");
        if (idx >= 0) {
            return path.substring(idx + "student_no=".length()).trim();
        }

        // 2) query stringten yakala
        String query = exchange.getRequestURI().getRawQuery(); // student_no=...
        if (query == null || query.isBlank()) return null;

        Map<String, String> q = parseQuery(query);
        return q.get("student_no");
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> map = new HashMap<>();
        for (String pair : rawQuery.split("&")) {
            int i = pair.indexOf('=');
            if (i <= 0) continue;
            String k = urlDecode(pair.substring(0, i));
            String v = urlDecode(pair.substring(i + 1));
            map.put(k, v);
        }
        return map;
    }

    private static String urlDecode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static void sendJson(HttpExchange exchange, int status, ObjectMapper mapper, Object bodyObj) throws IOException {
        byte[] body = mapper.writeValueAsBytes(bodyObj);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static List<Student> generateStudents(int count) {
        String[] names = {
                "Münip Utandı", "Nağme Yarkın", "Aysun Gültekin", "Deniz Aksoy", "Ece Yıldırım",
                "Mehmet Karaca", "Elif Demir", "Ahmet Yılmaz", "Zeynep Kaya", "Can Koç"
        };
        String[] deps = {
                "Classical Turkish Music", "Turkish Folk Music", "Computer Engineering",
                "Electrical Engineering", "Medicine", "Mathematics", "Physics"
        };

        Random r = new Random(42); // deterministik seed
        List<Student> list = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            long no = STUDENT_NO_START + i;
            String name = names[r.nextInt(names.length)];
            String dep = deps[r.nextInt(deps.length)];
            list.add(new Student(String.valueOf(no), name, dep));
        }
        return list;
    }
}
