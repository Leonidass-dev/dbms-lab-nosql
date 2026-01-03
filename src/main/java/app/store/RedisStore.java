package app.store;

import app.model.Student;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.*;

import java.util.*;

public class RedisStore implements AutoCloseable {

    private static final String KEY_PREFIX = "student:";
    private final JedisPool pool;
    private final ObjectMapper mapper;

    public RedisStore(String host, int port, ObjectMapper mapper) {
        this.mapper = mapper;
        this.pool = new JedisPool(new JedisPoolConfig(), host, port);
    }

    public void seedIfEmpty(List<Student> students) {
        // Sentinel kontrolü: ilk öğrenci varsa seed atlama
        String sentinelKey = KEY_PREFIX + students.get(0).getStudent_no();
        try (Jedis jedis = pool.getResource()) {
            if (jedis.exists(sentinelKey)) return;

            Pipeline p = jedis.pipelined();
            int i = 0;
            for (Student s : students) {
                try {
                    String json = mapper.writeValueAsString(s);
                    p.set(KEY_PREFIX + s.getStudent_no(), json);
                } catch (Exception e) {
                    throw new RuntimeException("Redis JSON serialize error", e);
                }

                i++;
                if (i % 1000 == 0) {
                    p.sync();
                }
            }
            p.sync();
        }
    }

    public Student getByStudentNo(String studentNo) {
        try (Jedis jedis = pool.getResource()) {
            String json = jedis.get(KEY_PREFIX + studentNo);
            if (json == null) return null;
            return mapper.readValue(json, Student.class);
        } catch (Exception e) {
            throw new RuntimeException("Redis read/parse error", e);
        }
    }

    @Override
    public void close() {
        pool.close();
    }
}
