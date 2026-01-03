package app.store;

import app.model.Student;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;   // <-- KRİTİK: IMap buradan gelir

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HazelcastStore implements AutoCloseable {

    private static final String MAP_NAME = "students";
    private final HazelcastInstance hz;
    private final IMap<String, Student> map;

    /**
     * mode:
     *  - "embedded" => aynı JVM içinde Hazelcast member açar (en kolayı)
     *  - "client"   => dışarıdaki cluster'a client ile bağlanır
     */
    public HazelcastStore(String mode, String clusterAddress) {
        if ("client".equalsIgnoreCase(mode)) {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress(clusterAddress);
            this.hz = HazelcastClient.newHazelcastClient(clientConfig);
        } else {
            Config cfg = new Config();
            cfg.setClusterName("dev");
            this.hz = Hazelcast.newHazelcastInstance(cfg);
        }
        this.map = hz.getMap(MAP_NAME);
    }

    public void seedIfEmpty(List<Student> students) {
        // Sentinel kontrol
        if (map.containsKey(students.get(0).getStudent_no())) return;

        Map<String, Student> batch = new HashMap<>(2000);
        int i = 0;
        for (Student s : students) {
            batch.put(s.getStudent_no(), s);
            i++;
            if (i % 2000 == 0) {
                map.putAll(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            map.putAll(batch);
        }
    }

    public Student getByStudentNo(String studentNo) {
        return map.get(studentNo);
    }

    @Override
    public void close() {
        try {
            hz.shutdown();
        } catch (Exception ignored) {}
    }
}
