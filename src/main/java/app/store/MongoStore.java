package app.store;

import app.model.Student;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class MongoStore implements AutoCloseable {

    private final MongoClient client;
    private final MongoCollection<Document> col;

    public MongoStore(String mongoUri, String dbName, String collectionName) {
        this.client = MongoClients.create(mongoUri);
        MongoDatabase db = client.getDatabase(dbName);
        this.col = db.getCollection(collectionName);

        // student_no üzerinde index (performans + opsiyonel unique)
        // Unique istemiyorsan unique(true) kaldır.
        IndexOptions opts = new IndexOptions().unique(true);
        col.createIndex(Indexes.ascending("student_no"), opts);
    }

    public void seedIfEmpty(List<Student> students) {
        // Sentinel kontrol
        String sentinelNo = students.get(0).getStudent_no();
        Document existing = col.find(eq("student_no", sentinelNo)).first();
        if (existing != null) return;

        List<WriteModel<Document>> bulk = new ArrayList<>(2000);
        int i = 0;

        for (Student s : students) {
            Document doc = new Document("student_no", s.getStudent_no())
                    .append("name", s.getName())
                    .append("department", s.getDepartment());

            bulk.add(new InsertOneModel<>(doc));
            i++;

            if (i % 2000 == 0) {
                col.bulkWrite(bulk, new BulkWriteOptions().ordered(false));
                bulk.clear();
            }
        }

        if (!bulk.isEmpty()) {
            col.bulkWrite(bulk, new BulkWriteOptions().ordered(false));
        }
    }

    public Student getByStudentNo(String studentNo) {
        Document d = col.find(eq("student_no", studentNo)).first();
        if (d == null) return null;

        return new Student(
                d.getString("student_no"),
                d.getString("name"),
                d.getString("department")
        );
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (Exception ignored) {}
    }
}
