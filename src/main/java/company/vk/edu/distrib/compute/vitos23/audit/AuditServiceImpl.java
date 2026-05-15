package company.vk.edu.distrib.compute.vitos23.audit;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static company.vk.edu.distrib.compute.vitos23.audit.AuditParameters.AUDIT_TOPIC_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("PMD.AvoidSynchronizedAtMethodLevel")
public class AuditServiceImpl implements AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditServiceImpl.class);
    private static final Duration POLL_DURATION = Duration.ofSeconds(1);

    private final Consumer<String, String> auditConsumer;
    private final Path storageFile;
    private final Gson gson = new Gson();
    private final List<AuditEvent> consumedEvents = Collections.synchronizedList(new ArrayList<>());

    private Thread listenerThread;
    private BufferedWriter storageWriter;

    public AuditServiceImpl(String bootstrapServers, String consumerGroupId, Path storageDir) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        );
        auditConsumer = new KafkaConsumer<>(config);
        storageFile = storageDir.resolve("audit-events-%s.jsonl".formatted(consumerGroupId));
    }

    @Override
    public synchronized void start() {
        if (listenerThread != null) {
            return;
        }
        try {
            Files.createDirectories(storageFile.getParent());
            storageWriter =
                    Files.newBufferedWriter(storageFile, UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize audit storage", e);
        }
        auditConsumer.subscribe(List.of(AUDIT_TOPIC_NAME));
        listenerThread = Thread.ofVirtual().start(this::pollLoop);
    }

    private void pollLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ConsumerRecords<String, String> records = auditConsumer.poll(POLL_DURATION);
                if (records.isEmpty()) {
                    continue;
                }
                for (var record : records) {
                    tryConsumeRecord(record);
                }
                commit();
            } catch (IOException e) {
                log.error("Failed to persist audit events", e);
            }
        }
    }

    private void tryConsumeRecord(ConsumerRecord<String, String> record) throws IOException {
        try {
            AuditEvent event = gson.fromJson(record.value(), AuditEvent.class);
            storageWriter.write(record.value());
            storageWriter.newLine();
            consumedEvents.add(event);
        } catch (JsonSyntaxException e) {
            log.warn("Skipped invalid audit event record {}", record);
        }
    }

    private void commit() throws IOException {
        storageWriter.flush();
        auditConsumer.commitSync();
    }

    @Override
    @SuppressWarnings("PMD.NullAssignment") // For Codacy
    public synchronized void stop() {
        if (listenerThread == null || !listenerThread.isAlive()) {
            closeResources();
            return;
        }
        listenerThread.interrupt();
        try {
            listenerThread.join();
            listenerThread = null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } finally {
            closeResources();
        }
    }

    private void closeResources() {
        try {
            auditConsumer.close();
        } finally {
            closeWriter();
        }
    }

    @SuppressWarnings("PMD.NullAssignment") // For Codacy
    private void closeWriter() {
        if (storageWriter == null) {
            return;
        }
        try {
            storageWriter.close();
            storageWriter = null;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<AuditEvent> listAuditEntries() {
        return Collections.unmodifiableList(consumedEvents);
    }
}
