package company.vk.edu.distrib.compute.vitos23.audit;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.AuditEvent;
import company.vk.edu.distrib.compute.AuditableKVService;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.KVServiceImpl;
import company.vk.edu.distrib.compute.vitos23.exception.ServerException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static company.vk.edu.distrib.compute.vitos23.audit.AuditParameters.AUDIT_TOPIC_NAME;

public class AuditableKVServiceImpl extends KVServiceImpl implements AuditableKVService {

    private static final int SYNC_SEND_TIMEOUT_SECONDS = 5;

    private final Gson gson = new Gson();

    private Producer<String, String> auditEventProducer;
    private boolean async;

    public AuditableKVServiceImpl(
            int port,
            EntityRequestProcessor entityRequestProcessor
    ) throws IOException {
        super(port, entityRequestProcessor);
    }

    /// Should be invoked before [#start()]
    @Override
    public void setBootstrapServers(String bootstrapServers) {
        if (auditEventProducer != null) {
            throw new IllegalStateException("Bootstrap servers cannot be changed");
        }
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.LINGER_MS_CONFIG, 0 // To make benchmark result more representative
        );
        auditEventProducer = new KafkaProducer<>(config);
    }

    /// Should be invoked before [#start()]
    @Override
    public void setAsync(boolean enabled) {
        async = enabled;
    }

    @Override
    public void stop() {
        try {
            if (auditEventProducer != null) {
                auditEventProducer.close();
            }
        } finally {
            super.stop();
        }
    }

    @Override
    protected void onEntityRequestReceived(HttpExchange exchange, String id) {
        if (auditEventProducer == null) {
            throw new IllegalStateException("Not initialized kafka producer");
        }
        var event = new AuditEvent(exchange.getRequestMethod(), id, System.currentTimeMillis());
        var record = new ProducerRecord<>(AUDIT_TOPIC_NAME, id, gson.toJson(event));
        var future = auditEventProducer.send(record);
        if (!async) {
            try {
                future.get(SYNC_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new ServerException(e);
            }
        }
    }

}
