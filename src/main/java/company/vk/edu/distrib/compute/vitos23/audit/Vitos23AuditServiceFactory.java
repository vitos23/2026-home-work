package company.vk.edu.distrib.compute.vitos23.audit;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class Vitos23AuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new AuditServiceImpl(bootstrapServers, consumerGroupId, Path.of("storage/vitos23/audit"));
    }
}
