package company.vk.edu.distrib.compute.vitos23.audit;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.vitos23.DirectEntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.InMemoryDao;

import java.io.IOException;

public class Vitos23AuditableKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        Dao<byte[]> dao = new InMemoryDao<>();
        EntityRequestProcessor entityRequestProcessor = new DirectEntityRequestProcessor(dao);
        return new AuditableKVServiceImpl(port, entityRequestProcessor);
    }
}
