package org.reactivecommons.async.servicebus.communucations;

import com.azure.core.management.Region;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.servicebus.models.NamespaceSku;
import com.azure.resourcemanager.servicebus.models.Queue;
import com.azure.resourcemanager.servicebus.models.ServiceBusNamespace;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.QueueDescription;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Log4j2
@AllArgsConstructor
public class TopologyCreator {

    //private final AzureResourceManager azureResourceManager;
    private final ManagementClient managementClient;

//    public Mono<String> createQueue(String name) {
//
//        log.info("Creando topologia de service bus....");
//
//        ServiceBusNamespace serviceBusNamespace = azureResourceManager.serviceBusNamespaces()
//                .define("namespaceName")
//                .withRegion(Region.US_WEST)
//                .withNewResourceGroup("prueba_resorce_name")
//                .withSku(NamespaceSku.STANDARD)
//                .create();
//
//
//        System.out.println("Created service bus " + serviceBusNamespace.name());
//
//        Queue firstQueue = serviceBusNamespace.queues().define(name)
//                .withSession()
//                .withDefaultMessageTTL(Duration.ofMinutes(10))
//                .withExpiredMessageMovedToDeadLetterQueue()
//                .withMessageMovedToDeadLetterQueueOnMaxDeliveryCount(40)
//                .create();
//
//        return Mono.just("");
//    }

    public Mono<String> createQueue(String name) {

        log.info("Creando queue de service bus....");

        try {
            if(!managementClient.queueExists(name))
            {
                managementClient.createQueue(name);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando cola ServiceBusException".concat(e.getMessage()));
        } catch (InterruptedException e) {
            log.info("Error creando cola InterruptedException ".concat(e.getMessage()));
        }

        return Mono.just("");
    }

    public Mono<String> createTopic(String name) {

        log.info("Creando topic de service bus....");

        try {
            if(!managementClient.topicExists(name))
            {
                managementClient.createTopic(name);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando tipic ServiceBusException".concat(e.getMessage()));
        } catch (InterruptedException e) {
            log.info("Error creando tipic InterruptedException ".concat(e.getMessage()));
        }

        return Mono.just("");
    }
}
