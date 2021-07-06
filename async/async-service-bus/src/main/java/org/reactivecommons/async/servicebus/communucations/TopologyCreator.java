package org.reactivecommons.async.servicebus.communucations;


import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.SubscriptionDescription;
import com.microsoft.azure.servicebus.management.TopicDescription;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.rules.CorrelationFilter;
import com.microsoft.azure.servicebus.rules.RuleDescription;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;

@Log
@AllArgsConstructor
public class TopologyCreator {

    //private final AzureResourceManager azureResourceManager;
    private final ManagementClient managementClient;
    //private ServiceBusAdministrationClient serviceBusAdministrationClient =  new ServiceBusAdministrationClient(new ServiceBusAdministrationAsyncClient());

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

    public Mono<Void> createQueue(String name) {

        log.info("Creando queue de service bus....");

        try {
            if(!managementClient.queueExists(name))
            {
                managementClient.createQueue(name);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando cola ServiceBusException".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando cola InterruptedException ".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        }
        return Mono.empty();
    }

    public Mono<Void> createTopic(String name) {

        log.info("Creando topic de service bus....");

        try {
            TopicDescription topicDescription = null;
            if(!managementClient.topicExists(name))
            {
                managementClient.createTopic(name);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando tipic ServiceBusException".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando tipic InterruptedException ".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        }
        return Mono.empty();
    }

    public Mono<Void> createSubscription(String topicName, String subscriptionName) {

        log.info("Creando subscription de service bus....");
        try {
            if(!managementClient.subscriptionExists(topicName, subscriptionName))
            {
                managementClient.createSubscription(topicName, subscriptionName);
                managementClient.deleteRule(topicName, subscriptionName, "$Default");
            }
        } catch (ServiceBusException e) {
            log.info("Error creando subscription ServiceBusException".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando subscription InterruptedException ".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        }
        return Mono.empty();
    }

    public Mono<Void> createRulesubscription(String topicName, String subscriptionName, String filterPath) {

        log.info("Creando rule subscription de service bus....");
        try {
            if(!managementClient.ruleExists(topicName, subscriptionName, filterPath))
            {
                CorrelationFilter correlationFilter = new CorrelationFilter();
                correlationFilter.setTo(filterPath);
                managementClient.createRule(topicName, subscriptionName, new RuleDescription(filterPath, correlationFilter));
            }
        } catch (ServiceBusException e) {
            log.info("Error creando subscription ServiceBusException".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando subscription InterruptedException ".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        }
        return Mono.empty();
    }


    public static class TopologyDefException extends RuntimeException {
        public TopologyDefException(Throwable cause) {
            super(cause);
        }
    }
}
