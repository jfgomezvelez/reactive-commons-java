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

    private final ManagementClient managementClient;

    public Mono<Void> createTopic(String topicName) {

        log.info("Creando topic de service bus....");

        try {
            if(!managementClient.topicExists(topicName))
            {
                managementClient.createTopic(topicName);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando topic ServiceBusException".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando topic InterruptedException ".concat(e.getMessage()));
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
