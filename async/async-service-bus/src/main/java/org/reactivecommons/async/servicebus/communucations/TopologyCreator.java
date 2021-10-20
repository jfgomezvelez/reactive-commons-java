package org.reactivecommons.async.servicebus.communucations;

import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.management.SubscriptionDescription;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.rules.CorrelationFilter;
import com.microsoft.azure.servicebus.rules.RuleDescription;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Log
@AllArgsConstructor
public class TopologyCreator {

    private final ManagementClient managementClient;

    public void deleteSubscription(String topicName, String subscriptionName) {
        try {
            managementClient.deleteSubscription(topicName, subscriptionName);
        } catch (ServiceBusException e) {
            log.info("Error creando topic ServiceBusException".concat(e.getMessage()));
        } catch (InterruptedException e) {
            log.info("Error creando topic InterruptedException ".concat(e.getMessage()));
        }
    }

    public Mono<Void> createTopic(String topicName) {

        log.info("Creando topic de service bus....");

        try {
            if (!managementClient.topicExists(topicName)) {
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

    public Mono<Void> createSubscription(String topicName, String subscriptionName,
                                         boolean withDLQRetry,
                                         int maxDeliveryCount,
                                         long messageLockDuration,
                                         long messageTimeToLive,
                                         long autoDeleteOnIdle) {

        log.info("Creando subscription de service bus....");
        try {

            SubscriptionDescription subscriptionDescription = new SubscriptionDescription(topicName, subscriptionName);
            subscriptionDescription.setEnableDeadLetteringOnMessageExpiration(withDLQRetry);
            subscriptionDescription.setMaxDeliveryCount(maxDeliveryCount);
            subscriptionDescription.setLockDuration(Duration.ofSeconds(messageLockDuration));
            subscriptionDescription.setDefaultMessageTimeToLive(Duration.ofDays(messageTimeToLive));
            subscriptionDescription.setAutoDeleteOnIdle(Duration.ofMinutes(autoDeleteOnIdle));

            if (!managementClient.subscriptionExists(topicName, subscriptionName)) {
                managementClient.createSubscription(subscriptionDescription);
                managementClient.deleteRule(topicName, subscriptionName, "$Default");
            } else {
                managementClient.updateSubscription(subscriptionDescription);
            }
        } catch (ServiceBusException e) {
            log.info("Error creando subscription ServiceBusException ".concat(e.getMessage()));
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
            CorrelationFilter correlationFilter = new CorrelationFilter();
            correlationFilter.setTo(filterPath);
            if (!managementClient.ruleExists(topicName, subscriptionName, filterPath)) {
                managementClient.createRule(topicName, subscriptionName, new RuleDescription(filterPath, correlationFilter));
            } else {
                managementClient.updateRule(topicName, subscriptionName, new RuleDescription(filterPath, correlationFilter));
            }
        } catch (ServiceBusException e) {
            log.info("Error creando rule ServiceBusException ".concat(e.getMessage()));
            return Mono.error(new TopologyDefException(e));
        } catch (InterruptedException e) {
            log.info("Error creando rule InterruptedException ".concat(e.getMessage()));
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
