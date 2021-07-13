package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.*;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Log
public class Listener {

    private final String topicName;
    private final String subscriptionName;
    protected final Consumer<ServiceBusReceivedMessageContext> processMessage;

    public Listener(String topicName, String subscriptionName, Consumer<ServiceBusReceivedMessageContext> processMessage) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.processMessage = processMessage;
    }

    public Listener(String topicName, String subscriptionName) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.processMessage = null;
    }

    public Mono<Void> start() {

        CountDownLatch countdownLatch = new CountDownLatch(1);

        String connectionString = "Endpoint=sb://reactivecommons-servicebus-sofka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dqaZiNhGjICV4ZFflQIrWwQ5eCftCMGIwSzqIl+Ib/A=";

        ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .topicName(topicName)
                .subscriptionName(subscriptionName)
                .processMessage(processMessage)
                .processError(context -> processError(context, countdownLatch))
                .buildProcessorClient();

        System.out.printf("Starting the processor topic %s, subscription %s", this.topicName, this.subscriptionName);
        processorClient.start();
        return Mono.empty();
    }

    public Flux<ServiceBusReceivedMessage> startAsync() {
        String connectionString = "Endpoint=sb://reactivecommons-servicebus-sofka.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dqaZiNhGjICV4ZFflQIrWwQ5eCftCMGIwSzqIl+Ib/A=";

        ServiceBusReceiverAsyncClient receiver = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subscriptionName)
                .buildAsyncClient();

        return receiver.receiveMessages();
    }

    private void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
        System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
                context.getFullyQualifiedNamespace(), context.getEntityPath());

        if (!(context.getException() instanceof ServiceBusException)) {
            System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
            return;
        }

        ServiceBusException exception = (ServiceBusException) context.getException();
        ServiceBusFailureReason reason = exception.getReason();

        if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
                || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
                || reason == ServiceBusFailureReason.UNAUTHORIZED) {
            System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
                    reason, exception.getMessage());

            countdownLatch.countDown();
        } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
            System.out.printf("Message lock lost for message: %s%n", context.getException());
        } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
            try {
                // Choosing an arbitrary amount of time to wait until trying again.
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                System.err.println("Unable to sleep for period of time");
            }
        } else {
            System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
                    reason, context.getException());
        }
    }
}
