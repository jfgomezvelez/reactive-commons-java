package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

import java.util.function.Function;

import static java.util.function.Function.identity;

@Log
public class GenericMessageListener {

    protected final String subscriptionName;
    protected final String topicName;
    private final ReactiveMessageListener reactiveMessageListener;


    public GenericMessageListener(String topicName, String subscriptionName, ReactiveMessageListener reactiveMessageListener) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.reactiveMessageListener = reactiveMessageListener;
    }

    public void startListener() {
        // log.log(Level.INFO, "Using max concurrency {0}, in queue: {1}", new Object[]{messageListener.getMaxConcurrency(), queueName});
//        if (useDLQRetries) {
//            log.log(Level.INFO, "ATTENTION! Using DLQ Strategy for retries with {0} + 1 Max Retries configured!", new Object[]{maxRetries});
//        } else {
//            log.log(Level.INFO, "ATTENTION! Using infinite fast retries as Retry Strategy");
//        }
//
//        ConsumeOptions consumeOptions = new ConsumeOptions();
//        consumeOptions.qos(messageListener.getPrefetchCount());

//        this.messageFlux = setUpBindings(messageListener.getTopologyCreator()).thenMany(
//                receiver.consumeManualAck(queueName, consumeOptions)
//                        .transform(this::consumeFaultTolerant));
//
//        onTerminate();
        setUpBindings(reactiveMessageListener.getTopologyCreator())
                .then(createListener(topicName, subscriptionName))
                .subscribe();
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return Mono.empty();
    }

    private Mono<Void> createListener(String topicName, String subscriptionName) {

        Listener listener = new Listener(topicName, subscriptionName, this::processMessage);

        listener.start();

        return Mono.empty();
    }

    protected void processMessage(ServiceBusReceivedMessageContext context) {

    }
}
