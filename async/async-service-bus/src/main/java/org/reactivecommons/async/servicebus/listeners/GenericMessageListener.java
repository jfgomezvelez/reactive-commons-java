package org.reactivecommons.async.servicebus.listeners;

import lombok.extern.java.Log;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.ConsumeOptions;

import java.util.logging.Level;

@Log
public class GenericMessageListener {

    protected final String queueName;
    private final ReactiveMessageListener reactiveMessageListener;

    public GenericMessageListener(String queueName, ReactiveMessageListener reactiveMessageListener) {
        this.queueName = queueName;
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
        setUpBindings(reactiveMessageListener.getTopologyCreator());
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return Mono.empty();
    }
}
