package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.commons.utils.LoggerSubscriber;
import org.reactivecommons.async.servicebus.ServiceBusMessage;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.rabbitmq.AcknowledgableDelivery;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static reactor.core.publisher.Mono.defer;

@Log
public abstract class GenericMessageListener {

    protected final String subscriptionName;
    protected final String topicName;
    private final ReactiveMessageListener reactiveMessageListener;
    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);
    private final ConcurrentHashMap<String, Function<Message, Mono<Object>>> handlers = new ConcurrentHashMap<>();
    private final CustomReporter customReporter;
    private final String objectType;
    private volatile Flux<ServiceBusReceivedMessage> messageFlux;


    public GenericMessageListener(
            String topicName,
            String subscriptionName,
            ReactiveMessageListener reactiveMessageListener,
            CustomReporter customReporter,
            String objectType) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.reactiveMessageListener = reactiveMessageListener;
        this.customReporter = customReporter;
        this.objectType = objectType;
    }

    public void startListener() {
        log.log(Level.INFO, "Using max concurrency {0}, in queue: {1}", new Object[]{reactiveMessageListener.getMaxConcurrency(), subscriptionName});
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
        this.messageFlux = setUpBindings(reactiveMessageListener.getTopologyCreator())
                .thenMany(createListenerAsync(topicName, subscriptionName))
                .transform(this::consumeFaultTolerant);

        onTerminate();
    }

    private void onTerminate() {
        messageFlux.doOnTerminate(this::onTerminate)
                .subscribe(new LoggerSubscriber<>(getClass().getName()));
    }

    private Flux<ServiceBusReceivedMessage> consumeFaultTolerant(Flux<ServiceBusReceivedMessage> messageFlux) {
        return messageFlux.flatMap(msj -> {
            final Instant init = Instant.now();
            return handle(msj, init);
                    //.doOnSuccess(ServiceBusReceivedMessage::)
                    //.onErrorResume(err -> requeueOrAck(msj, err, init));
        }, reactiveMessageListener.getMaxConcurrency());
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return Mono.empty();
    }

    private Mono<Void> createListener(String topicName, String subscriptionName) {

        Listener listener = new Listener(topicName, subscriptionName);

        return listener.start();
    }

    private Flux<ServiceBusReceivedMessage> createListenerAsync(String topicName, String subscriptionName) {

        Listener listener = new Listener(topicName, subscriptionName);

        return listener.startAsync();
    }

    private void receiver(ServiceBusReceivedMessage context) {

        final String executorPath = getExecutorPath(context);

        final Function<Message, Mono<Object>> handler = getExecutor(executorPath);

        final Message message = ServiceBusMessage.fromDelivery(context);

        System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", context.getMessageId(),
                context.getSequenceNumber(), context.getBody());

        final Instant initTime = Instant.now();

        defer(() -> handler.apply(message))
                .transform(enrichPostProcess(message))
                .doOnSuccess(o -> logExecution(executorPath, initTime, true))
                .subscribeOn(scheduler);
    }

    private Mono<ServiceBusReceivedMessage> handle(ServiceBusReceivedMessage context, Instant initTime) {

        try {
            final String executorPath = getExecutorPath(context);

            final Function<Message, Mono<Object>> handler = getExecutor(executorPath);

            final Message message = ServiceBusMessage.fromDelivery(context);

            System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", context.getMessageId(),
                    context.getSequenceNumber(), context.getBody());

            return defer(() -> handler.apply(message))
                    .transform(enrichPostProcess(message))
                    .doOnSuccess(o -> logExecution(executorPath, initTime, true))
                    .subscribeOn(scheduler)
                    .thenReturn(context);

        } catch (Exception e) {
            log.log(Level.SEVERE, format("ATTENTION !! Outer error protection reached for %s, in Async Consumer!! Severe Warning! ", context.getMessageId()));
            return Mono.error(e);
        }
    }

    protected abstract String getExecutorPath(ServiceBusReceivedMessage context);

    protected Function<Mono<Object>, Mono<Object>> enrichPostProcess(Message msg) {
        return identity();
    }

    private Function<Message, Mono<Object>> getExecutor(String path) {
        final Function<Message, Mono<Object>> handler = handlers.get(path);
        return handler != null ? handler : computeRawMessageHandler(path);
    }

    private Function<Message, Mono<Object>> computeRawMessageHandler(String commandId) {
        return handlers.computeIfAbsent(commandId, s ->
                rawMessageHandler(commandId)
        );
    }

    protected abstract Function<Message, Mono<Object>> rawMessageHandler(String executorPath);

    private void logExecution(String executorPath, Instant initTime, boolean success) {
        try {
            final Instant afterExecutionTime = Instant.now();
            final long timeElapsed = Duration.between(initTime, afterExecutionTime).toMillis();
            doLogExecution(executorPath, timeElapsed);
            customReporter.reportMetric(objectType, executorPath, timeElapsed, success);
        } catch (Exception e) {
            log.log(Level.WARNING, "Unable to send execution metrics!", e);
        }
    }

    private void doLogExecution(String executorPath, long timeElapsed) {
        log.log(Level.FINE, String.format("%s with path %s handled, took %d ms",
                objectType, executorPath, timeElapsed));
    }

}
