package org.reactivecommons.async.servicebus.listeners;


import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.DiscardNotifier;
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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;

import static java.lang.String.format;
import static java.util.function.Function.identity;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;
import static org.reactivecommons.async.commons.Headers.REPLY_ID;
import static reactor.core.publisher.Mono.defer;

@Log
public abstract class GenericMessageListener {

    protected final String subscriptionName;
    protected final String topicName;
    protected final boolean isAutoACK;
    private final ReactiveMessageListener reactiveMessageListener;
    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 100);
    private final ConcurrentHashMap<String, Function<Message, Mono<Object>>> handlers = new ConcurrentHashMap<>();
    private final CustomReporter customReporter;
    private final String objectType;
    private volatile Flux<ServiceBusReceivedMessage> messageFlux;
    protected final boolean withDLQRetry;
    protected final int maxDeliveryCount;
    private final int delayBetweenRetry;
    protected final long messageLockDuration;
    protected final long messageTimeToLive;
    private Listener listener;
    private DiscardNotifier discardNotifier;
    private ServiceBusClientBuilder serviceBusClientBuilder;

    public GenericMessageListener(
            String topicName,
            String subscriptionName,
            ReactiveMessageListener reactiveMessageListener,
            CustomReporter customReporter,
            String objectType,
            boolean withDLQRetry,
            int maxDeliveryCount,
            int delayBetweenRetry,
            long messageLockDuration,
            long messageTimeToLive,
            DiscardNotifier discardNotifier,
            ServiceBusClientBuilder serviceBusClientBuilder,
            boolean isAutoACK) {
        this.topicName = topicName;
        this.subscriptionName = subscriptionName;
        this.reactiveMessageListener = reactiveMessageListener;
        this.customReporter = customReporter;
        this.objectType = objectType;
        this.withDLQRetry = withDLQRetry;
        this.maxDeliveryCount = maxDeliveryCount;
        this.delayBetweenRetry = delayBetweenRetry;
        this.messageLockDuration = messageLockDuration;
        this.discardNotifier = discardNotifier;
        this.messageTimeToLive = messageTimeToLive;
        this.serviceBusClientBuilder = serviceBusClientBuilder;
        this.isAutoACK = isAutoACK;
    }

    public void startListener() {
        log.log(Level.INFO, "Using max concurrency {0}, in queue: {1}", new Object[]{reactiveMessageListener.getMaxConcurrency()/*, queueName*/});
        if (withDLQRetry) {
            log.log(Level.INFO, "ATTENTION! Using DLQ Strategy for retries with {0} + 1 Max Retries configured!"/*, new Object[]{maxRetries}*/);
        } else {
            log.log(Level.INFO, "ATTENTION! Using infinite fast retries as Retry Strategy");
        }

        this.listener = new Listener(topicName, subscriptionName, reactiveMessageListener.getPrefetchCount(), serviceBusClientBuilder);

        this.messageFlux = setUpBindings(reactiveMessageListener.getTopologyCreator())
                .thenMany(listener.startAsync(isAutoACK ? ServiceBusReceiveMode.RECEIVE_AND_DELETE : ServiceBusReceiveMode.PEEK_LOCK ))
                .transform(this::consumeFaultTolerant);

        onTerminate();

    }

    private void onTerminate() {
        messageFlux.doOnTerminate(this::onTerminate)
                .subscribe(new LoggerSubscriber<>(getClass().getName()));
    }

    private Flux<ServiceBusReceivedMessage> consumeFaultTolerant(Flux<ServiceBusReceivedMessage> messageFlux) {

        log.info("Concurrencia actual : " + reactiveMessageListener.getMaxConcurrency());

        return messageFlux
                .flatMap(message -> {
                    final Instant init = Instant.now();
                    return handle(message, init)
                            .flatMap(ms -> {
                                        if (!isAutoACK) {
                                            return listener.getServiceBusReceiverAsyncClient()
                                                    .complete(message)
                                                    .thenReturn(ms);
                                        }
                                        return Mono.just(ms);
                                    }
                            )
                            .onErrorResume(err -> requeueOrDiscard(message, err));
                }, reactiveMessageListener.getMaxConcurrency());
    }

    private Mono<ServiceBusReceivedMessage> requeueOrDiscard(ServiceBusReceivedMessage msj, Throwable err) {

        if (isAutoACK)
            return Mono.just(msj);

        final ServiceBusReceiverAsyncClient serviceBusReceiverAsyncClient = listener.getServiceBusReceiverAsyncClient();
        log.info("Reintento ".concat(String.valueOf(msj.getDeliveryCount())).concat(" Con delayBetweenRetry ".concat(String.valueOf(delayBetweenRetry))));


        return Mono.just(msj)
                .delayElement(Duration.ofSeconds(delayBetweenRetry))
                .flatMap(resul -> {
                    if (msj.getDeliveryCount() < (maxDeliveryCount - 1)) {
                        return serviceBusReceiverAsyncClient.abandon(msj);
                    } else {
                        return serviceBusReceiverAsyncClient.abandon(msj).then(discardNotifier.notifyDiscard(ServiceBusMessage.fromDelivery(msj), err));
                    }
                })
                .onErrorResume(error -> {
                    log.info("Error in requeueOrAck ".concat(error.getMessage()));
                    return Mono.empty();
                })
                .thenReturn(msj);
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return Mono.empty();
    }

    private Mono<ServiceBusReceivedMessage> handle(ServiceBusReceivedMessage context, Instant initTime) {

        try {
            final String executorPath = getExecutorPath(context);

            final Function<Message, Mono<Object>> handler = getExecutor(executorPath);

            final Message message = ServiceBusMessage.fromDelivery(context);

            String replyID = "SIN_".concat(REPLY_ID);
            if (message.getProperties().getHeaders().containsKey(REPLY_ID))
                replyID = message.getProperties().getHeaders().get(REPLY_ID).toString();

            String correlationID = "SIN_".concat(CORRELATION_ID);
            if (message.getProperties().getHeaders().containsKey(REPLY_ID))
                correlationID = message.getProperties().getHeaders().get(CORRELATION_ID).toString();

            log.info(String.format("[DebPerf][RC]MENSAJE-RECIBIDO] [%s] [%s]", replyID, correlationID));

            System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", context.getSessionId(),
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
