package org.reactivecommons.async.servicebus.listeners;

import com.azure.core.amqp.models.AmqpAnnotatedMessage;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import lombok.extern.java.Log;
import org.reactivecommons.async.api.handlers.registered.RegisteredQueryHandler;
import org.reactivecommons.async.commons.DiscardNotifier;
import org.reactivecommons.async.commons.QueryExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static org.reactivecommons.async.commons.Headers.*;

@Log
public class ApplicationQueryListener extends GenericMessageListener {

    private final HandlerResolver resolver;
    private final MessageConverter converter;
    private final ReactiveMessageSender reactiveMessageSender;
    private final String replyTopicName;
    private final long autoDeleteOnIdle;

    public ApplicationQueryListener(ReactiveMessageSender reactiveMessageSender,
                                    ReactiveMessageListener reactiveMessageListener,
                                    HandlerResolver resolver,
                                    MessageConverter converter,
                                    String directTopicName,
                                    String replyTopicName,
                                    String subscriptionName,
                                    CustomReporter customReporter,
                                    boolean withDLQRetry,
                                    int maxDeliveryCount,
                                    int delayBetweenRetry,
                                    long messageLockDuration,
                                    long messageTimeToLive,
                                    long autoDeleteOnIdle,
                                    boolean autoACK,
                                    DiscardNotifier discardNotifier,
                                    ServiceBusClientBuilder serviceBusClientBuilder) {
        super(directTopicName, subscriptionName, reactiveMessageListener, customReporter, "query"
                , withDLQRetry, maxDeliveryCount, delayBetweenRetry, messageLockDuration, messageTimeToLive,
                discardNotifier, serviceBusClientBuilder, autoACK);
        this.resolver = resolver;
        this.converter = converter;
        this.reactiveMessageSender = reactiveMessageSender;
        this.replyTopicName = replyTopicName;
        this.autoDeleteOnIdle = autoDeleteOnIdle;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName, withDLQRetry, maxDeliveryCount,
                        messageLockDuration, messageTimeToLive, autoDeleteOnIdle))
                .then(creator.createRulesubscription(topicName, subscriptionName, subscriptionName))
                .then();
    }

    protected Function<Mono<Object>, Mono<Object>> enrichPostProcess(Message message) {

        return m -> m.materialize().flatMap(signal -> {
            if (signal.isOnError()) {
                return Mono.error(ofNullable(signal.getThrowable()).orElseGet(RuntimeException::new));
            }
            if (signal.isOnComplete()) {
                return Mono.empty();
            }

            return reply(message, signal.get());
        });
    }

    private Mono<Void> reply(Message msg, Object object) {

        final String replyID = msg.getProperties().getHeaders().get(REPLY_ID).toString();

        final String correlationID = msg.getProperties().getHeaders().get(CORRELATION_ID).toString();

        final HashMap<String, Object> headers = new HashMap<>();

        headers.put(CORRELATION_ID, correlationID);

        log.info(String.format("[DebPerf][RC][ENVIADO-RESPUESTA-QUERY] [%s] [%s]", replyID, correlationID));

        return reactiveMessageSender.publishAsync(object, replyTopicName, replyID, headers);
    }

    @Override
    protected String getExecutorPath(ServiceBusReceivedMessage context) {
        AmqpAnnotatedMessage message = context.getRawAmqpMessage();
        return message.getApplicationProperties().get(SERVED_QUERY_ID).toString();
    }

    @Override
    protected Function<Message, Mono<Object>> rawMessageHandler(String executorPath) {

        final RegisteredQueryHandler<Object, Object> handler = resolver.getQueryHandler(executorPath);

        if (handler == null) {
            return message -> Mono.error(new RuntimeException("Handler Not registered for Query: " + executorPath));
        }

        final Class<?> handlerClass = handler.getQueryClass();

        Function<Message, Object> messageConverter = msj -> converter.readAsyncQuery(msj, handlerClass).getQueryData();

        final QueryExecutor<Object, Object> executor = new QueryExecutor<>(handler.getHandler(), messageConverter);

        log.info("[DebPerf][RC][PROCESANDO-SOLICITUD-QUERY]");

        return executor::execute;
    }
}
