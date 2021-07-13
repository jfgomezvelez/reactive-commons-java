package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import lombok.extern.java.Log;
import org.reactivecommons.async.api.handlers.registered.RegisteredQueryHandler;
import org.reactivecommons.async.commons.QueryExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.function.Function;

import static java.util.Optional.ofNullable;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;
import static org.reactivecommons.async.commons.Headers.REPLY_ID;

@Log
public class ApplicationQueryListener extends GenericMessageListener {

    private final HandlerResolver resolver;
    private final MessageConverter converter;
    private final ReactiveMessageSender reactiveMessageSender;
    private final String replyTopicName;

    public ApplicationQueryListener(
            ReactiveMessageSender reactiveMessageSender,
            ReactiveMessageListener reactiveMessageListener,
            HandlerResolver resolver,
            MessageConverter converter,
            String directTopicName,
            String replyTopicName,
            String subscriptionName,
            CustomReporter customReporter,
            String connectionString) {
        super(directTopicName, subscriptionName, reactiveMessageListener, customReporter, "query", connectionString);
        this.resolver = resolver;
        this.converter = converter;
        this.reactiveMessageSender = reactiveMessageSender;
        this.replyTopicName = replyTopicName;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName))
                .thenMany(Flux.fromIterable(resolver.getQueryHandlers())
                        .flatMap(listener ->
                                creator.createRulesubscription(topicName, subscriptionName, listener.getPath())
                        )
                )
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

    @Override
    protected String getExecutorPath(ServiceBusReceivedMessage context) {
        return context.getTo();
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

        return executor::execute;
    }

    private Mono<Void> reply(Message msg, Object object) {

        final String replyID = msg.getProperties().getHeaders().get(REPLY_ID).toString();

        final String correlationID = msg.getProperties().getHeaders().get(CORRELATION_ID).toString();

        final HashMap<String, Object> headers = new HashMap<>();

        headers.put(CORRELATION_ID, correlationID);

        return reactiveMessageSender.publish(object, replyTopicName, replyID, headers);
    }
}
