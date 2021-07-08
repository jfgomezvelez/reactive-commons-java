package org.reactivecommons.async.servicebus.listeners;

import lombok.extern.java.Log;
import org.reactivecommons.async.api.handlers.registered.RegisteredQueryHandler;
import org.reactivecommons.async.commons.QueryExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Log
public class ApplicationQueryListener extends GenericMessageListener {

    private final String topicName;
    private final HandlerResolver resolver;
    private final MessageConverter converter;

    public ApplicationQueryListener(String topicName,
                                    ReactiveMessageListener reactiveMessageListener,
                                    HandlerResolver resolver,
                                    MessageConverter converter,
                                    String subscriptionName) {
        super(subscriptionName, reactiveMessageListener);
        this.topicName = topicName;
        this.resolver = resolver;
        this.converter = converter;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .thenMany(Flux.fromIterable(resolver.getQueryHandlers())
                        .flatMap(listener ->
                                creator.createSubscription(topicName, listener.getPath())
                                        .then(creator.createRulesubscription(topicName, listener.getPath(), listener.getPath())
                                                .then(createListener(topicName, listener.getPath())))
                        )
                )
                .then();
    }

    private Mono<Listener> createListener(String topicName, String subscriptionName) {

        final RegisteredQueryHandler<Object, Object> handler = resolver.getQueryHandler(subscriptionName);

        if (handler == null) {
            return Mono.error(new RuntimeException("Handler Not registered for Query: " + subscriptionName));
        }
        final Class<?> handlerClass = handler.getQueryClass();

        Function<Message, Object> messageConverter = msj -> converter.readAsyncQuery(msj, handlerClass).getQueryData();

        final QueryExecutor<Object, Object> executor = new QueryExecutor<>(handler.getHandler(), messageConverter);

        Listener listener = new QueryListener(topicName, subscriptionName, executor);

        listener.start();

        return Mono.just(listener);
    }
}
