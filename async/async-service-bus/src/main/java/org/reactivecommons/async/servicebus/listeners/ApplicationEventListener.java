package org.reactivecommons.async.servicebus.listeners;

import org.reactivecommons.api.domain.DomainEvent;
import org.reactivecommons.async.api.handlers.registered.RegisteredEventListener;
import org.reactivecommons.async.commons.EventExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class ApplicationEventListener extends GenericMessageListener {

    private final String topicName;
    private final HandlerResolver resolver;
    private final MessageConverter messageConverter;

    public ApplicationEventListener(String topicName,
                                    ReactiveMessageListener reactiveMessageListener,
                                    HandlerResolver resolver,
                                    MessageConverter messageConverter,
                                    String subscriptionName
    ) {
        super(subscriptionName, reactiveMessageListener);
        this.resolver = resolver;
        this.topicName = topicName;
        this.messageConverter = messageConverter;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return creator.createTopic(topicName)
                .thenMany(Flux.fromIterable(resolver.getEventListeners())
                        .flatMap(listener ->
                                creator.createSubscription(topicName, listener.getPath())
                                        .then(creator.createRulesubscription(topicName, listener.getPath(), listener.getPath())
                                                .then(createListener(topicName, listener.getPath())))
                        )
                )
                .then();
    }

    private Mono<Listener> createListener(String topicName, String subscriptionName) {

        RegisteredEventListener<Object> handler = getEventListener(subscriptionName);

        final Class<Object> eventClass = handler.getInputClass();

        Function<Message, DomainEvent<Object>> converter = msj -> messageConverter.readDomainEvent(msj, eventClass);

        final EventExecutor<Object> executor = new EventExecutor<>(handler.getHandler(), converter);

        Listener listener = new EventListener(topicName, subscriptionName, executor);

        listener.start();

        return Mono.just(listener);
    }

    private RegisteredEventListener<Object> getEventListener(String matchedKey) {
        RegisteredEventListener<Object> eventListener = resolver.getEventListener(matchedKey);

        if (eventListener == null) {
            return resolver.getDynamicEventsHandler(matchedKey);
        }

        return eventListener;
    }

}
