package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.DomainEvent;
import org.reactivecommons.async.api.handlers.registered.RegisteredEventListener;
import org.reactivecommons.async.commons.EventExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.commons.utils.matcher.KeyMatcher;
import org.reactivecommons.async.commons.utils.matcher.Matcher;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Function;


@Log
public class ApplicationEventListener extends GenericMessageListener {

    private final HandlerResolver resolver;
    private final MessageConverter messageConverter;
    private final Matcher keyMatcher;
    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);


    public ApplicationEventListener(String topicName,
                                    ReactiveMessageListener reactiveMessageListener,
                                    HandlerResolver resolver,
                                    MessageConverter messageConverter,
                                    String subscriptionName,
                                    CustomReporter errorReporter,
                                    String connectionString
    ) {
        super(topicName ,subscriptionName, reactiveMessageListener, errorReporter, "event", connectionString);
        this.resolver = resolver;
        this.messageConverter = messageConverter;
        this.keyMatcher = new KeyMatcher();
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        return creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName))
                .thenMany(Flux.fromIterable(resolver.getEventListeners())
                        .flatMap(listener ->
                                creator.createRulesubscription(topicName, subscriptionName, listener.getPath())
                        )
                )
                .then();
    }

    @Override
    protected String getExecutorPath(ServiceBusReceivedMessage context) {
        return context.getTo();
    }

    @Override
    protected Function<Message, Mono<Object>> rawMessageHandler(String executorPath) {

        final String matchedKey = keyMatcher.match(resolver.getToListenEventNames(), executorPath);

        final RegisteredEventListener<Object> handler = getEventListener(matchedKey);

        final Class<Object> eventClass = handler.getInputClass();

        Function<Message, DomainEvent<Object>> converter = msj -> messageConverter.readDomainEvent(msj, eventClass);

        final EventExecutor<Object> executor = new EventExecutor<>(handler.getHandler(), converter);

        return msj -> executor
                .execute(msj)
                .cast(Object.class);
    }

    private RegisteredEventListener<Object> getEventListener(String matchedKey) {
        RegisteredEventListener<Object> eventListener = resolver.getEventListener(matchedKey);

        if (eventListener == null) {
            return resolver.getDynamicEventsHandler(matchedKey);
        }

        return eventListener;
    }

}
