package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.function.Function;
import java.util.logging.Level;

import static java.lang.String.format;

@Log
public class ApplicationEventListener extends GenericMessageListener {

    private final HandlerResolver resolver;
    private final MessageConverter messageConverter;

    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);


    public ApplicationEventListener(String topicName,
                                    ReactiveMessageListener reactiveMessageListener,
                                    HandlerResolver resolver,
                                    MessageConverter messageConverter,
                                    String subscriptionName
    ) {
        super(topicName ,subscriptionName, reactiveMessageListener);
        this.resolver = resolver;
        this.messageConverter = messageConverter;
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

    protected void processMessage(ServiceBusReceivedMessageContext context) {

        ServiceBusReceivedMessage serviceBusReceivedMessage = context.getMessage();

        try {
            System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", serviceBusReceivedMessage.getMessageId(),
                    serviceBusReceivedMessage.getSequenceNumber(), serviceBusReceivedMessage.getBody());


            final Message message = org.reactivecommons.async.servicebus.ServiceBusMessage.fromDelivery(serviceBusReceivedMessage);

            RegisteredEventListener<Object> handler = getEventListener(message.getProperties().getHeaders().get("to").toString());

            final Class<Object> eventClass = handler.getInputClass();

            Function<Message, DomainEvent<Object>> converter = msj -> messageConverter.readDomainEvent(msj, eventClass);

            final EventExecutor<Object> executor = new EventExecutor<>(handler.getHandler(), converter);

            executor.execute(message)
                    .subscribeOn(scheduler);

        } catch (Exception e) {
            log.log(Level.SEVERE, format("ATTENTION !! Outer error protection reached for %s, in Async Consumer!! Severe Warning! ", serviceBusReceivedMessage.getRawAmqpMessage().getProperties().getMessageId()), e);
        }

    }

    private RegisteredEventListener<Object> getEventListener(String matchedKey) {
        RegisteredEventListener<Object> eventListener = resolver.getEventListener(matchedKey);

        if (eventListener == null) {
            return resolver.getDynamicEventsHandler(matchedKey);
        }

        return eventListener;
    }

}
