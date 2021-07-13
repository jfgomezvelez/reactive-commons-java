package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
import org.reactivecommons.async.api.handlers.registered.RegisteredQueryHandler;
import org.reactivecommons.async.commons.QueryExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.function.Function;
import java.util.logging.Level;

import static java.lang.String.format;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;
import static org.reactivecommons.async.commons.Headers.REPLY_ID;

@Log
public class ApplicationQueryListener extends GenericMessageListener {

    private final HandlerResolver resolver;
    private final MessageConverter converter;
    private final ReactiveMessageSender reactiveMessageSender;
    private final String replyTopicName;

    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);


    public ApplicationQueryListener(
            ReactiveMessageSender reactiveMessageSender,
            ReactiveMessageListener reactiveMessageListener,
            HandlerResolver resolver,
            MessageConverter converter,
            String directTopicName,
            String replyTopicName,
            String subscriptionName) {
        super(directTopicName, subscriptionName, reactiveMessageListener);
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

    protected void processMessage(ServiceBusReceivedMessageContext context) {

        ServiceBusReceivedMessage serviceBusReceivedMessage = context.getMessage();

        try {
            final Message message = org.reactivecommons.async.servicebus.ServiceBusMessage.fromDelivery(serviceBusReceivedMessage);

            System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", serviceBusReceivedMessage.getMessageId(),
                    serviceBusReceivedMessage.getSequenceNumber(), serviceBusReceivedMessage.getBody());

            final RegisteredQueryHandler<Object, Object> handler = resolver.getQueryHandler(message.getProperties().getHeaders().get("to").toString());

            if (handler == null) {
                throw new RuntimeException("Handler Not registered for Query: " + subscriptionName);
            }
            final Class<?> handlerClass = handler.getQueryClass();

            Function<Message, Object> messageConverter = msj -> converter.readAsyncQuery(msj, handlerClass).getQueryData();

            final QueryExecutor<Object, Object> executor = new QueryExecutor<>(handler.getHandler(), messageConverter);

            executor.execute(message)
                    .materialize()
                    .flatMap(result -> enrichPostProcess(message, result.get()))
                    .subscribeOn(scheduler);
        } catch (Exception e) {
            log.log(Level.SEVERE, format("ATTENTION !! Outer error protection reached for %s, in Async Consumer!! Severe Warning! ", serviceBusReceivedMessage.getRawAmqpMessage().getProperties().getMessageId()), e);
        }
    }

    private Mono<Void> enrichPostProcess(Message msg, Object object) {

        final String replyID = msg.getProperties().getHeaders().get(REPLY_ID).toString();

        final String correlationID = msg.getProperties().getHeaders().get(CORRELATION_ID).toString();

        final HashMap<String, Object> headers = new HashMap<>();

        headers.put(CORRELATION_ID, correlationID);

        return reactiveMessageSender.publish(object, replyTopicName, replyID);
    }
}
