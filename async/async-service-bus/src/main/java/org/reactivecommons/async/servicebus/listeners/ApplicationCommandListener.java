package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.handlers.registered.RegisteredCommandHandler;
import org.reactivecommons.async.commons.CommandExecutor;
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
public class ApplicationCommandListener extends GenericMessageListener {

    private final MessageConverter messageConverter;
    private HandlerResolver resolver;

    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);

    public ApplicationCommandListener(String topicName,
                                      ReactiveMessageListener reactiveMessageListener,
                                      HandlerResolver resolver,
                                      MessageConverter messageConverter,
                                      String subscriptionName) {
        super(topicName, subscriptionName, reactiveMessageListener);
        this.resolver = resolver;
        this.messageConverter = messageConverter;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName))
                .thenMany(Flux.fromIterable(resolver.getCommandHandlers())
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

            final Command<Object> command = messageConverter.readCommandStructure(message);

            final RegisteredCommandHandler<Object> handler = resolver.getCommandHandler(command.getName());

            final Class<Object> eventClass = handler.getInputClass();

            Function<Message, Command<Object>> converter = msj -> messageConverter.readCommand(msj, eventClass);

            final CommandExecutor<Object> executor = new CommandExecutor<>(handler.getHandler(), converter);

            executor.execute(message)
                    .subscribeOn(scheduler);

        } catch (Exception e) {
            log.log(Level.SEVERE, format("ATTENTION !! Outer error protection reached for %s, in Async Consumer!! Severe Warning! ", serviceBusReceivedMessage.getRawAmqpMessage().getProperties().getMessageId()), e);
        }
    }
}
