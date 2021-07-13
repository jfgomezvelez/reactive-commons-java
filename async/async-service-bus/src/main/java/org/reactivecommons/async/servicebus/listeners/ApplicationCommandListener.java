package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.handlers.registered.RegisteredCommandHandler;
import org.reactivecommons.async.commons.CommandExecutor;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.ServiceBusMessage;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

@Log
public class ApplicationCommandListener extends GenericMessageListener {

    private final MessageConverter messageConverter;
    private HandlerResolver resolver;

    public ApplicationCommandListener(String topicName,
                                      ReactiveMessageListener reactiveMessageListener,
                                      HandlerResolver resolver,
                                      MessageConverter messageConverter,
                                      String subscriptionName,
                                      CustomReporter errorReporter,
                                      String connectionString) {
        super(topicName, subscriptionName, reactiveMessageListener, errorReporter, "command", connectionString);
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

    @Override
    protected String getExecutorPath(ServiceBusReceivedMessage context) {
        final Command<Object> command = messageConverter.readCommandStructure(ServiceBusMessage.fromDelivery(context));
        return command.getName();
    }

    @Override
    protected Function<Message, Mono<Object>> rawMessageHandler(String executorPath) {

        final RegisteredCommandHandler<Object> handler = resolver.getCommandHandler(executorPath);

        final Class<Object> eventClass = handler.getInputClass();

        Function<Message, Command<Object>> converter = msj -> messageConverter.readCommand(msj, eventClass);

        final CommandExecutor<Object> executor = new CommandExecutor<>(handler.getHandler(), converter);

        return msj -> executor.execute(msj).cast(Object.class);
    }
}
