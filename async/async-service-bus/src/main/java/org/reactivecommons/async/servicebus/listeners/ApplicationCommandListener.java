package org.reactivecommons.async.servicebus.listeners;

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

import java.util.function.Function;

public class ApplicationCommandListener extends GenericMessageListener {

    private final String topicName;
    private final MessageConverter messageConverter;
    private HandlerResolver resolver;

    public ApplicationCommandListener(String topicName,
                                      ReactiveMessageListener reactiveMessageListener,
                                      HandlerResolver resolver,
                                      MessageConverter messageConverter,
                                      String subscriptionName){
        super(subscriptionName, reactiveMessageListener);
        this.resolver = resolver;
        this.topicName = topicName;
        this.messageConverter = messageConverter;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .thenMany(Flux.fromIterable(resolver.getCommandHandlers())
                        .flatMap(listener ->
                                creator.createSubscription(topicName, listener.getPath())
                                        .then(creator.createRulesubscription(topicName, listener.getPath(), listener.getPath())
                                                .then(createListener(topicName, listener.getPath())))
                        )
                )
                .then();

    }

    private Mono<Listener> createListener(String topicName, String subscriptionName) {

        final RegisteredCommandHandler<Object> handler = resolver.getCommandHandler(subscriptionName);

        final Class<Object> eventClass = handler.getInputClass();

        Function<Message, Command<Object>> converter = msj -> messageConverter.readCommand(msj, eventClass);

        final CommandExecutor<Object> executor = new CommandExecutor<>(handler.getHandler(), converter);

        Listener listener = new CommandListener(topicName, subscriptionName, executor);

        listener.start();

        return Mono.just(listener);
    }

}
