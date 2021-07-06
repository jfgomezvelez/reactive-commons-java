package org.reactivecommons.async.servicebus.listeners;

import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

public class ApplicationEventListener extends GenericMessageListener {

    private final String eventsExchange;

    public ApplicationEventListener(String eventsExchange, String queueName, ReactiveMessageListener reactiveMessageListener) {
        super(queueName, reactiveMessageListener);
        this.eventsExchange = eventsExchange;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {
        creator.createTopic(eventsExchange);
        creator.createQueue(queueName);

        return Mono.just("").then();
//        final Mono<AMQP.Exchange.DeclareOk> declareExchange = creator.declare(ExchangeSpecification.exchange(eventsExchange).durable(true).type("topic"));
//        final Flux<AMQP.Queue.BindOk> bindings = fromIterable(resolver.getEventListeners()).flatMap(listener -> creator.bind(BindingSpecification.binding(eventsExchange, listener.getPath(), queueName)));
//        if (withDLQRetry) {
//            final String eventsDLQExchangeName = format("%s.%s.DLQ", appName, eventsExchange);
//            final String retryExchangeName = format("%s.%s", appName, eventsExchange);
//            final Mono<AMQP.Exchange.DeclareOk> retryExchange = creator.declare(ExchangeSpecification.exchange(retryExchangeName).durable(true).type("topic"));
//            final Mono<AMQP.Exchange.DeclareOk> declareExchangeDLQ = creator.declare(ExchangeSpecification.exchange(eventsDLQExchangeName).durable(true).type("topic"));
//            final Mono<AMQP.Queue.DeclareOk> declareDLQ = creator.declareDLQ(queueName, retryExchangeName, retryDelay, maxLengthBytes);
//            final Mono<AMQP.Queue.DeclareOk> declareQueue = creator.declareQueue(queueName, eventsDLQExchangeName, maxLengthBytes);
//            final Mono<AMQP.Queue.BindOk> bindingDLQ = creator.bind(BindingSpecification.binding(eventsDLQExchangeName, "#", queueName + ".DLQ"));
//            final Mono<AMQP.Queue.BindOk> retryBinding = creator.bind(BindingSpecification.binding(retryExchangeName, "#", queueName));
//            return declareExchange.then(retryExchange).then(declareExchangeDLQ).then(declareQueue).then(declareDLQ).thenMany(bindings).then(bindingDLQ).then(retryBinding).then();
//        } else {
//            final Mono<AMQP.Queue.DeclareOk> declareQueue = creator.declareQueue(queueName, maxLengthBytes);
//            return declareExchange.then(declareQueue).thenMany(bindings).then();
//        }

    }
}
