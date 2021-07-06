package org.reactivecommons.async.servicebus.listeners;

import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

public class ApplicationCommandListener extends GenericMessageListener {

    private final String topicExchange;

    public ApplicationCommandListener(String topicExchange, String subscriptionName, ReactiveMessageListener reactiveMessageListener){
        super(subscriptionName, reactiveMessageListener);
        this.topicExchange = topicExchange;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        creator.createTopic(topicExchange);
        creator.createSubscription(topicExchange, subscriptionName);

        return Mono.just("").then();

//        final Mono<AMQP.Exchange.DeclareOk> declareExchange = creator.declare(ExchangeSpecification.exchange(directExchange).durable(true).type("direct"));
//        if (withDLQRetry) {
//            final Mono<AMQP.Exchange.DeclareOk> declareExchangeDLQ = creator.declare(ExchangeSpecification.exchange(directExchange + ".DLQ").durable(true).type("direct"));
//            final Mono<AMQP.Queue.DeclareOk> declareQueue = creator.declareQueue(queueName, directExchange + ".DLQ", maxLengthBytes);
//            final Mono<AMQP.Queue.DeclareOk> declareDLQ = creator.declareDLQ(queueName, directExchange, retryDelay, maxLengthBytes);
//            final Mono<AMQP.Queue.BindOk> binding = creator.bind(BindingSpecification.binding(directExchange, queueName, queueName));
//            final Mono<AMQP.Queue.BindOk> bindingDLQ = creator.bind(BindingSpecification.binding(directExchange + ".DLQ", queueName, queueName + ".DLQ"));
//            return declareExchange.then(declareExchangeDLQ).then(declareDLQ).then(declareQueue).then(bindingDLQ).then(binding).then();
//        } else {
//            final Mono<AMQP.Queue.DeclareOk> declareQueue = creator.declareQueue(queueName, maxLengthBytes);
//            final Mono<AMQP.Queue.BindOk> binding = creator.bind(BindingSpecification.binding(directExchange, queueName, queueName));
//            return declareExchange.then(declareQueue).then(binding).then();
//        }
    }

}
