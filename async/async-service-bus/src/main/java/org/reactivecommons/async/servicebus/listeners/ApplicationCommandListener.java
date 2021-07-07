package org.reactivecommons.async.servicebus.listeners;

import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ApplicationCommandListener extends GenericMessageListener {

    private final String topicName;
    private HandlerResolver resolver;

    public ApplicationCommandListener(String topicName, String subscriptionName, ReactiveMessageListener reactiveMessageListener, HandlerResolver resolver){
        super(subscriptionName, reactiveMessageListener);
        this.resolver = resolver;
        this.topicName = topicName;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        return creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName));

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
