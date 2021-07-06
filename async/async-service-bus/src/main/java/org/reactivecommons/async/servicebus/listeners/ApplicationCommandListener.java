package org.reactivecommons.async.servicebus.listeners;


import com.rabbitmq.client.AMQP;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

public class ApplicationCommandListener extends GenericMessageListener {

    private final String directExchange;

    public ApplicationCommandListener(String directExchange, String queueName, ReactiveMessageListener reactiveMessageListener){
        super(queueName, reactiveMessageListener);
        this.directExchange = directExchange;
    }

    protected Mono<Void> setUpBindings(TopologyCreator creator) {

        creator.createTopic(directExchange);
        creator.createQueue(queueName);

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
