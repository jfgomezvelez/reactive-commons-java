package org.reactivecommons.async.servicebus.communucations;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Map;

@RequiredArgsConstructor
@Log4j2
public class ReactiveMessageSender {

    private final ServiceBusClientBuilder.ServiceBusSenderClientBuilder serviceBusSenderClientBuilder;
    private final MessageConverter messageConverter;
    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);


    public <T> Mono<Void> publish(T object, String topicName, String subscriptionName) {
        Message message = messageConverter.toMessage(object);

        ServiceBusSenderAsyncClient senderClient = serviceBusSenderClientBuilder
                .topicName(topicName)
                .buildAsyncClient();

        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.getBody());

        serviceBusMessage.setTo(subscriptionName);

        serviceBusMessage.setContentType(message.getProperties().getContentType());
        serviceBusMessage.getRawAmqpMessage().getProperties().setContentEncoding(serviceBusMessage.getContentType());

        return senderClient.sendMessage(serviceBusMessage);
    }

    public <T> Mono<Void> publish(T object, String topicName, String ruleName,  Map<String, Object> headers) {

        Message message = messageConverter.toMessage(object);

        ServiceBusSenderAsyncClient senderClient = serviceBusSenderClientBuilder
                .topicName(topicName)
                .buildAsyncClient();

        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.getBody());

        serviceBusMessage.setTo(ruleName);

        serviceBusMessage.getApplicationProperties().putAll(headers);

        serviceBusMessage.setContentType(message.getProperties().getContentType());

        serviceBusMessage.getRawAmqpMessage().getProperties().setContentEncoding(serviceBusMessage.getContentType());

        senderClient.sendMessage(serviceBusMessage)
                .publishOn(scheduler)
                .subscribe();

        return  Mono.empty();
    }
}
