package org.reactivecommons.async.servicebus.communucations;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Log4j2
public class ReactiveMessageSender {

    private final ServiceBusClientBuilder.ServiceBusSenderClientBuilder serviceBusSenderClientBuilder;
    private final MessageConverter messageConverter;


    public <T> Mono<Void> publish(T object, String topicName, String subscriptionName) {
        Message message = messageConverter.toMessage(object);

        ServiceBusSenderClient senderClient = serviceBusSenderClientBuilder
                .topicName(topicName)
                .buildClient();

        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.getBody());
        serviceBusMessage.setTo(subscriptionName);
        senderClient.sendMessage(serviceBusMessage);
        return Mono.just(true).then();
    }
}
