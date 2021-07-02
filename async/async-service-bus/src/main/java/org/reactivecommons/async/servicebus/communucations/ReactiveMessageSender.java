package org.reactivecommons.async.servicebus.communucations;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Log4j2
public class ReactiveMessageSender {

    private final ServiceBusClientBuilder.ServiceBusSenderClientBuilder serviceBusSenderClientBuilder;

    public <T> Mono<Void> publish(T message, String targetName) {
        ServiceBusSenderClient senderClient = serviceBusSenderClientBuilder
                .topicName(targetName)
                .buildClient();
        try {
            senderClient.sendMessage(new ServiceBusMessage(objectToJSON(message)));
            return Mono.just(true).then();
        } catch (JsonProcessingException e) {
            return Mono.error(e);
        }
    }

    private <T> String objectToJSON(T message) throws JsonProcessingException {
        ObjectWriter ow = new ObjectMapper().writer();
        String json = ow.writeValueAsString(message);
        return json;
    }
}
