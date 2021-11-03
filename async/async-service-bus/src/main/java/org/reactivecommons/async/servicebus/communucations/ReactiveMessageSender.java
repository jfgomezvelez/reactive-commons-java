package org.reactivecommons.async.servicebus.communucations;

import com.azure.core.amqp.AmqpRetryMode;
import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderAsyncClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.converters.MessageConverter;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.reactivecommons.async.commons.Headers.DESTINATION_TOPIC;

@RequiredArgsConstructor
@Log4j2
public class ReactiveMessageSender {

    private final ServiceBusClientBuilder serviceBusClientBuilder;
    private final MessageConverter messageConverter;

    public <T> Mono<Void> publishAsync(T object, String topicName, String subscriptionName) {
        Message message = messageConverter.toMessage(object);

        ServiceBusSenderAsyncClient senderClient = serviceBusClientBuilder
                .retryOptions(createAmqpRetryOptions())
                .sender()
                .topicName(topicName)
                .buildAsyncClient();


        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.getBody());

        serviceBusMessage.setTo(subscriptionName);

        final HashMap<String, Object> headers = new HashMap<>();

        headers.put(DESTINATION_TOPIC, topicName);

        serviceBusMessage.getApplicationProperties().putAll(headers);

        serviceBusMessage.setContentType(message.getProperties().getContentType());

        serviceBusMessage.getRawAmqpMessage().getProperties().setContentEncoding(serviceBusMessage.getContentType());

        return senderClient.sendMessage(serviceBusMessage);
    }

    public  Mono<Void> publishAsync(ServiceBusMessage serviceBusMessage) {

        String destinationTopic = serviceBusMessage.getApplicationProperties().get(DESTINATION_TOPIC).toString();

        ServiceBusSenderAsyncClient senderClient = serviceBusClientBuilder
                .retryOptions(createAmqpRetryOptions())
                .sender()
                .topicName(destinationTopic)
                .buildAsyncClient();

        return senderClient.sendMessage(serviceBusMessage);
    }

    public <T> Mono<Void> publishAsync(T object, String topicName, String ruleName, Map<String, Object> headers) {

        Message message = messageConverter.toMessage(object);

        ServiceBusSenderAsyncClient senderClient = serviceBusClientBuilder
                .retryOptions(createAmqpRetryOptions())
                .sender()
                .topicName(topicName)
                .buildAsyncClient();

        ServiceBusMessage serviceBusMessage = new ServiceBusMessage(message.getBody());

        serviceBusMessage.setTo(ruleName);

        final HashMap<String, Object> headersTopic = new HashMap<>();

        headersTopic.put(DESTINATION_TOPIC, topicName);

        serviceBusMessage.getApplicationProperties().putAll(headersTopic);

        serviceBusMessage.getApplicationProperties().putAll(headers);

        serviceBusMessage.setContentType(message.getProperties().getContentType());

        serviceBusMessage.getRawAmqpMessage().getProperties().setContentEncoding(serviceBusMessage.getContentType());

        log.info(String.format("[DebPerf][RC][ENVIAND0-MENSAJE] [%s] [%s]", ruleName, topicName));

        return senderClient.sendMessage(serviceBusMessage).doOnNext(resultado -> log.info(String.format("[DebPerf][RC][MENSAJE-ENVIADO] [%s] [%s]", ruleName, topicName)));
    }

    private AmqpRetryOptions createAmqpRetryOptions(){
        AmqpRetryOptions amqpRetryOptions = new AmqpRetryOptions();
        amqpRetryOptions.setMaxRetries(86400000);
        amqpRetryOptions.setMaxDelay(Duration.ofHours(24));
        amqpRetryOptions.setTryTimeout(Duration.ofSeconds(5));
        return amqpRetryOptions;
    }
}
