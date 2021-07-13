package org.reactivecommons.async.servicebus;

import com.azure.core.amqp.models.AmqpMessageProperties;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import lombok.Data;
import org.reactivecommons.async.commons.communications.Message;

import java.util.HashMap;
import java.util.Map;

@Data
public class ServiceBusMessage implements Message {
    private final byte[] body;
    private final Properties properties;

    @Data
    public static class ServiceBusMessageProperties implements Properties {
        private String contentType;
        private String contentEncoding;
        private long contentLength;
        private Map<String, Object> headers = new HashMap<>();
    }

    public static ServiceBusMessage fromDelivery(ServiceBusReceivedMessage message) {
        return new ServiceBusMessage(message.getBody().toBytes(), createMessageProps(message));
    }

    private static Message.Properties createMessageProps(ServiceBusReceivedMessage message) {
        final ServiceBusMessage.ServiceBusMessageProperties properties = new ServiceBusMessage.ServiceBusMessageProperties();
        properties.setHeaders(message.getApplicationProperties());
        properties.getHeaders().putAll(getHeaders(message.getRawAmqpMessage().getProperties()));
        properties.setContentType(message.getContentType());
        properties.setContentEncoding(message.getRawAmqpMessage().getProperties().getContentEncoding());
        return properties;
    }

    private static Map<String, Object> getHeaders(AmqpMessageProperties amqpMessageProperties) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("messageId", (amqpMessageProperties.getMessageId() == null) ? null : amqpMessageProperties.getMessageId().toString());
        headers.put("to", (amqpMessageProperties.getTo() == null) ? null: amqpMessageProperties.getTo().toString());
        headers.put("subject", amqpMessageProperties.getSubject());
        headers.put("absoluteExpiryTime", amqpMessageProperties.getAbsoluteExpiryTime());
        headers.put("correlationId", (amqpMessageProperties.getCorrelationId() == null) ? null : amqpMessageProperties.getCorrelationId().toString());
        headers.put("creationTime", amqpMessageProperties.getCreationTime());
        headers.put("groupId", amqpMessageProperties.getGroupId());
        headers.put("groupSequence", amqpMessageProperties.getGroupSequence());
        headers.put("replyToGroupId", amqpMessageProperties.getReplyToGroupId());
        headers.put("replyTo", (amqpMessageProperties.getReplyTo() == null) ? null : amqpMessageProperties.getReplyTo().toString());
        return headers;
    }
}
