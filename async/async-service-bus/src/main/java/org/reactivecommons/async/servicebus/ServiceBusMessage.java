package org.reactivecommons.async.servicebus;

import com.rabbitmq.client.Delivery;
import lombok.Data;
import org.reactivecommons.async.commons.communications.Message;

import java.util.HashMap;
import java.util.Map;

@Data
public class ServiceBusMessage implements Message {
    private final byte[] body;
    private final Properties properties;

    @Data
    public static class ServiceBusMessageProperties implements Properties{
        private String contentType;
        private String contentEncoding;
        private long contentLength;
        private Map<String, Object> headers = new HashMap<>();
    }

    public static ServiceBusMessage fromDelivery(Delivery delivery){
        return new ServiceBusMessage(delivery.getBody(), createMessageProps(delivery));
    }

    private static Message.Properties createMessageProps(Delivery msj) {
        final ServiceBusMessage.ServiceBusMessageProperties properties = new ServiceBusMessage.ServiceBusMessageProperties();
        properties.setHeaders(msj.getProperties().getHeaders());
        properties.setContentType(msj.getProperties().getContentType());
        properties.setContentEncoding(msj.getProperties().getContentEncoding());
        return properties;
    }
}
