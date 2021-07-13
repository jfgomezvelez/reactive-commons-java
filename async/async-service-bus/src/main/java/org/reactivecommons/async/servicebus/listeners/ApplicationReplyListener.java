package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.ServiceBusMessage;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Mono;

import java.util.logging.Level;

import static org.reactivecommons.async.commons.Headers.COMPLETION_ONLY_SIGNAL;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;
import static reactor.rabbitmq.ResourcesSpecification.*;

@Log
public class ApplicationReplyListener {

    private final ReactiveReplyRouter router;
    private final TopologyCreator creator;
    private final String subscriptionName;
    private final String topicName;

    public ApplicationReplyListener(ReactiveReplyRouter router, ReactiveMessageListener listener, String topicName, String subscriptionName) {
        this.router = router;
        this.subscriptionName = subscriptionName;
        this.creator = listener.getTopologyCreator();
        this.topicName = topicName;
    }

    public void startListening(String routeKey) {

        creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName))
                .then(creator.createRulesubscription(topicName, subscriptionName, routeKey))
                .then(createLister());
    }

    private Mono<Void> createLister() {
        Listener listener = new Listener(topicName, subscriptionName, this::receiver);

        listener.start();

        return Mono.empty();
    }

    private void receiver(ServiceBusReceivedMessageContext serviceBusReceivedMessageContext) {

        try {
            Message message = ServiceBusMessage.fromDelivery(serviceBusReceivedMessageContext.getMessage());

            final String correlationID = message.getProperties().getHeaders().get(CORRELATION_ID).toString();
            final boolean isEmpty = message.getProperties().getHeaders().get(COMPLETION_ONLY_SIGNAL) != null;
            if (isEmpty) {
                router.routeEmpty(correlationID);
            } else {
                router.routeReply(correlationID, message);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error in reply reception", e);
        }
    }

}
