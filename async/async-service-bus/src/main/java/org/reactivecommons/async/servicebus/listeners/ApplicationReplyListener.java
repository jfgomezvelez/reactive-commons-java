package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import lombok.Data;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.commons.utils.LoggerSubscriber;
import org.reactivecommons.async.servicebus.ServiceBusMessage;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.logging.Level;

import static org.reactivecommons.async.commons.Headers.COMPLETION_ONLY_SIGNAL;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;

@Log
@Data
public class ApplicationReplyListener {

    private final ReactiveReplyRouter router;
    private final TopologyCreator creator;
    private final String subscriptionName;
    private final String topicName;
    private final int maxDeliveryCount;
    private final long messageLockDuration;
    private final long messageTimeToLive;
    private final long autoDeleteOnIdle;
    private final ServiceBusClientBuilder serviceBusClientBuilder;
    private volatile Flux<ServiceBusReceivedMessage> deliveryFlux;
    private final ReactiveMessageListener reactiveMessageListener;

    public ApplicationReplyListener(
            ReactiveReplyRouter router,
            ReactiveMessageListener listener,
            String topicName,
            String subscriptionName,
            int maxDeliveryCount,
            long messageLockDuration,
            long messageTimeToLive,
            long autoDeleteOnIdle,
            ReactiveMessageListener reactiveMessageListener,
            ServiceBusClientBuilder serviceBusClientBuilder
    ) {
        this.router = router;
        this.subscriptionName = subscriptionName;
        this.creator = listener.getTopologyCreator();
        this.topicName = topicName;
        this.maxDeliveryCount = maxDeliveryCount;
        this.messageLockDuration = messageLockDuration;
        this.messageTimeToLive = messageTimeToLive;
        this.reactiveMessageListener = reactiveMessageListener;
        this.serviceBusClientBuilder = serviceBusClientBuilder;
        this.autoDeleteOnIdle = autoDeleteOnIdle;
    }

    public void startListening(String routeKey) {

        deliveryFlux = creator.createTopic(topicName)
                .then(creator.createSubscription(topicName, subscriptionName, false, maxDeliveryCount,
                        messageLockDuration, messageTimeToLive, autoDeleteOnIdle))
                .then(creator.createRulesubscription(topicName, subscriptionName, routeKey))
                .thenMany(createLister())
                .flatMap(message -> Mono.just(message).doOnNext(ms -> receiver(ms)), reactiveMessageListener.getMaxConcurrency())
                .onErrorResume(error -> {
                    log.info("Error in dequeue reply ".concat(error.getMessage()));
                    return Mono.empty();
                });
        onTerminate();
    }

    private Flux<ServiceBusReceivedMessage> createLister() {

        Listener listener = new Listener(topicName, subscriptionName, reactiveMessageListener.getPrefetchCount(), serviceBusClientBuilder);

        return listener.startAsync(true);
    }

    private void receiver(ServiceBusReceivedMessage serviceBusReceivedMessage) {

        try {
            Message message = ServiceBusMessage.fromDelivery(serviceBusReceivedMessage);

            final String correlationID = message.getProperties().getHeaders().get(CORRELATION_ID).toString();

            final boolean isEmpty = message.getProperties().getHeaders().get(COMPLETION_ONLY_SIGNAL) != null;

            log.info(String.format("[DebPerf][RC][RECIBIDO-RESPUESTA-QUERY] [%s]", correlationID));
            if (isEmpty) {
                router.routeEmpty(correlationID);
            } else {
                router.routeReply(correlationID, message);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error in reply reception", e);
        }
    }

    private void onTerminate() {
        deliveryFlux.doOnTerminate(this::onTerminate)
                .subscribe(new LoggerSubscriber<>(getClass().getName()));
    }

}
