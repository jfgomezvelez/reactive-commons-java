package org.reactivecommons.async.servicebus.listeners;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.CommandExecutor;
import org.reactivecommons.async.commons.EventExecutor;
import org.reactivecommons.async.commons.communications.Message;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.logging.Level;

import static java.lang.String.format;

@Log
public class CommandListener extends Listener{

    private final CommandExecutor<Object> executor;

    private final Scheduler scheduler = Schedulers.newParallel(getClass().getSimpleName(), 12);


    public CommandListener(String topicName, String subscriptionName, CommandExecutor<Object> executor) {
        super(topicName, subscriptionName);
        this.executor = executor;
    }

    protected void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage serviceBusReceivedMessage = context.getMessage();

        try {
            System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", serviceBusReceivedMessage.getMessageId(),
                    serviceBusReceivedMessage.getSequenceNumber(), serviceBusReceivedMessage.getBody());
            System.out.println(serviceBusReceivedMessage.getBody().toString());
            final Message message = org.reactivecommons.async.servicebus.ServiceBusMessage.fromDelivery(serviceBusReceivedMessage);
            executor.execute(message)
                    .subscribeOn(scheduler);
        } catch (Exception e) {
            log.log(Level.SEVERE, format("ATTENTION !! Outer error protection reached for %s, in Async Consumer!! Severe Warning! ", serviceBusReceivedMessage.getRawAmqpMessage().getProperties().getMessageId()), e);
        }
    }
}
