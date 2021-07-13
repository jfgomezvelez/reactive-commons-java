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

import java.util.function.Consumer;
import java.util.logging.Level;

import static java.lang.String.format;

@Log
public class CommandListener extends Listener{

    public CommandListener(String topicName, String subscriptionName, Consumer<ServiceBusReceivedMessageContext> processMessage) {
        super(topicName, subscriptionName, processMessage);
    }
}
