package org.reactivecommons.async.servicebus;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.AsyncQuery;
import org.reactivecommons.async.api.CommandAsyncGateway;
import org.reactivecommons.async.api.DirectAsyncGateway;
import org.reactivecommons.async.api.From;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static java.lang.Boolean.TRUE;
import static org.reactivecommons.async.commons.Headers.*;
import static reactor.core.publisher.Mono.fromCallable;

@Log
@RequiredArgsConstructor
public class ServiceBusCommandAsyncGateway implements CommandAsyncGateway {

    private final ReactiveMessageSender sender;
    private final String topicName;


    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName) {
        return sender.publishAsync(command, topicName, targetName);
    }
}
