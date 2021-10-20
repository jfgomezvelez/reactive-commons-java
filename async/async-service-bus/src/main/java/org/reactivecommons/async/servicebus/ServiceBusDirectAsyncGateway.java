package org.reactivecommons.async.servicebus;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.AsyncQuery;
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
import static org.reactivecommons.async.commons.Headers.SERVED_QUERY_ID;
import static reactor.core.publisher.Mono.fromCallable;

@Log
@RequiredArgsConstructor
public class ServiceBusDirectAsyncGateway implements DirectAsyncGateway {

    private final BrokerConfig config;
    private final ReactiveMessageSender sender;
    private final ReactiveReplyRouter router;
    private final MessageConverter converter;
    private final String topicName;
    private final String topicGlobalName;


    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName) {
        return sender.publishAsync(command, topicName, targetName);
    }

    @Override
    public <T, R> Mono<R> requestReply(AsyncQuery<T> query, String targetName, Class<R> type) {

        final String correlationID = UUID.randomUUID().toString().replaceAll("-", "");

        final Mono<R> replyHolder = router.register(correlationID)
                .timeout(config.getReplyTimeout())
                .doOnError(TimeoutException.class, e -> {
                    log.warning(String.format("TimeoutException : [%s] [%s]",config.getReplyTimeout(), correlationID));
                    router.deregister(correlationID);
                })
                .flatMap(s -> fromCallable(() -> converter.readValue(s, type)));

        Map<String, Object> headers = new HashMap<>();
        headers.put(REPLY_ID, config.getRoutingKey());
        headers.put(SERVED_QUERY_ID, query.getResource());
        headers.put(CORRELATION_ID, correlationID);

       log.info(String.format("[DebPerf][RC][ENVIADO-QUERY] [%s] [%s] [%s] [%s] [%s]", query.getQueryData(), targetName + ".query", config.getRoutingKey(), correlationID, config.getRoutingKey()));

        return sender.publishAsync(query, topicName, targetName + ".query", headers)
                .then(replyHolder);
    }

    @Override
    public <T> Mono<Void> reply(T response, From from) {

        final HashMap<String, Object> headers = new HashMap<>();
        headers.put(CORRELATION_ID, from.getCorrelationID());

        if (response == null) {
            headers.put(COMPLETION_ONLY_SIGNAL, TRUE.toString());
        }

        return sender.publishAsync(response, topicGlobalName, from.getReplyID(), headers);
    }
}
