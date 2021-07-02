package org.reactivecommons.async.servicebus;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.async.api.AsyncQuery;
import org.reactivecommons.async.api.DirectAsyncGateway;
import org.reactivecommons.async.api.From;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import reactor.core.publisher.Mono;

import java.util.HashMap;

import static java.lang.Boolean.TRUE;
import static org.reactivecommons.async.commons.Headers.COMPLETION_ONLY_SIGNAL;
import static org.reactivecommons.async.commons.Headers.CORRELATION_ID;

@RequiredArgsConstructor
public class RabbitDirectAsyncGateway implements DirectAsyncGateway {

    private final ReactiveMessageSender sender;

    @Override
    public <T> Mono<Void> sendCommand(Command<T> command, String targetName) {
        return null;
    }

    @Override
    public <T, R> Mono<R> requestReply(AsyncQuery<T> query, String targetName, Class<R> type) {
        return null;
    }

    @Override
    public <T> Mono<Void> reply(T response, From from) {

        final HashMap<String, Object> headers = new HashMap<>();
        headers.put(CORRELATION_ID, from.getCorrelationID());

        if (response == null) {
            headers.put(COMPLETION_ONLY_SIGNAL, TRUE.toString());
        }

        return null;
    }
}
