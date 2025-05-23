package org.reactivecommons.async.api;

import org.reactivecommons.api.domain.Command;
import reactor.core.publisher.Mono;

public interface CommandAsyncGateway {
    <T> Mono<Void> sendCommand(Command<T> command, String targetName);
}
