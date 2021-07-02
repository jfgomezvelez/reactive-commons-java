package org.reactivecommons.async.servicebus;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.api.domain.DomainEvent;
import org.reactivecommons.api.domain.DomainEventBus;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class ServiceBusDomainEventBus implements DomainEventBus {

    private final ReactiveMessageSender sender;
    private final String topicName;

    @Override
    public <T> Mono<Void> emit(DomainEvent<T> event) {
        return sender.publish(event, topicName)
                .onErrorMap(err -> new RuntimeException("Event send failure: " + event.getName(), err));
    }
}
