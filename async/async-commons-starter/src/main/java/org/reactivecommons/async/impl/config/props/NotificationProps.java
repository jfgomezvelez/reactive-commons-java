package org.reactivecommons.async.impl.config.props;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.reactivecommons.async.impl.utils.NameGenerator;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

@Getter
@Setter
@RequiredArgsConstructor
public class NotificationProps {

    private final AtomicReference<String> queueName = new AtomicReference<>();

    private final String queueSuffix = "notification";

    private Optional<Integer> maxLengthBytes = Optional.empty();

    private Optional<String> overflow = Optional.empty();

    private Optional<String> queueMode = Optional.empty();

    public String getQueueName(String applicationName) {
        final String name = this.queueName.get();
        if(name == null) return getGeneratedName(applicationName);
        return name;
    }

    private String getGeneratedName(String applicationName) {
        String generatedName = NameGenerator.generateNameFrom(applicationName, queueSuffix);
        return this.queueName
                .compareAndSet(null, generatedName) ?
                generatedName : this.queueName.get();
    }
}
