package org.reactivecommons.async.servicebus.communucations;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ReactiveMessageListener {

    private final TopologyCreator topologyCreator;
    private final Integer maxConcurrency;
    private final Integer prefetchCount;

    public ReactiveMessageListener(TopologyCreator topologyCreator) {
        this(topologyCreator, 250, 250);
    }
}
