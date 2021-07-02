package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.DomainEventBus;
import org.reactivecommons.async.servicebus.ServiceBusDomainEventBus;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;

@Log
@Configuration
@Import(ServiceBusConfig.class)
@RequiredArgsConstructor
public class EventBusConfig {

    private final BrokerConfigProps props;
    private final TopologyCreator topology;

    @PostConstruct
    public void createTopology() {
        topology.createTopic(props.getDomainEventsExchangeName()).subscribe();
    }

    @Bean
    public DomainEventBus domainEventBus(ReactiveMessageSender sender) {
        return new ServiceBusDomainEventBus(sender, props.getDomainEventsExchangeName());
    }
}
