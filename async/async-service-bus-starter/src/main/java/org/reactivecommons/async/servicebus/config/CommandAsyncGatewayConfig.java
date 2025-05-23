package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.ServiceBusCommandAsyncGateway;
import org.reactivecommons.async.servicebus.ServiceBusDirectAsyncGateway;
import org.reactivecommons.async.servicebus.communucations.ManagementServiceBusClient;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationReplyListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class CommandAsyncGatewayConfig {

    private final BrokerConfigProps props;

    @Bean
    public ServiceBusCommandAsyncGateway serviceBusDirectAsyncGateway(ReactiveMessageSender sender) {
        return new ServiceBusCommandAsyncGateway(sender, props.getDirectMessagesExchangeName());
    }
}
