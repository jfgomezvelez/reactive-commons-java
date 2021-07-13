package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.ServiceBusDirectAsyncGateway;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.config.props.AzureProps;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationReplyListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class DirectAsyncGatewayConfig {

    private final BrokerConfigProps props;
    private final AzureProps azureProps;

    @Bean
    public ServiceBusDirectAsyncGateway rabbitDirectAsyncGateway(BrokerConfig config,
                                                                 ReactiveReplyRouter router,
                                                                 ReactiveMessageSender sender,
                                                                 MessageConverter converter) throws Exception {
        return new ServiceBusDirectAsyncGateway(config, sender, router , converter, props.getDirectMessagesExchangeName());
    }

    @Bean
    public ApplicationReplyListener msgListener(ReactiveReplyRouter router, BrokerConfig config, ReactiveMessageListener listener)  {
        final ApplicationReplyListener replyListener = new ApplicationReplyListener(
                router,
                listener,
                props.getGlobalReplyExchangeName(),
                props.getReplyQueue(),
                azureProps.getConnectionString()
        );
        replyListener.startListening(config.getRoutingKey());
        return replyListener;
    }


    @Bean
    public ReactiveReplyRouter router() {
        return new ReactiveReplyRouter();
    }
}
