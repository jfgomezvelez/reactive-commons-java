package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.ServiceBusDirectAsyncGateway;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationReplyListener;
import org.reactivecommons.async.servicebus.communucations.ManagementServiceBusClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class DirectAsyncGatewayConfig {

    private final BrokerConfigProps props;
    private final AsyncProps asyncProps;

    @Bean
    public ServiceBusDirectAsyncGateway serviceBusDirectAsyncGateway(BrokerConfig config,
                                                                 ReactiveReplyRouter router,
                                                                 ReactiveMessageSender sender,
                                                                 MessageConverter converter) {
        return new ServiceBusDirectAsyncGateway(config,
                sender,
                router,
                converter,
                props.getDirectMessagesExchangeName(),
                props.getGlobalReplyExchangeName()
        );
    }

    @Bean
    public ApplicationReplyListener msgListener(ReactiveReplyRouter router,
                                                BrokerConfig config,
                                                ReactiveMessageListener listener,
                                                ReactiveMessageListener reactiveMessageListener,
                                                ManagementServiceBusClient serviceBusClientBuilder) {
        final ApplicationReplyListener replyListener = new ApplicationReplyListener(
                router,
                listener,
                props.getGlobalReplyExchangeName(),
                props.getReplyQueue(),
                asyncProps.getGlobal().getMaxDeliveryCount(),
                asyncProps.getGlobal().getMessageLockDuration(),
                asyncProps.getGlobal().getMessageTimeToLive(),
                asyncProps.getGlobal().getAutoDeleteOnIdle(),
                reactiveMessageListener,
                serviceBusClientBuilder
        );
        replyListener.startListening(config.getRoutingKey());
        return replyListener;
    }


    @Bean
    public ReactiveReplyRouter router() {
        return new ReactiveReplyRouter();
    }

    @Bean(destroyMethod = "destroy")
    public ComponentDestroy componentDestroy(ApplicationReplyListener applicationReplyListener) {
        return new ComponentDestroy(applicationReplyListener);
    }
}
