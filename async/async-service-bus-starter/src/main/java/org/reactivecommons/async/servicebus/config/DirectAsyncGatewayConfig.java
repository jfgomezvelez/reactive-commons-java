package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.reply.ReactiveReplyRouter;
import org.reactivecommons.async.servicebus.RabbitDirectAsyncGateway;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ServiceBusConfig.class)
@RequiredArgsConstructor
public class DirectAsyncGatewayConfig {

    private final BrokerConfigProps props;

    @Bean
    public RabbitDirectAsyncGateway rabbitDirectAsyncGateway(BrokerConfig config, ReactiveReplyRouter router, ReactiveMessageSender rSender, MessageConverter converter) throws Exception {
        return new RabbitDirectAsyncGateway(rSender);
    }

//    @Bean
//    public ApplicationReplyListener msgListener(ReactiveReplyRouter router, BrokerConfig config, ReactiveMessageListener listener)  {
//        final ApplicationReplyListener replyListener = new ApplicationReplyListener(router, listener, props.getReplyQueue());
//        replyListener.startListening(config.getRoutingKey());
//        return replyListener;
//    }


    @Bean
    public ReactiveReplyRouter router() {
        return new ReactiveReplyRouter();
    }


}
