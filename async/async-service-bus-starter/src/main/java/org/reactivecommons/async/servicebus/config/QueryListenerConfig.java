package org.reactivecommons.async.servicebus.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.DiscardNotifier;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationQueryListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class QueryListenerConfig {


    @Value("${spring.application.name}")
    private String appName;

    private final AsyncProps asyncProps;

    @Bean
    public ApplicationQueryListener queryListener(
            ReactiveMessageSender reactiveMessageSender,
            ReactiveMessageListener listener,
            HandlerResolver resolver,
            MessageConverter converter,
            CustomReporter errorReporter,
            DiscardNotifier discardNotifier,
            ServiceBusClientBuilder serviceBusClientBuilder) {

        final ApplicationQueryListener applicationQueryListener = new ApplicationQueryListener(
                reactiveMessageSender,
                listener,
                resolver,
                converter,
                asyncProps.getDirect().getExchange(),
                asyncProps.getGlobal().getExchange(),
                appName + ".query",
                errorReporter,
                asyncProps.getWithDLQRetry(),
                asyncProps.getDirect().getMaxDeliveryCount(),
                asyncProps.getDirect().getDelayBetweenRetry(),
                asyncProps.getDirect().getMessageLockDuration(),
                asyncProps.getDirect().getMessageTimeToLive(),
                asyncProps.getDirect().getAutoDeleteOnIdle(),
                asyncProps.getDirect().getWithAutoACKforQuery(),
                discardNotifier, serviceBusClientBuilder);

        applicationQueryListener.startListener();

        return applicationQueryListener;
    }
}
