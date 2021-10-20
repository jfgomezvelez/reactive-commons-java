package org.reactivecommons.async.servicebus.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.DiscardNotifier;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.AzureProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationEventListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Log
@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class EventListenersConfig {

    @Value("${spring.application.name}")
    private String appName;

    private final AsyncProps asyncProps;

    private final AzureProps azureProps;

    @Bean
    public ApplicationEventListener eventListener(HandlerResolver resolver,
                                                  MessageConverter messageConverter,
                                                  ReactiveMessageListener reactiveMessageListener,
                                                  CustomReporter errorReporter,
                                                  DiscardNotifier discardNotifier,
                                                  ServiceBusClientBuilder serviceBusClientBuilder) {

        final ApplicationEventListener applicationEventListener = new ApplicationEventListener(asyncProps.getDomain().getEvents().getExchange(),
                 reactiveMessageListener, resolver, messageConverter, appName + ".subsEvents",
                errorReporter, asyncProps.getWithDLQRetry(),
                asyncProps.getDomain().getEvents().getMaxDeliveryCount(),
                asyncProps.getDomain().getEvents().getDelayBetweenRetry(),
                asyncProps.getDomain().getEvents().getMessageLockDuration(),
                asyncProps.getDomain().getEvents().getMessageTimeToLive(),
                asyncProps.getDomain().getEvents().getAutoDeleteOnIdle(),
                discardNotifier, serviceBusClientBuilder);

        applicationEventListener.startListener();

        return applicationEventListener;
    }
}
