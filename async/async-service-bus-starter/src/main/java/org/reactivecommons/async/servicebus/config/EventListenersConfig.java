package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationEventListener;
import org.reactivecommons.async.servicebus.listeners.Listener;
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

    @Bean
    public ApplicationEventListener eventListener(HandlerResolver resolver,
                                                  MessageConverter messageConverter,
                                                  ReactiveMessageListener reactiveMessageListener,
                                                  CustomReporter errorReporter) {

        final ApplicationEventListener applicationEventListener = new ApplicationEventListener(asyncProps.getDomain().getEvents().getExchange(),
                appName + ".subsEvents", reactiveMessageListener, resolver, messageConverter);

        applicationEventListener.startListener();

        return applicationEventListener;
    }
}
