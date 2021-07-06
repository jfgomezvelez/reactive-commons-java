package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.DiscardNotifier;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.config.ServiceBusConfig;
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
    public ApplicationQueryListener queryListener(ReactiveMessageListener listener,
                                                  HandlerResolver resolver,
                                                  MessageConverter converter,
                                                  CustomReporter errorReporter) {
//        final ApplicationQueryListener listener = new ApplicationQueryListener(rlistener,
//                appName + ".query", resolver, sender, asyncProps.getDirect().getExchange(), converter,
//                asyncProps.getGlobal().getExchange(), asyncProps.getWithDLQRetry(), asyncProps.getMaxRetries(),
//                asyncProps.getRetryDelay(),asyncProps.getGlobal().getMaxLengthBytes(),  discardNotifier, errorReporter);
//
//        listener.startListener();

        final ApplicationQueryListener applicationQueryListener = new ApplicationQueryListener(
                asyncProps.getDirect().getExchange(), appName + ".query", listener);

        applicationQueryListener.startListener();

        return applicationQueryListener;
    }
}
