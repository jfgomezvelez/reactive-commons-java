package org.reactivecommons.async.servicebus.config;

import lombok.RequiredArgsConstructor;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.AzureProps;
import org.reactivecommons.async.servicebus.listeners.ApplicationCommandListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@RequiredArgsConstructor
@Import(ServiceBusConfig.class)
public class CommandListenersConfig {

    @Value("${spring.application.name}")
    private String appName;

    private final AsyncProps asyncProps;

    private final AzureProps azureProps;

    @Bean
    public ApplicationCommandListener applicationCommandListener(ReactiveMessageListener listener,
                                                                 HandlerResolver resolver,
                                                                 MessageConverter converter,
                                                                 CustomReporter errorReporter) {
        ApplicationCommandListener applicationCommandListener = new ApplicationCommandListener(
                asyncProps.getDirect().getExchange(),
                listener,
                resolver,
                converter,
                appName,
                errorReporter,
                azureProps.getConnectionString()
        );

        applicationCommandListener.startListener();

        return applicationCommandListener;
    }
}