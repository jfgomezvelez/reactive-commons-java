package org.reactivecommons.async.servicebus.config;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.rabbitmq.client.Connection;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.api.domain.Command;
import org.reactivecommons.api.domain.DomainEvent;
import org.reactivecommons.async.api.AsyncQuery;
import org.reactivecommons.async.api.DefaultCommandHandler;
import org.reactivecommons.async.api.HandlerRegistry;
import org.reactivecommons.async.api.handlers.registered.RegisteredCommandHandler;
import org.reactivecommons.async.api.handlers.registered.RegisteredEventListener;
import org.reactivecommons.async.api.handlers.registered.RegisteredQueryHandler;
import org.reactivecommons.async.commons.communications.Message;
import org.reactivecommons.async.commons.config.BrokerConfig;
import org.reactivecommons.async.commons.converters.MessageConverter;
import org.reactivecommons.async.commons.converters.json.DefaultObjectMapperSupplier;
import org.reactivecommons.async.commons.converters.json.ObjectMapperSupplier;
import org.reactivecommons.async.commons.ext.CustomReporter;
import org.reactivecommons.async.servicebus.HandlerResolver;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageListener;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.reactivecommons.async.servicebus.converters.jso.JacksonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Log
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties({
        ServiceBusProperties.class,
        AsyncProps.class
})
@Import(BrokerConfigProps.class)
public class ServiceBusConfig {

    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public ReactiveMessageSender messageSender(BrokerConfigProps props, ServiceBusClientBuilder.ServiceBusSenderClientBuilder serviceBusSenderClientBuilder) {
        String exchangeName = props.getDomainEventsExchangeName();
        return new ReactiveMessageSender(serviceBusSenderClientBuilder);
    }

    @Bean
    public ServiceBusClientBuilder.ServiceBusSenderClientBuilder getServiceBusSenderClientBuilder() {
        log.info("Creando objeto de ServiceBusClientBuilder...");
        String connectionString = "Endpoint=sb://reactivecommons-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=I/hFzd5nhUehWfxVR1RwE8tITllYXSz62xvOV7OChNI=";
        return new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender();
    }

    @Bean
    public TopologyCreator getTopology(ManagementClient managementClient) {
        log.info("Creando objeto de TopologyCreator...");
        return new TopologyCreator(managementClient);
    }

    @Bean
    public ManagementClient getManagementClient() {
        log.info("Creando objeto de ManagementClient...");
        String connectionString = "Endpoint=sb://reactivecommons-servicebus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=I/hFzd5nhUehWfxVR1RwE8tITllYXSz62xvOV7OChNI=";
        ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(connectionString);
        return new ManagementClient(connectionStringBuilder);
    }

    @Bean
    public ReactiveMessageListener messageListener(/*ConnectionFactoryProvider provider*/ TopologyCreator topologyCreator) {
//        final Mono<Connection> connection =
//                createConnectionMono(provider.getConnectionFactory(), appName, LISTENER_TYPE);
//        final Receiver receiver = RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connection));
//        final Sender sender = RabbitFlux.createSender(new SenderOptions().connectionMono(connection));
//
//        return new ReactiveMessageListener(receiver,
//                new TopologyCreator(sender),
//                asyncProps.getFlux().getMaxConcurrency(),
//                asyncProps.getPrefetchCount());

        return new ReactiveMessageListener(topologyCreator);
    }

    @Bean
    public HandlerResolver resolver(ApplicationContext context, DefaultCommandHandler defaultCommandHandler) {
        final Map<String, HandlerRegistry> registries = context.getBeansOfType(HandlerRegistry.class);

        final ConcurrentMap<String, RegisteredQueryHandler<?, ?>> queryHandlers = registries
                .values().stream()
                .flatMap(r -> r.getHandlers().stream())
                .collect(ConcurrentHashMap::new, (map, handler) -> map.put(handler.getPath(), handler),
                        ConcurrentHashMap::putAll);

        final ConcurrentMap<String, RegisteredEventListener<?>> eventListeners = registries
                .values().stream()
                .flatMap(r -> r.getEventListeners().stream())
                .collect(ConcurrentHashMap::new, (map, handler) -> map.put(handler.getPath(), handler),
                        ConcurrentHashMap::putAll);

        final ConcurrentMap<String, RegisteredEventListener<?>> dynamicEventHandlers = registries
                .values().stream()
                .flatMap(r -> r.getDynamicEventsHandlers().stream())
                .collect(ConcurrentHashMap::new, (map, handler) -> map.put(handler.getPath(), handler),
                        ConcurrentHashMap::putAll);

        final ConcurrentMap<String, RegisteredCommandHandler<?>> commandHandlers = registries
                .values().stream()
                .flatMap(r -> r.getCommandHandlers().stream())
                .collect(ConcurrentHashMap::new, (map, handler) -> map.put(handler.getPath(), handler),
                        ConcurrentHashMap::putAll);

        final ConcurrentMap<String, RegisteredEventListener<?>> eventNotificationListener = registries
                .values()
                .stream()
                .flatMap(r -> r.getEventNotificationListener().stream())
                .collect(ConcurrentHashMap::new, (map, handler) -> map.put(handler.getPath(), handler),
                        ConcurrentHashMap::putAll);

        return new HandlerResolver(queryHandlers, eventListeners, eventNotificationListener,
                dynamicEventHandlers, commandHandlers) {
            @Override
            @SuppressWarnings("unchecked")
            public <T> RegisteredCommandHandler<T> getCommandHandler(String path) {
                final RegisteredCommandHandler<T> handler = super.getCommandHandler(path);
                return handler != null ? handler : new RegisteredCommandHandler<>("", defaultCommandHandler, Object.class);
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultCommandHandler defaultCommandHandler() {
        return message -> Mono.error(new RuntimeException("No Handler Registered"));
    }

    @Bean
    public SenderOptions reactiveCommonsSenderOptions(/*ConnectionFactoryProvider provider, RabbitProperties rabbitProperties*/) {
//        final Mono<Connection> senderConnection = createConnectionMono(provider.getConnectionFactory(), appName, SENDER_TYPE);
//        final ChannelPoolOptions channelPoolOptions = new ChannelPoolOptions();
//        final PropertyMapper map = PropertyMapper.get();
//
//        map.from(rabbitProperties.getCache().getChannel()::getSize).whenNonNull()
//                .to(channelPoolOptions::maxCacheSize);
//
//        final ChannelPool channelPool = ChannelPoolFactory.createChannelPool(
//                senderConnection,
//                channelPoolOptions
//        );
//
//        return new SenderOptions()
//                .channelPool(channelPool)
//                .resourceManagementChannelMono(channelPool.getChannelMono()
//                        .transform(Utils::cache));
        return null;
    }

    @Bean
    @ConditionalOnMissingBean
    public MessageConverter messageConverter(ObjectMapperSupplier objectMapperSupplier) {
        return new JacksonMessageConverter(objectMapperSupplier.get());
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapperSupplier objectMapperSupplier() {
        return new DefaultObjectMapperSupplier();
    }

    @Bean
    @ConditionalOnMissingBean
    public CustomReporter reactiveCommonsCustomErrorReporter() {
        return new CustomReporter() {
            @Override
            public Mono<Void> reportError(Throwable ex, Message rawMessage, Command<?> message, boolean redelivered) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reportError(Throwable ex, Message rawMessage, DomainEvent<?> message, boolean redelivered) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reportError(Throwable ex, Message rawMessage, AsyncQuery<?> message, boolean redelivered) {
                return Mono.empty();
            }
        };
    }

    @Bean
    @ConditionalOnMissingBean
    public BrokerConfig brokerConfig() {
        return new BrokerConfig();
    }
}
