package org.reactivecommons.async.servicebus.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "services-bus")
@Data
public class ServiceBusProperties {
    private String xxx;
}
