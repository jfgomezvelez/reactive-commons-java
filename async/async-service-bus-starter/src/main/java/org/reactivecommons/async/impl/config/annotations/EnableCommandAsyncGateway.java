package org.reactivecommons.async.impl.config.annotations;

import org.reactivecommons.async.servicebus.config.CommandAsyncGatewayConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
@Import(CommandAsyncGatewayConfig.class)
@Configuration
public @interface EnableCommandAsyncGateway {
}
