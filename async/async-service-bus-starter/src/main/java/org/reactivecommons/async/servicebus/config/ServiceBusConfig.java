package org.reactivecommons.async.servicebus.config;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.*;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import com.microsoft.azure.servicebus.management.ManagementClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivecommons.async.servicebus.communucations.ReactiveMessageSender;
import org.reactivecommons.async.servicebus.communucations.TopologyCreator;
import org.reactivecommons.async.servicebus.config.props.AsyncProps;
import org.reactivecommons.async.servicebus.config.props.BrokerConfigProps;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.logging.Level;

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
        final ReactiveMessageSender sender = new ReactiveMessageSender(serviceBusSenderClientBuilder);
        return sender;
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

//    @Bean
//    public AzureResourceManager getAzureResourceManager() {
//        log.info("Creando objeto de AzureResourceManager...");
//
//        AzureProfile profile = new AzureProfile(AzureEnvironment.AZURE);
//
////        TokenCredential credential = new DefaultAzureCredentialBuilder()
////                .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
////                .build();
//
//        ClientSecretCredential credential = null;
//        try {
//             credential = new ClientSecretCredentialBuilder()
//                    //.clientId("5d1fcf7d-2346-4fc4-8f40-938ecea55a21")
//                    //.clientSecret("ebb67ff8-5dec-4dae-9f7e-b0950ba6fd31")
//                    //.tenantId("cb62475d-4351-4bc0-b27c-30e4e689fd46")
//                    // authority host is optional
//                    //.authorityHost("<AZURE_AUTHORITY_HOST>")
//                    .build();
//        }catch (Exception exc){
//            log.log(Level.SEVERE, exc.getMessage());
//        }
//
//        log.info("Creando objeto de ClientSecretCredential...");
//
//
////        AzureProfile profile = new AzureProfile("604c67e9-2c6b-4420-8946-784c5a6a43f9", "27356ef5-4f69-4272-b4db-1004e397e7f2", AzureEnvironment.AZURE);
////        EnvironmentCredential credential = new EnvironmentCredentialBuilder()
////                .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
////                .build();
//
//        AzureResourceManager azure = AzureResourceManager
//                .authenticate(credential, profile)
//                .withDefaultSubscription();
//
//        log.info("Creando objeto de AzureResourceManager...");
//
//
//        return azure;
//    }
}
