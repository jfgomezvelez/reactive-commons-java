package org.reactivecommons.async.servicebus.communucations;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;

public class ManagementServiceBusClient {

    private ServiceBusClientBuilder serviceBusClientBuilder;
    final String connectionString;

    public ManagementServiceBusClient(String connectionString) {
        this.connectionString = connectionString;
    }

    public synchronized ServiceBusClientBuilder getInstante() {
        if (serviceBusClientBuilder != null)
            return serviceBusClientBuilder;
        serviceBusClientBuilder = new ServiceBusClientBuilder()
                .connectionString(connectionString);
        return serviceBusClientBuilder;
    }

    public synchronized void renewConnection(){
        this.serviceBusClientBuilder = null;
    }
}
