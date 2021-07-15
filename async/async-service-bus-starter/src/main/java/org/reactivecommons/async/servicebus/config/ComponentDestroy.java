package org.reactivecommons.async.servicebus.config;

import lombok.AllArgsConstructor;
import org.reactivecommons.async.servicebus.listeners.ApplicationReplyListener;

@AllArgsConstructor
public class ComponentDestroy {

    private final ApplicationReplyListener applicationReplyListene;

    public void destroy() {
        System.out.println("Callback triggered - bean destroy method.");
        System.out.println("Deleting subscription ".concat(applicationReplyListene.getSubscriptionName()));
        applicationReplyListene.getCreator().deleteSubscription(applicationReplyListene.getTopicName(), applicationReplyListene.getSubscriptionName());
    }
}
