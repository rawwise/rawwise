package io.github.rawwise.axiom.orchestrator;

import java.util.List;
import java.util.Optional;

public interface PubSubService {
    void publish(String topic, String agentId, byte[] message) throws CoordException;

    SubscriptionHandle subscribe(String topic, String agentId) throws CoordException;

    boolean unsubscribe(String subscriptionId) throws CoordException;

    Optional<SubscriptionHandle> getSubscription(String subscriptionId) throws CoordException;

    List<SubscriptionHandle> listSubscriptions(String topic) throws CoordException;

    List<PubSubMessage> pullMessages(String topic, String subscriptionId, int maxMessages)
            throws CoordException;
}
