package org.reactivecommons.async.servicebus.config.props;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
public class GlobalProps {

    private String exchange = "globalReply";

    private Optional<Integer> maxLengthBytes = Optional.empty();

    private Integer maxDeliveryCount = 10;

    private Integer delayBetweenRetry = 5;

    private Integer messageLockDuration = 60;

    private Long messageTimeToLive = 10675199L;

    private Long autoDeleteOnIdle = 10675199L;

}
