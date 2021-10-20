package org.reactivecommons.async.servicebus.config.props;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
public class DirectProps {

    private String exchange = "directMessages";

    private Optional<Integer> maxLengthBytes = Optional.empty();

    private Integer maxDeliveryCount = 10;

    private Integer delayBetweenRetry = 5;

    private Long messageLockDuration = 60L;

    private Long messageTimeToLive = 10675199L;

    private Long autoDeleteOnIdle = 10675199L;

    private Boolean withAutoACKforCommand = false;

    private Boolean withAutoACKforQuery = false;

}
