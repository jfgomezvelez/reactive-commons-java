package org.reactivecommons.async.servicebus.config.props;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
public class GlobalProps {

    private String exchange = "globalReply";

    private Optional<Integer> maxLengthBytes = Optional.empty();

    //private Optional<Integer> idleIntervalAutomaticallyDeleted = Optional.of(5);

}
