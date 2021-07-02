package org.reactivecommons.async.servicebus.config.props;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
public class DirectProps {

    private String exchange = "directMessages";

    private Optional<Integer> maxLengthBytes = Optional.empty();

}
