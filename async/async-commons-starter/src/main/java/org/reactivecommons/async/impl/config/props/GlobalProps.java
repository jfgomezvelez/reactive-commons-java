package org.reactivecommons.async.impl.config.props;

import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
public class GlobalProps {

    private String exchange = "globalReply";

    private Optional<Integer> maxLengthBytes = Optional.empty();

    private Optional<String> overflow = Optional.empty();

    private Optional<String> queueMode = Optional.empty();
}
