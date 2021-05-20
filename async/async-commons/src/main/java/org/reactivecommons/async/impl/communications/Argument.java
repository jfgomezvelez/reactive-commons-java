package org.reactivecommons.async.impl.communications;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Optional;

@Getter
@Setter
@Builder(toBuilder = true)
public class Argument {

    private Optional<Integer> maxLengthBytes;

    private Optional<String> overflow;

    private Optional<String> queueMode;
}
