package com.sdu.stream.topology.flow.router.help;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Stream Description
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
@Builder
public class StreamDesc implements Serializable {

    // Stream Id
    private String streamId;

    // Stream Flag
    private String flag;

    public boolean interest(String input) {
        return input.startsWith(flag);
    }
}
