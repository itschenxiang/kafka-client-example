package org.example;

import lombok.Data;

@Data
public class DelayMessage {

    private long deliverTs;

    private String msg;

}
