package com.talentica.hungryHippos.consumers.http;

import com.lmax.disruptor.EventFactory;

/**
 * Created by santoshm1 on 6/9/15.
 */
public class LongEventFactory implements EventFactory<LongEvent> {
    public LongEvent newInstance() {
        return new LongEvent();
    }
}
