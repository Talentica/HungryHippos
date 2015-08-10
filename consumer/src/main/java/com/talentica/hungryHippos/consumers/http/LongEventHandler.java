package com.talentica.hungryHippos.consumers.http;

import com.lmax.disruptor.EventHandler;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.lang.model.DataValueClasses;

import java.io.*;


/**
 * Created by santoshm1 on 6/9/15.
 */
public class LongEventHandler implements EventHandler<LongEvent>
{

    static final LongEventInterface i = DataValueClasses.newDirectReference(LongEventInterface.class);

    final String path = "/tmp/direct-instance";
    static Chronicle chronicle = null;
    {
        try {
            chronicle = ChronicleQueueBuilder.vanilla(path).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static ExcerptAppender appender;

    {
        try {
            appender = chronicle.createAppender();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch) throws InterruptedException, IOException {

        appender.startExcerpt(i.maxSize());

        i.bytes(appender, 0);
        i.setOwner(event.get());

        appender.position(i.maxSize());
        appender.finish();
    }




}
