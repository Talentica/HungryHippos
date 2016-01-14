package com.talentica.hungryHippos.consumers.http;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.DataValueClasses;

/**
 * Created by abhinavn on 10/6/15.
 */
public class ChronicleConsumer {

    final String path = "/tmp/direct-instance";
    
    private static final Logger LOGGER=LoggerFactory.getLogger(ChronicleConsumer.class);
    
    static Chronicle chronicle = null;

    {
        try {
            chronicle = ChronicleQueueBuilder.vanilla(path).build();
        } catch (IOException e) {
            LOGGER.error("Error occurred while initializing.",e);;
        }
    }

    static ExcerptTailer tailer ;

    {
        try {
            tailer = chronicle.createTailer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //private static CassandraClient cassandraClient = new CassandraClient("172.19.101.189");

    public static void main(String ... args)
    {
        ChronicleConsumer consumer = new ChronicleConsumer();
        consumer.doWork();
      //  cassandraClient.close();
    }



    private void doWork() {
        while (tailer.nextIndex()) {
            final LongEventInterface j = DataValueClasses.newDirectReference(LongEventInterface.class);
            j.bytes(tailer, 0);
			j.getOwner();
            tailer.finish();
        }
        LOGGER.info("Done with work.");
    }


}
