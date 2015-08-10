package com.talentica.hungryHippos.consumers.http;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.model.DataValueClasses;

import java.io.IOException;

/**
 * Created by abhinavn on 10/6/15.
 */
public class ChronicleConsumer {

    final String path = "/tmp/direct-instance";
    static Chronicle chronicle = null;

    {
        try {
            chronicle = ChronicleQueueBuilder.vanilla(path).build();
        } catch (IOException e) {
            e.printStackTrace();
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
        int i=0;
        while (tailer.nextIndex()) {
            i++;
            final LongEventInterface j = DataValueClasses.newDirectReference(LongEventInterface.class);
            j.bytes(tailer, 0);
            final String s = j.getOwner();
//            System.out.println(i);
           System.out.println(s);
            tailer.finish();
        }
        System.out.println("DOne");
    }


}
