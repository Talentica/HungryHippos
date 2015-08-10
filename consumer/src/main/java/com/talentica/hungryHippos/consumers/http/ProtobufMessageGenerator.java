package com.talentica.hungryHippos.consumers.http;

import static com.talentica.hungryHippos.msg.Message.MessageDemo;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by debasishc on 15/6/15.
 */
public class ProtobufMessageGenerator {
    public static void main(String [] args) throws IOException {
        MessageDemo messageDemo = MessageDemo.newBuilder().setName("A name").build();

        FileOutputStream fout = new FileOutputStream("tempData");
        messageDemo.writeTo(fout);
        fout.flush();
        fout.close();
    }
}
