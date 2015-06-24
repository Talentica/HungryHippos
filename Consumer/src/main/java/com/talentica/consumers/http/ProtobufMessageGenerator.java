package com.talentica.consumers.http;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.talentica.msg.Message.*;

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
