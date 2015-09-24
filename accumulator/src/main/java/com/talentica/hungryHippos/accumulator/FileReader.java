package com.talentica.hungryHippos.accumulator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader {
    //65536*8
    ByteBuffer buf= ByteBuffer.allocate(65536);
    FileChannel channel;
    int readCount = 0;

    public FileReader(String filename) throws FileNotFoundException {
        channel = new FileInputStream(filename).getChannel();
        buf.clear();
    }

    public String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        while(true){
            if(readCount<=0){
                buf.clear();
                readCount = channel.read(buf);
                if(readCount<0){
                    break;
                }
                buf.flip();
            }
            byte nextChar = buf.get();
            readCount--;

            if(nextChar!='\n') {
               sb.append((char)nextChar);
            }else{
                break;
            }

        }
        return sb.toString();
    }

    public static void main(String [] args) throws Exception{
        long startTime = System.currentTimeMillis();
        FileReader reader = new FileReader("sampledata.txt");
        int num = 0;
        while(true){
            String val = reader.readLine();
            System.out.println(val);
            if(val.equals("")){
                break;
            }
            num++;

        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Take: "+(endTime-startTime));
        System.out.println(num);

    }

}