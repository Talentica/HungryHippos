package com.talentica.hungryHippos.accumulator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
    private int numfields;
    private int maxsize;
    private char[][] buffer;
    public void  setNumFields(int numFields){
        this.numfields = numFields;
        buffer = new char[numFields][];
    }
    public void setMaxsize(int maxsize){
        this.maxsize = maxsize;
        for(int i=0;i<numfields;i++){
            buffer[i] = new char[maxsize];
        }
    }
    public char[][] readCommaSeparated() throws IOException {

        int charIndex = 0;
        int fieldIndex=0;
        while(true){

            if(readCount<=0){
                buf.clear();
                readCount = channel.read(buf);
                if(readCount<0){
                    return null;
                }
                buf.flip();
            }
            byte nextChar = buf.get();
            readCount--;
            charIndex++;
            if(nextChar == ','){
                buffer[fieldIndex][charIndex] = '\0';
                charIndex=0;
                fieldIndex++;
            }else if(nextChar=='\n') {
                break;
            }else{
                buffer[fieldIndex][charIndex] = ((char)nextChar);

            }

        }
        return buffer;
    }

    private static String[] charToString(char[][] input){
        String[] all = new String[input.length];
        for(int i=0;i<input.length;i++){
            all[i] = new String(input[i]);
        }
        return all;
    }

    public static void main(String [] args) throws Exception{
        long startTime = System.currentTimeMillis();
        FileReader reader = new FileReader("sampledata.txt");
        reader.setNumFields(8);
        reader.setMaxsize(25);
        int num = 0;
        while(true){
            char[][] val = reader.readCommaSeparated();

            if(val == null){
                break;
            }
            System.out.println(Arrays.toString(charToString(val)));
            num++;

        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Take: "+(endTime-startTime));
        System.out.println(num);

    }

}