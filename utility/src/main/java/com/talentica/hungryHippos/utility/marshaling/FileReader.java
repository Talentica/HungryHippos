package com.talentica.hungryHippos.utility.marshaling;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.talentica.hungryHippos.utility.PathUtil;


/**
 * Created by debasishc on 22/6/15.
 */
public class FileReader implements Reader {
    //65536*8
    ByteBuffer buf= ByteBuffer.allocate(65536);
    FileChannel channel;
	int readCount = -1;

    @SuppressWarnings("resource")
	public FileReader(String filename) throws IOException {
        channel = new FileInputStream(new File(PathUtil.CURRENT_DIRECTORY).getCanonicalPath()+PathUtil.FORWARD_SLASH+filename).getChannel();
        buf.clear();
    }

	@SuppressWarnings("resource")
	public FileReader(File file) throws IOException {
		channel = new FileInputStream(file).getChannel();
		buf.clear();
	}

    /* (non-Javadoc)
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#readLine()
	 */
    @Override
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
    private MutableCharArrayString[] buffer;
    /* (non-Javadoc)
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#setNumFields(int)
	 */
    @Override
	public void  setNumFields(int numFields){
        this.numfields = numFields;
        buffer = new MutableCharArrayString[numFields];
    }
    /* (non-Javadoc)
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#setMaxsize(int)
	 */
    @Override
	public void setMaxsize(int maxsize){
        for(int i=0;i<numfields;i++){
            buffer[i] = new MutableCharArrayString(maxsize);
        }
    }
    /* (non-Javadoc)
	 * @see com.talentica.hungryHippos.utility.marshaling.Reader#readCommaSeparated()
	 */
    @Override
	public MutableCharArrayString[] read() throws IOException {
        for(MutableCharArrayString s:buffer){
            s.reset();
        }
        int fieldIndex=0;
        while(true){
            if(readCount<=0){
                buf.clear();
				int reducedReadCount = readCount;
                readCount = channel.read(buf);
				if (reducedReadCount == 0 && fieldIndex == numfields) {
					return buffer;
				} else if (readCount < 0) {
					return null;
                }
                buf.flip();
            }
            byte nextChar = buf.get();
            readCount--;
            if(nextChar == ','){
                fieldIndex++;
            }else if(nextChar=='\n') {
				// Ignore blank lines with no data.
				if (fieldIndex == 0) {
					return read();
				} else {
					break;
				}
            }else{
                buffer[fieldIndex].addCharacter((char)nextChar);
            }
        }
        return buffer;
    }

    public static void main(String [] args) throws Exception{
        long startTime = System.currentTimeMillis();
        Reader reader = new FileReader("sampledata.txt");
        reader.setNumFields(8);
        reader.setMaxsize(25);
        int num = 0;
        while(true){
            MutableCharArrayString[] val = reader.read();

            if(val == null){
				reader.close();
                break;
            }

            //System.out.println(Arrays.toString(val));
            num++;

        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Take: "+(endTime-startTime));
        System.out.println(num);

    }

	@Override
	public void close() throws IOException {
		if (channel != null && channel.isOpen()) {
			channel.close();
		}
	}

}