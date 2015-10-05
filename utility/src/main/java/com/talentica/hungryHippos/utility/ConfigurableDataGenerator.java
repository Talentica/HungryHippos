package com.talentica.hungryHippos.utility;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by debasishc on 5/10/15.
 */
public class ConfigurableDataGenerator {


    public final static char [] allChars;
    public final static char[] allNumbers;
    static {
        allChars = new char[26];
        for(int i=0;i<26;i++){
            allChars[i]=(char)('a'+i);
        }

        allNumbers = new char[10];
        for(int i=0;i<10;i++){
            allNumbers[i]=(char)('0'+i);
        }
    }


    private static List<String> generateAllCombinations(int numChars, char[] sourceChars){

        List<String> retList = new ArrayList<>();
        if(numChars<=0){

            retList.add("");
            return retList;
        }
        List<String> listForTheRest = generateAllCombinations(numChars - 1, sourceChars);
        for(char c:sourceChars){
            for(String source:listForTheRest){
                retList.add(c+source);
            }
        }
        return retList;
    }

    private static double skewRandom(){
        double start = Math.random();
        return start*start;
    }


    private static class ColumnConfig{
        public final char[] sourceChars;
        public final int count;
        public final String [] valueSet;

        public ColumnConfig(char[] sourceChars, int count) {
            this.sourceChars = sourceChars;
            this.count = count;
            this.valueSet = generateAllCombinations(count, sourceChars).toArray(new String[0]);
        }
    }

    public static void main(String [] args) throws FileNotFoundException {

        if(args.length<3){
            System.out.println("Usage java com.talentica.hungryHippos.utility.ConfigurableDataGenerator [numberOfRows] [filename] [column_desc]*");
            System.out.println("column_desc := (C|N)':'[number_of_characters");

        }
        long entryCount = Long.parseLong(args[0]);
        String filename = args[1];

        ColumnConfig[] configs = new ColumnConfig[args.length-2];

        for(int i=0;i<args.length-2;i++){
            String [] parts = args[i+2].split(":");
            char[]  sourceChars = null;
            switch (parts[0]){
                case "C":
                    sourceChars=allChars;
                    break;
                case "N":
                    sourceChars=allNumbers;
                    break;
            }
            int count = Integer.parseInt(parts[1]);
            ColumnConfig config = new ColumnConfig(sourceChars,count);
            configs[i]=config;
        }




        PrintWriter out = new PrintWriter(new File(filename));
        long start = System.currentTimeMillis();
        System.out.println(generateAllCombinations(3,allNumbers));
        for(int i=0;i<entryCount;i++){
            for(int j=0;j<configs.length;j++){
                int toSelct = (int)(skewRandom() * configs[j].valueSet.length);

                out.print(configs[j].valueSet[toSelct]);
                if(j<configs.length-1){
                    out.print(",");
                }

            }
            out.println();
        }
        long end = System.currentTimeMillis();
        out.flush();
        out.close();
        System.out.println("Time taken in ms: "+(end-start));

    }
}
