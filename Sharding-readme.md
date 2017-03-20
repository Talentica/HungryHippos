Before publishing data to HungryHippos, You have to run the sharding module.
Sharding module uses a sample file and creates sharding table. This Sharding table is 
used while publishing data. There are some configuration related details user have to provide 
before running Sharding module. There are 2 configuration files related to sharding which 
are explained below : 

1) sharding-client-config.xml

Assuming your input file contains lines with just two fields like below.
The fields are created using "," as delimiter.

samsung,78904566<br/>
apple,865478<br/>
nokia,732<br/>

Mobile is the column name given to field1 . i.e; samsung | apple | nokia<br/>
Number is the column name given to field2 . i.e; 7890566 | 865478 ..

A sample sharding-client-config.xml file looks like below :

    <tns:sharding-client-config>
      <tns:input>
        <tns:sample-file-path>/home/sohanc/D_drive/dataSet/testDataForPublish.txt</tns:sample-file-path>
        <tns:distributed-file-path>/sohan/dir/input</tns:distributed-file-path>
        <tns:data-description>
          <tns:column>
            <tns:name>Mobile</tns:name>
            <tns:data-type>STRING</tns:data-type>
            <tns:size>2</tns:size>
          </tns:column>
          <tns:column>
            <tns:name>Number</tns:name>
            <tns:data-type>INT</tns:data-type>
            <tns:size>0</tns:size>
          </tns:column>
        </tns:data-description>
        <tns:data-parser-config>
          <tns:class-name>com.talentica.hungryHippos.client.data.parser.CsvDataParser</tns:class-name>
        </tns:data-parser-config>
      </tns:input>
      <tns:sharding-dimensions>key1</tns:sharding-dimensions>
      <tns:maximum-size-of-single-block-data>200</tns:maximum-size-of-single-block-data>
      <tns:bad-records-file-out>/home/sohanc/bad_rec</tns:bad-records-file-out>
    </tns:sharding-client-config>

Explaination : 

    <tns:sharding-client-config>
      <tns:input>
        <tns:sample-file-path> provide sample file path. </tns:sample-file-path>
        <tns:distributed-file-path> distributed path of input file where HungryHippos will store the file. </tns:distributed-file-path>
        <tns:data-description> Describe all the columns in a record
          <tns:column> 
            <tns:name> name of the column </tns:name>
            <tns:data-type>Data-type of column</tns:data-type>
            <tns:size> max number of characters for String and 0 for other datatypes</tns:size>
          </tns:column>
          Repeat this column element for all columns description.
        </tns:data-description>
        <tns:data-parser-config> By default HungryHippos CsvDataParser provided.
          <tns:class-name>com.talentica.hungryHippos.client.data.parser.CsvDataParser</tns:class-name>
        </tns:data-parser-config>
      </tns:input>
      <tns:sharding-dimensions>comma separeted column names which user has identified as dimensions.</tns:sharding-dimensions>
      <tns:maximum-size-of-single-block-data> Max size of a single record </tns:maximum-size-of-single-block-data>
      <tns:bad-records-file-out>file path for storing records which does not fulfil the data-description given above.</tns:bad-records-file-out>
    </tns:sharding-client-config>


2) sharding-server-config.xml

A sample sharding-server-config.xml file looks like below :

    <tns:sharding-server-config
      xmlns:tns="http://www.talentica.com/hungryhippos/config/sharding"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.talentica.com/hungryhippos/config/sharding sharding-server-config.xsd ">

      <tns:maximum-shard-file-size-in-bytes>200428800</tns:maximum-shard-file-size-in-bytes>
      <tns:maximum-no-of-shard-buckets-size>20000</tns:maximum-no-of-shard-buckets-size>
    </tns:sharding-server-config>

Explaination : 

    <tns:maximum-shard-file-size-in-bytes> Maximum Size of the sharding table files generated.</tns:maximum-shard-file-size-in-bytes>
    <tns:maximum-no-of-shard-buckets-size> Maximum number of buckets user wants to create.</tns:maximum-no-of-shard-buckets-size>
