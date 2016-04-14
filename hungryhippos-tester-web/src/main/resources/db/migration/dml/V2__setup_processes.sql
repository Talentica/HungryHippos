insert into `hungryhippos_tester`.`process`(process_id,name,description) values(1,'FILE_DOWNLOAD','Input data file download');
insert into `hungryhippos_tester`.`process`(process_id,name,description) values(2,'SAMPLING','Create sample from data file for sharding process');
insert into `hungryhippos_tester`.`process`(process_id,name,description) values(3,'SHARDING','Run sharding on sampled data');
insert into `hungryhippos_tester`.`process`(process_id,name,description) values(4,'DATA_PUBLISHING','Publish data to all nodes in cluster');
insert into `hungryhippos_tester`.`process`(process_id,name,description) values(5,'JOB_EXECUTION','job execution');
insert into `hungryhippos_tester`.`process`(process_id,name,description) values(6,'OUTPUT_TRANSFER','Transferring output data files for downloading');