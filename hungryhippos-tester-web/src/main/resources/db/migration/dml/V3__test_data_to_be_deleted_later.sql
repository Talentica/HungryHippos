insert into `hungryhippos_tester`.`user` values(1,'Nitin','Kasat','nitin.kasat@talentica.com','password');
insert into `hungryhippos_tester`.`job` values(1,'ABNCSDS7612','NOT_STARTED',now(),null,1);
insert into `hungryhippos_tester`.`job_input` values(1,1,'http://172.32.45.45/test/sampledata.csv',23123123,'STRING-1,STRING-1,STRING-1,STRING-3,STRING-3,STRING-3,DOUBLE-0,DOUBLE-0,STRING-5','0,1,2');
insert into `hungryhippos_tester`.`job_output` values(1,1,'http://172.32.45.99/job/ABNCSDS7612/output.csv',1212);
update `hungryhippos_tester`.`job` set status='IN_PROGRESS' where job_id=1;
insert into `hungryhippos_tester`.`process_instance` values(1,1,1);
insert into `hungryhippos_tester`.`process_instance` values(2,2,1);
insert into `hungryhippos_tester`.`process_instance_detail` values(1,1,0,'172.54.34.29','COMPLETED',STR_TO_DATE('2/15/2016 8:06:26 AM', '%c/%e/%Y %r'),STR_TO_DATE('2/15/2016 9:06:26 AM', '%c/%e/%Y %r'),null);
insert into `hungryhippos_tester`.`process_instance_detail` values(2,2,1,'172.54.34.30','COMPLETED',STR_TO_DATE('2/15/2016 9:06:27 AM', '%c/%e/%Y %r'),null,null);