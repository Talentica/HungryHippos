CREATE DATABASE IF NOT EXISTS hungryhippos_tester;  

  CREATE TABLE `hungryhippos_tester`.`user` (
  `user_id` INT NOT NULL AUTO_INCREMENT,
  `first_name` VARCHAR(45) NOT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  `email_address` VARCHAR(45) NOT NULL,
  `password` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`user_id`));

CREATE TABLE `hungryhippos_tester`.`job` (
  `job_id` INT NOT NULL AUTO_INCREMENT,
  `job_uuid` VARCHAR(45) NOT NULL,
  `status` VARCHAR(45) NOT NULL,
  `date_time_submitted` DATETIME NOT NULL,
  `date_time_started` DATETIME NULL,
  `date_time_finished` DATETIME NULL,
  `user_id` INT NOT NULL,
  `file_system` VARCHAR(45) NULL,	
   PRIMARY KEY (`job_id`),
  INDEX `user_id_idx` (`user_id` ASC),
  CONSTRAINT `user_id`
    FOREIGN KEY (`user_id`)
    REFERENCES `hungryhippos_tester`.`user` (`user_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
    
CREATE TABLE `hungryhippos_tester`.`process` (
  `process_id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NOT NULL,
  `description` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`process_id`));

 
CREATE TABLE `hungryhippos_tester`.`process_instance` (
  `process_instance_id` INT NOT NULL AUTO_INCREMENT,
  `process_id` INT NOT NULL,
  `job_id` INT NOT NULL,
  PRIMARY KEY (`process_instance_id`),
  INDEX `process_id_idx` (`process_id` ASC),
  INDEX `job_id_idx` (`job_id` ASC),
  CONSTRAINT `job_id`
    FOREIGN KEY (`job_id`)
    REFERENCES `hungryhippos_tester`.`job` (`job_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `process_id`
    FOREIGN KEY (`process_id`)
    REFERENCES `hungryhippos_tester`.`process` (`process_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);

CREATE TABLE `hungryhippos_tester`.`process_instance_detail` (
  `process_instance_detail_id` INT NOT NULL AUTO_INCREMENT,
  `process_instance_id` INT NOT NULL,
  `node_id` INT NULL,
  `node_ip` VARCHAR(45) NOT NULL,
  `status` VARCHAR(45) NOT NULL,
  `execution_start_time` DATETIME NOT NULL,
  `execution_end_time` DATETIME NULL,
  `error_message` VARCHAR(255) NULL,
  PRIMARY KEY (`process_instance_detail_id`),
  INDEX `process_instance_id_idx` (`process_instance_id` ASC),
  CONSTRAINT `process_instance_id`
    FOREIGN KEY (`process_instance_id`)
    REFERENCES `hungryhippos_tester`.`process_instance` (`process_instance_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);


CREATE TABLE `hungryhippos_tester`.`job_input` (
  `job_input_id` INT NOT NULL AUTO_INCREMENT,
  `job_id` INT NOT NULL,
  `data_location` VARCHAR(200) NOT NULL,
  `data_size_in_kbs` DECIMAL(19,2) NOT NULL,
  `data_type_configuration` VARCHAR(1000) NOT NULL,
  `sharding_dimensions` VARCHAR(300) NOT NULL,
  PRIMARY KEY (`job_input_id`),
  INDEX `job_id_idx` (`job_id` ASC),
  CONSTRAINT `job_id_fk_job_input`
    FOREIGN KEY (`job_id`)
    REFERENCES `hungryhippos_tester`.`job` (`job_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);

CREATE TABLE `hungryhippos_tester`.`job_output` (
  `job_output_id` INT NOT NULL AUTO_INCREMENT,
  `job_id` INT NOT NULL,
  `data_location` VARCHAR(150) NOT NULL,
  `data_size_in_kbs` DECIMAL(19,2) NOT NULL,
  PRIMARY KEY (`job_output_id`),
  INDEX `job_id_job_output_fk_idx` (`job_id` ASC),
  CONSTRAINT `job_id_job_output_fk`
    FOREIGN KEY (`job_id`)
    REFERENCES `hungryhippos_tester`.`job` (`job_id`)
    ON DELETE NO ACTION
ON UPDATE NO ACTION);
