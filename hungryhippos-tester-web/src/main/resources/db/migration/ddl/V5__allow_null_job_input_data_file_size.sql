CREATE TABLE `hungryhippos_tester`.`role` (
  `role_id` INT NOT NULL AUTO_INCREMENT,
  `role` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`role_id`));


CREATE TABLE `hungryhippos_tester`.`user_role` (
  `user_role_id` INT NOT NULL AUTO_INCREMENT,
  `user_id` INT NOT NULL,
  `role_id` INT NOT NULL,
  PRIMARY KEY (`user_role_id`),
  INDEX `fk_user_role_user_idx` (`user_id` ASC),
  CONSTRAINT `fk_user_role_user`
    FOREIGN KEY (`user_id`)
    REFERENCES `hungryhippos_tester`.`user` (`user_id`),
  CONSTRAINT `fk_user_role_role`
    FOREIGN KEY (`role_id`)
    REFERENCES `hungryhippos_tester`.`role` (`role_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
