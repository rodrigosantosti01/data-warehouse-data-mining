-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema dwh
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema dwh
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `dwh` DEFAULT CHARACTER SET utf8 ;
USE `dwh` ;

-- -----------------------------------------------------
-- Table `dwh`.`instituicao`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`instituicao` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `id_orgao` CHAR(10) NOT NULL,
  `nome_orgao` VARCHAR(255) NOT NULL,
  `id_orgao_subordinado` CHAR(10) NOT NULL,
  `nome_orgao_subordinado` VARCHAR(255) NOT NULL,
  `id_unidade_gestora` CHAR(10) NOT NULL,
  `nome_unidade_gestora` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_orgao_UNIQUE` (`id_orgao` ASC, `id_orgao_subordinado` ASC, `id_unidade_gestora` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dwh`.`programa`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`programa` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `id_programa` CHAR(10) NOT NULL,
  `nome_programa` VARCHAR(255) NOT NULL,
  `id_acao` CHAR(10) NOT NULL,
  `nome_acao` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_programa_UNIQUE` (`id_programa` ASC, `id_acao` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dwh`.`funcao`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`funcao` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `id_funcao` CHAR(10) NOT NULL,
  `nome_funcao` VARCHAR(255) NOT NULL,
  `id_subfuncao` CHAR(10) NOT NULL,
  `nome_subfuncao` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_funcao_UNIQUE` (`id_funcao` ASC, `id_subfuncao` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dwh`.`natureza_despesa`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`natureza_despesa` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `id_grupo_despesa` CHAR(10) NOT NULL,
  `nome_grupo_despesa` VARCHAR(255) NOT NULL,
  `id_elemento_despesa` CHAR(10) NOT NULL,
  `nome_elemento_despesa` VARCHAR(255) NOT NULL,
  `id_modalidade_despesa` CHAR(10) NOT NULL,
  `nome_modalidade_despesa` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_grupo_despesa_UNIQUE` (`id_grupo_despesa` ASC, `id_elemento_despesa` ASC, `id_modalidade_despesa` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dwh`.`lancamento`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`lancamento` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `data_lancamento` VARCHAR(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX  `data_lancamento_UNIQUE`(`data_lancamento`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `dwh`.`fato_despesa`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dwh`.`fato_despesa` (
  `instituicao_id` INT NOT NULL,
  `programa_id` INT NOT NULL,
  `funcao_id` INT NOT NULL,
  `natureza_despesa_id` INT NOT NULL,
  `periodo_id` INT NOT NULL,
  `valor_liquidado` FLOAT NULL,
  `valor_orcado` FLOAT NULL,
  PRIMARY KEY (`instituicao_id`, `programa_id`, `funcao_id`, `natureza_despesa_id`, `periodo_id`),
  INDEX `fk_fato_despesa_programa1_idx` (`programa_id` ASC),
  INDEX `fk_fato_despesa_funcao1_idx` (`funcao_id` ASC),
  INDEX `fk_fato_despesa_natureza_despesa1_idx` (`natureza_despesa_id` ASC),
  INDEX `fk_fato_despesa_periodo1_idx` (`periodo_id` ASC),
  CONSTRAINT `fk_fato_despesa_instituicao1`
    FOREIGN KEY (`instituicao_id`)
    REFERENCES `dwh`.`instituicao` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fato_despesa_programa1`
    FOREIGN KEY (`programa_id`)
    REFERENCES `dwh`.`programa` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fato_despesa_funcao1`
    FOREIGN KEY (`funcao_id`)
    REFERENCES `dwh`.`funcao` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fato_despesa_natureza_despesa1`
    FOREIGN KEY (`natureza_despesa_id`)
    REFERENCES `dwh`.`natureza_despesa` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fato_despesa_periodo1`
    FOREIGN KEY (`periodo_id`)
    REFERENCES `dwh`.`lancamento` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;