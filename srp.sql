CREATE TABLE `srp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `date` varchar(255) DEFAULT NULL,
  `hour` varchar(255) DEFAULT NULL,
  `kwh` varchar(45) DEFAULT NULL,
  `cost` varchar(45) DEFAULT NULL,
  `isotime` varchar(265) DEFAULT NULL,
  `datetime` timestamp NULL DEFAULT NULL,
  `temperature` varchar(255) DEFAULT NULL,
  `humidity` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb3;