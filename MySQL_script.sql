use IOTDB;
CREATE TABLE `ALLInfo` (
  `DataID` int NOT NULL AUTO_INCREMENT,
  `File_no` int DEFAULT NULL,
  `Burst_no` int DEFAULT NULL,
  `SNRs` double DEFAULT NULL,
  `Burst_start_offset` int DEFAULT NULL,
  `Burst_end_offset` int DEFAULT NULL,
  `Burst_duration_microsec` int DEFAULT NULL,
  `CFO` double DEFAULT NULL,
  `receiver_address` varchar(240) DEFAULT NULL,
  `transmitter_address` varchar(240) DEFAULT NULL,
  `mac_frame_type` varchar(240) DEFAULT NULL,
  `format_` varchar(240) DEFAULT NULL,
  `Burst_name` varchar(240) DEFAULT NULL,
  `File_name` varchar(240) DEFAULT NULL,
  `SdrPozisyonID` varchar(240) DEFAULT NULL,
  `CihazPozisyonID` varchar(240) DEFAULT NULL,
  `SdrID` varchar(240) DEFAULT NULL,
  `DateTime` datetime DEFAULT NULL,
  PRIMARY KEY (`DataID`)
) ENGINE=InnoDB AUTO_INCREMENT=5023871 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci