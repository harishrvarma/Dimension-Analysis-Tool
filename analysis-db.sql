-- phpMyAdmin SQL Dump
-- version 5.1.3
-- https://www.phpmyadmin.net/
--
-- Host: mysql8.0volume:3306
-- Generation Time: Mar 13, 2026 at 01:34 PM
-- Server version: 8.0.27
-- PHP Version: 8.0.15

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `analysis_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `alembic_version`
--

CREATE TABLE `alembic_version` (
  `version_num` varchar(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dimension_product`
--

CREATE TABLE `dimension_product` (
  `product_id` int NOT NULL,
  `group_id` int NOT NULL,
  `system_product_id` varchar(100) DEFAULT NULL,
  `brand` varchar(255) DEFAULT NULL,
  `brand_id` int DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `category_id` int DEFAULT NULL,
  `product_type` varchar(255) DEFAULT NULL,
  `qb_code` varchar(100) DEFAULT NULL,
  `name` varchar(500) DEFAULT NULL,
  `ori_height` float DEFAULT NULL,
  `ori_width` float DEFAULT NULL,
  `ori_depth` float DEFAULT NULL,
  `height` float DEFAULT NULL,
  `width` float DEFAULT NULL,
  `depth` float DEFAULT NULL,
  `weight` float DEFAULT NULL,
  `base_image_url` varchar(1000) DEFAULT NULL,
  `product_url` varchar(1000) DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `dbs_status` tinyint DEFAULT NULL,
  `iqr_status` tinyint DEFAULT NULL,
  `iqr_height_status` tinyint DEFAULT NULL,
  `iqr_width_status` tinyint DEFAULT NULL,
  `iqr_depth_status` tinyint DEFAULT NULL,
  `final_status` tinyint DEFAULT NULL,
  `skip_status` tinyint DEFAULT NULL,
  `skip_status_updated_date` datetime DEFAULT NULL,
  `analyzed_date` datetime DEFAULT NULL,
  `dimension_status` varchar(50) DEFAULT NULL,
  `dimension_failed` varchar(50) DEFAULT NULL,
  `iteration_closed` int DEFAULT NULL,
  `outlier_mode` tinyint DEFAULT NULL COMMENT '0=Autometic, 1=Manually',
  `eps` decimal(10,2) DEFAULT NULL,
  `sample` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dimension_product_group`
--

CREATE TABLE `dimension_product_group` (
  `group_id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  `product_count` int DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `default_selected` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Yes=1, No=0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dimension_product_iteration`
--

CREATE TABLE `dimension_product_iteration` (
  `iteration_id` int NOT NULL,
  `product_group_id` int NOT NULL,
  `algorithm` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `brand` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `product_type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `eps` decimal(10,2) DEFAULT NULL,
  `sample` int DEFAULT NULL,
  `total_items` int DEFAULT NULL,
  `analyzed_items` int DEFAULT NULL,
  `pending_items` int DEFAULT NULL,
  `outlier_items` int DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `unique_number` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `dimension_product_iteration_item`
--

CREATE TABLE `dimension_product_iteration_item` (
  `id` int NOT NULL,
  `system_product_id` varchar(100) NOT NULL,
  `brand` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `cluster` varchar(50) DEFAULT NULL,
  `cluster_items` int DEFAULT NULL,
  `cluster_items_per` decimal(10,2) DEFAULT NULL,
  `outlier_mode` tinyint DEFAULT NULL COMMENT '0=Auto, 1=Manual',
  `status` tinyint DEFAULT NULL COMMENT '0=Outlier, 1=Normal',
  `final_status` tinyint DEFAULT NULL,
  `iteration_id` int NOT NULL,
  `product_type` varchar(255) DEFAULT NULL,
  `analyzed_date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_attribute`
--

CREATE TABLE `matching_attribute` (
  `attribute_id` int NOT NULL,
  `attribute_name` varchar(100) NOT NULL,
  `default_weightage` decimal(6,3) NOT NULL,
  `attribute_type` enum('default','price','status') NOT NULL,
  `competitor_attribute` varchar(25) NOT NULL,
  `data_type` varchar(25) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_competitor_product`
--

CREATE TABLE `matching_competitor_product` (
  `competitor_product_id` int NOT NULL,
  `system_product_id` int NOT NULL,
  `competitor_id` int NOT NULL,
  `sku` varchar(150) NOT NULL,
  `part_number` varchar(150) DEFAULT NULL,
  `price` float NOT NULL,
  `url` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_configuration_group`
--

CREATE TABLE `matching_configuration_group` (
  `configuration_id` int NOT NULL,
  `attribute_id` int NOT NULL,
  `attribute_value` varchar(255) NOT NULL,
  `algorithm_id` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_scores`
--

CREATE TABLE `matching_scores` (
  `score_id` bigint NOT NULL,
  `system_product_id` int NOT NULL,
  `competitor_product_id` int NOT NULL,
  `total_score` decimal(8,3) DEFAULT NULL,
  `score_status` varchar(30) DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `algorithm_id` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_score_attributes`
--

CREATE TABLE `matching_score_attributes` (
  `score_attribute_id` bigint NOT NULL,
  `score_id` bigint NOT NULL,
  `attribute_id` int NOT NULL,
  `score` decimal(8,3) NOT NULL,
  `system_product_id` int NOT NULL,
  `competitor_product_id` int NOT NULL,
  `algorithm_id` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `matching_system_product`
--

CREATE TABLE `matching_system_product` (
  `product_id` int NOT NULL,
  `system_product_id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  `sku` varchar(150) NOT NULL,
  `part_number` varchar(150) DEFAULT NULL,
  `price` float NOT NULL,
  `url` text,
  `competitor_product_id` int DEFAULT NULL,
  `matched_date` datetime DEFAULT NULL,
  `review_status` int NOT NULL,
  `category` varchar(255) NOT NULL,
  `brand` varchar(255) NOT NULL,
  `web_id` varchar(25) NOT NULL,
  `product_type` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `alembic_version`
--
ALTER TABLE `alembic_version`
  ADD PRIMARY KEY (`version_num`);

--
-- Indexes for table `dimension_product`
--
ALTER TABLE `dimension_product`
  ADD PRIMARY KEY (`product_id`),
  ADD KEY `group_id` (`group_id`),
  ADD KEY `idx_product_system_product_id` (`system_product_id`);

--
-- Indexes for table `dimension_product_group`
--
ALTER TABLE `dimension_product_group`
  ADD PRIMARY KEY (`group_id`);

--
-- Indexes for table `dimension_product_iteration`
--
ALTER TABLE `dimension_product_iteration`
  ADD PRIMARY KEY (`iteration_id`),
  ADD KEY `product_group_id` (`product_group_id`);

--
-- Indexes for table `dimension_product_iteration_item`
--
ALTER TABLE `dimension_product_iteration_item`
  ADD PRIMARY KEY (`id`),
  ADD KEY `system_product_id` (`system_product_id`),
  ADD KEY `dimension_product_iteration_item_ibfk_1` (`iteration_id`);

--
-- Indexes for table `matching_attribute`
--
ALTER TABLE `matching_attribute`
  ADD PRIMARY KEY (`attribute_id`),
  ADD UNIQUE KEY `uq_attribute_name` (`attribute_name`);

--
-- Indexes for table `matching_competitor_product`
--
ALTER TABLE `matching_competitor_product`
  ADD PRIMARY KEY (`competitor_product_id`);

--
-- Indexes for table `matching_configuration_group`
--
ALTER TABLE `matching_configuration_group`
  ADD PRIMARY KEY (`configuration_id`);

--
-- Indexes for table `matching_scores`
--
ALTER TABLE `matching_scores`
  ADD PRIMARY KEY (`score_id`),
  ADD KEY `idx_ms_competitor_product` (`competitor_product_id`);

--
-- Indexes for table `matching_score_attributes`
--
ALTER TABLE `matching_score_attributes`
  ADD PRIMARY KEY (`score_attribute_id`),
  ADD KEY `idx_msa_attribute` (`attribute_id`);

--
-- Indexes for table `matching_system_product`
--
ALTER TABLE `matching_system_product`
  ADD PRIMARY KEY (`product_id`),
  ADD KEY `matched_ref_id` (`competitor_product_id`),
  ADD KEY `idx_msp_product_id` (`product_id`),
  ADD KEY `idx_msp_system_product_id` (`system_product_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `dimension_product`
--
ALTER TABLE `dimension_product`
  MODIFY `product_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dimension_product_group`
--
ALTER TABLE `dimension_product_group`
  MODIFY `group_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dimension_product_iteration`
--
ALTER TABLE `dimension_product_iteration`
  MODIFY `iteration_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `dimension_product_iteration_item`
--
ALTER TABLE `dimension_product_iteration_item`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_attribute`
--
ALTER TABLE `matching_attribute`
  MODIFY `attribute_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_competitor_product`
--
ALTER TABLE `matching_competitor_product`
  MODIFY `competitor_product_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_configuration_group`
--
ALTER TABLE `matching_configuration_group`
  MODIFY `configuration_id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_scores`
--
ALTER TABLE `matching_scores`
  MODIFY `score_id` bigint NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_score_attributes`
--
ALTER TABLE `matching_score_attributes`
  MODIFY `score_attribute_id` bigint NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `matching_system_product`
--
ALTER TABLE `matching_system_product`
  MODIFY `product_id` int NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `dimension_product`
--
ALTER TABLE `dimension_product`
  ADD CONSTRAINT `dimension_product_ibfk_1` FOREIGN KEY (`group_id`) REFERENCES `dimension_product_group` (`group_id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `dimension_product_iteration`
--
ALTER TABLE `dimension_product_iteration`
  ADD CONSTRAINT `dimension_product_iteration_ibfk_1` FOREIGN KEY (`product_group_id`) REFERENCES `dimension_product_group` (`group_id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `dimension_product_iteration_item`
--
ALTER TABLE `dimension_product_iteration_item`
  ADD CONSTRAINT `dimension_product_iteration_item_ibfk_1` FOREIGN KEY (`iteration_id`) REFERENCES `dimension_product_iteration` (`iteration_id`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `dimension_product_iteration_item_ibfk_2` FOREIGN KEY (`system_product_id`) REFERENCES `dimension_product` (`system_product_id`) ON DELETE CASCADE ON UPDATE CASCADE;

--
-- Constraints for table `matching_score_attributes`
--
ALTER TABLE `matching_score_attributes`
  ADD CONSTRAINT `fk_msa_attribute` FOREIGN KEY (`attribute_id`) REFERENCES `matching_attribute` (`attribute_id`) ON DELETE CASCADE;

--
-- Constraints for table `matching_system_product`
--
ALTER TABLE `matching_system_product`
  ADD CONSTRAINT `matching_system_product_ibfk_1` FOREIGN KEY (`competitor_product_id`) REFERENCES `matching_competitor_product` (`competitor_product_id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
