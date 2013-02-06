-- phpMyAdmin SQL Dump
-- version 3.4.5
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Feb 06, 2013 at 04:06 AM
-- Server version: 5.5.16
-- PHP Version: 5.3.8

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `twitter`
--

-- --------------------------------------------------------

--
-- Table structure for table `hashtags`
--

CREATE TABLE IF NOT EXISTS `hashtags` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `string` varchar(150) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `hashtag_uses`
--

CREATE TABLE IF NOT EXISTS `hashtag_uses` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `tweet_id` bigint(20) unsigned NOT NULL,
  `hashtag_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `hashtag_id` (`hashtag_id`),
  KEY `tweet_id` (`tweet_id`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `mentions`
--

CREATE TABLE IF NOT EXISTS `mentions` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `tweet_id` bigint(20) unsigned NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `tweet_id` (`tweet_id`),
  KEY `user_id` (`user_id`)
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `tweets`
--

CREATE TABLE IF NOT EXISTS `tweets` (
  `id` bigint(20) unsigned NOT NULL,
  `user_id` int(10) unsigned NOT NULL,
  `created_at` datetime NOT NULL,
  `in_reply_to_status_id` bigint(20) unsigned DEFAULT NULL,
  `in_reply_to_user_id` int(11) DEFAULT NULL,
  `retweet_of_status_id` bigint(20) DEFAULT NULL,
  `text` varchar(255) CHARACTER SET utf8mb4 NOT NULL,
  `followers_count` int(11) NOT NULL,
  `friends_count` int(11) NOT NULL,
  `json` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `users`
--

CREATE TABLE IF NOT EXISTS `users` (
  `id` int(11) NOT NULL,
  `screen_name` varchar(100) NOT NULL,
  `name` varchar(100) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `location` varchar(150) DEFAULT NULL,
  `utc_offset` int(11) DEFAULT NULL,
  `lang` varchar(15) DEFAULT NULL,
  `time_zone` varchar(150) DEFAULT NULL,
  `statuses_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
