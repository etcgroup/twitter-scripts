-- --
-- Add the sentiment column
-- --
ALTER TABLE tweets
ADD `sentiment` float(10,9) DEFAULT NULL;

ALTER TABLE tweets
ADD INDEX `sentiment` (`sentiment`);

-- --
-- Add the tweet type fields
-- --
ALTER TABLE tweets
ADD `is_retweet` bool DEFAULT NULL,
ADD `is_reply` bool DEFAULT NULL;

-- --
-- Set up the is_retweet and is_reply fields
-- --
UPDATE tweets SET
is_retweet = (retweet_of_status_id IS NOT NULL 
OR `text` RLIKE "^RT @.*"),
is_reply = (
    in_reply_to_status_id IS NOT NULL 
    OR in_reply_to_user_id IS NOT NULL 
    OR `text` RLIKE "^@.*"
);

-- --
-- Add indices on the reply_to fields
-- --
ALTER TABLE tweets
ADD INDEX `in_reply_to_status_id` (`in_reply_to_status_id`),
ADD INDEX `in_reply_to_user_id` (`in_reply_to_user_id`);

-- --
-- Add indices to the retweet and reply fields
-- --
ALTER TABLE tweets
ADD INDEX `is_retweet` (`is_retweet`),
ADD INDEX `is_reply` (`is_reply`);

-- --
--
-- Conversation Update Feb 22
--
-- --

--
-- Table structure for table `conversations`
--

CREATE TABLE IF NOT EXISTS `conversations` (
  `id` int(11) NOT NULL,
  `breadth` int(11) DEFAULT NULL,
  `depth` int(11) DEFAULT NULL,
  `root_tweet` int(11) DEFAULT NULL,
  `tweet_count` int(11) DEFAULT NULL,
  `start` datetime DEFAULT NULL,
  `end` datetime DEFAULT NULL,
  `users_count` int(11) DEFAULT NULL,
  `retweet_count` int(11) DEFAULT NULL,
  `sentiment` float(10,9) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- --
-- Add conversations and tweets recount
-- --

ALTER TABLE tweets
ADD  `conversation_id` int(10) unsigned DEFAULT NULL,
ADD  `depth` int(11) unsigned DEFAULT NULL,
ADD  `retweet_count` int(11) DEFAULT NULL,
ADD KEY `conversation_id` (`conversation_id`);

-- --
-- Update conversation counts
-- --

SET SQL_SAFE_UPDATES=0;

UPDATE tweets t
JOIN (
  SELECT original.id, SUM(IF(rt.retweet_of_status_id IS NULL, 0, 1)) as retweet_count
  FROM tweets original
  LEFT JOIN tweets rt
  ON original.id = rt.retweet_of_status_id
  GROUP BY original.id
) AS counts
SET t.retweet_count = counts.retweet_count
WHERE t.id = counts.id
