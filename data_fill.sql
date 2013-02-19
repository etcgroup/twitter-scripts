----
-- Add the sentiment column
----
ALTER TABLE tweets
ADD `sentiment` float(10,9) DEFAULT NULL;

ALTER TABLE tweets
ADD INDEX `sentiment` (`sentiment`);

----
-- Add the tweet type fields
----
ALTER TABLE tweets
ADD `is_retweet` bool DEFAULT NULL,
ADD `is_reply` bool DEFAULT NULL;

----
-- Set up the is_retweet and is_reply fields
----
UPDATE tweets SET
is_retweet = (retweet_of_status_id IS NOT NULL 
OR `text` RLIKE "^RT @.*"),
is_reply = (
    in_reply_to_status_id IS NOT NULL 
    OR in_reply_to_user_id IS NOT NULL 
    OR `text` RLIKE "^@.*"
);

----
-- Add indices on the reply_to fields
----
ALTER TABLE tweets
ADD INDEX `in_reply_to_status_id` (`in_reply_to_status_id`),
ADD INDEX `in_reply_to_user_id` (`in_reply_to_user_id`);

----
-- Add indices to the retweet and reply fields
----
ALTER TABLE tweets
ADD INDEX `is_retweet` (`is_retweet`),
ADD INDEX `is_reply` (`is_reply`);