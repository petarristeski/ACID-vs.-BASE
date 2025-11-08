-- Social Media schema (Cassandra)
-- Optimized for feed reads and high write throughput.

CREATE KEYSPACE IF NOT EXISTS social
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE social;

-- User posts ordered by time
CREATE TABLE IF NOT EXISTS posts_by_user (
  user_id uuid,
  created_at timestamp,
  post_id uuid,
  content text,
  PRIMARY KEY ((user_id), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC);

-- Materialized feed per user (fanout on write or async)
CREATE TABLE IF NOT EXISTS feed_by_user (
  user_id uuid,
  created_at timestamp,
  post_user_id uuid,
  post_id uuid,
  content text,
  PRIMARY KEY ((user_id), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC);

-- Comments by post
CREATE TABLE IF NOT EXISTS comments_by_post (
  post_id uuid,
  created_at timestamp,
  comment_id uuid,
  user_id uuid,
  content text,
  PRIMARY KEY ((post_id), created_at, comment_id)
) WITH CLUSTERING ORDER BY (created_at DESC, comment_id ASC);

-- Likes by post
CREATE TABLE IF NOT EXISTS likes_by_post (
  post_id uuid,
  user_id uuid,
  created_at timestamp,
  PRIMARY KEY ((post_id), user_id)
);

-- Follows: who each user follows
CREATE TABLE IF NOT EXISTS follows_by_user (
  user_id uuid,
  followee_id uuid,
  created_at timestamp,
  PRIMARY KEY ((user_id), followee_id)
);

