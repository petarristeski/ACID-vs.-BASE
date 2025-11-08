-- Social Media schema (PostgreSQL)
-- Eventual consistency acceptable for feeds; relational integrity for core entities.

CREATE SCHEMA IF NOT EXISTS social;
SET search_path TO social, public;

CREATE TABLE IF NOT EXISTS users (
  user_id    uuid PRIMARY KEY,
  username   text UNIQUE NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS posts (
  post_id    uuid PRIMARY KEY,
  user_id    uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  content    text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_posts_user_created ON posts (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS comments (
  comment_id uuid PRIMARY KEY,
  post_id    uuid NOT NULL REFERENCES posts(post_id) ON DELETE CASCADE,
  user_id    uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  content    text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_comments_post_created ON comments (post_id, created_at DESC);

CREATE TABLE IF NOT EXISTS likes (
  post_id    uuid NOT NULL REFERENCES posts(post_id) ON DELETE CASCADE,
  user_id    uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (post_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_likes_user_created ON likes (user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS follows (
  follower_id uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  followee_id uuid NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  created_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (follower_id, followee_id)
);
CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows (followee_id);

-- Optional: materialized feed can be built asynchronously into an app-managed table.

