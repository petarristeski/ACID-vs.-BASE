// Social Media schema (MongoDB)
// Eventual consistency acceptable; denormalize where it helps reads.

use('social');

if (!db.getCollectionNames().includes('users')) {
  db.createCollection('users', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['_id','username','created_at'],
      properties: {
        _id: { bsonType: 'binData' },
        username: { bsonType: 'string' },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}
db.users.createIndex({ username: 1 }, { unique: true });

if (!db.getCollectionNames().includes('posts')) {
  db.createCollection('posts', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['_id','user_id','content','created_at'],
      properties: {
        _id: { bsonType: 'binData' },
        user_id: { bsonType: 'binData' },
        content: { bsonType: 'string' },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}
db.posts.createIndex({ user_id: 1, created_at: -1 });

if (!db.getCollectionNames().includes('comments')) {
  db.createCollection('comments', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['_id','post_id','user_id','content','created_at'],
      properties: {
        _id: { bsonType: 'binData' },
        post_id: { bsonType: 'binData' },
        user_id: { bsonType: 'binData' },
        content: { bsonType: 'string' },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}
db.comments.createIndex({ post_id: 1, created_at: -1 });

if (!db.getCollectionNames().includes('likes')) {
  db.createCollection('likes', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['post_id','user_id','created_at'],
      properties: {
        post_id: { bsonType: 'binData' },
        user_id: { bsonType: 'binData' },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}
db.likes.createIndex({ post_id: 1, user_id: 1 }, { unique: true });
db.likes.createIndex({ user_id: 1, created_at: -1 });

// Follows relationships
if (!db.getCollectionNames().includes('follows')) {
  db.createCollection('follows', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['follower_id','followee_id','created_at'],
      properties: {
        follower_id: { bsonType: 'binData' },
        followee_id: { bsonType: 'binData' },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}
db.follows.createIndex({ follower_id: 1, followee_id: 1 }, { unique: true });
db.follows.createIndex({ followee_id: 1 });

// Optional materialized timeline per user (append-only, for fast feed reads):
// db.createCollection('timeline');
// db.timeline.createIndex({ user_id: 1, created_at: -1 });
