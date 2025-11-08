// IoT schema (MongoDB)
// Uses time-series collections with retention.

use('iot');

if (!db.getCollectionNames().includes('devices')) {
  db.createCollection('devices', {
    validator: { $jsonSchema: {
      bsonType: 'object',
      required: ['_id','model','created_at'],
      properties: {
        _id: { bsonType: 'string' }, // device_id as string
        model: { bsonType: 'string' },
        location: { bsonType: ['string','null'] },
        created_at: { bsonType: 'date' }
      }
    }}
  });
}

// Time-series readings (MongoDB 5.0+). Meta stored in device_id.
if (!db.getCollectionNames().includes('readings')) {
  db.createCollection('readings', {
    timeseries: {
      timeField: 'ts',
      metaField: 'device_id',
      granularity: 'seconds'
    },
    expireAfterSeconds: 31536000 // 365 days retention
  });
}

db.readings.createIndex({ device_id: 1, ts: -1 });
