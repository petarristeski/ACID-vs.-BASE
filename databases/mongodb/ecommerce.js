// E-commerce schema (MongoDB)
// Run with: docker exec -it mongodb6 mongosh --username root --password root --authenticationDatabase admin --file /path/to/this/file

use('ecommerce');

// Products
if (!db.getCollectionNames().includes('products')) {
  db.createCollection('products', {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['sku', 'name', 'price_cents', 'stock', 'updated_at'],
        properties: {
          sku: { bsonType: 'string' },
          name: { bsonType: 'string' },
          price_cents: { bsonType: 'int', minimum: 0 },
          stock: { bsonType: 'int', minimum: 0 },
          updated_at: { bsonType: 'date' }
        }
      }
    }
  });
}
db.products.createIndex({ sku: 1 }, { unique: true });

// Orders
if (!db.getCollectionNames().includes('orders')) {
  db.createCollection('orders', {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'customer_id', 'status', 'total_cents', 'created_at', 'items'],
        properties: {
          _id: { bsonType: 'binData' }, // UUID
          customer_id: { bsonType: 'binData' },
          status: { enum: ['pending','paid','cancelled','shipped','failed'] },
          total_cents: { bsonType: 'int', minimum: 0 },
          created_at: { bsonType: 'date' },
          items: {
            bsonType: 'array',
            items: {
              bsonType: 'object',
              required: ['sku','qty','price_cents'],
              properties: {
                sku: { bsonType: 'string' },
                qty: { bsonType: 'int', minimum: 1 },
                price_cents: { bsonType: 'int', minimum: 0 }
              }
            }
          }
        }
      }
    }
  });
}
db.orders.createIndex({ customer_id: 1, created_at: -1 });

// Payments (1-to-1 with order)
if (!db.getCollectionNames().includes('payments')) {
  db.createCollection('payments', {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id','order_id','amount_cents','status','created_at'],
        properties: {
          _id: { bsonType: 'binData' }, // payment UUID
          order_id: { bsonType: 'binData' },
          amount_cents: { bsonType: 'int', minimum: 0 },
          status: { enum: ['authorized','captured','failed','refunded'] },
          created_at: { bsonType: 'date' }
        }
      }
    }
  });
}
db.payments.createIndex({ order_id: 1 }, { unique: true });

// Note: For strict consistency across docs, use multi-document transactions on a replica set.
