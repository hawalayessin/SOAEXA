const express = require('express');
const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

// Configuration Kafka
const kafka = new Kafka({
  clientId: 'storage-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'storage-group' });

// Configuration MongoDB
const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'commercial-classification';
const client = new MongoClient(mongoUrl);

// Schémas MongoDB
const transactionSchema = {
  transaction_id: String,
  client_id: String,
  montant: Number,
  produit: String,
  type_paiement: String,
  lieu: String,
  date: Date,
  classifications: [String],
  metadata: Object,
  stored_at: Date
};

const clientStatsSchema = {
  client_id: String,
  total_transactions: Number,
  total_amount: Number,
  average_amount: Number,
  std_dev: Number,
  last_transaction: Date,
  updated_at: Date
};

async function updateClientStats(db, transaction) {
  const statsCollection = db.collection('client_stats');

  // Mettre à jour les statistiques du client
  await statsCollection.updateOne(
    { client_id: transaction.client_id },
    [
      {
        $set: {
          last_transaction: new Date(transaction.date),
          total_transactions: { $add: ['$total_transactions', 1] },
          total_amount: { $add: ['$total_amount', transaction.montant] },
          updated_at: new Date()
        }
      },
      {
        $set: {
          average_amount: { $divide: ['$total_amount', '$total_transactions'] }
        }
      }
    ],
    { upsert: true }
  );

  // Calculer l'écart-type
  const allTransactions = await db.collection('transactions')
    .find({ client_id: transaction.client_id })
    .toArray();

  const amounts = allTransactions.map(t => t.montant);
  const mean = amounts.reduce((a, b) => a + b, 0) / amounts.length;
  const variance = amounts.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / amounts.length;
  const stdDev = Math.sqrt(variance);

  await statsCollection.updateOne(
    { client_id: transaction.client_id },
    {
      $set: {
        std_dev: stdDev,
        updated_at: new Date()
      }
    }
  );
}

async function processTransactions() {
  try {
    await consumer.connect();
    await client.connect();

    const db = client.db(dbName);

    // Créer les index pour optimiser les requêtes
    await db.collection('transactions').createIndex({ transaction_id: 1 }, { unique: true });
    await db.collection('transactions').createIndex({ client_id: 1 });
    await db.collection('transactions').createIndex({ date: 1 });
    await db.collection('transactions').createIndex({ classifications: 1 });

    await db.collection('client_stats').createIndex({ client_id: 1 }, { unique: true });

    await consumer.subscribe({ topic: 'classified-transactions', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const transaction = JSON.parse(message.value.toString());

          // Stocker la transaction
          await db.collection('transactions').insertOne({
            ...transaction,
            date: new Date(transaction.date),
            stored_at: new Date()
          });

          // Mettre à jour les statistiques du client
          await updateClientStats(db, transaction);

          console.log(`Transaction ${transaction.transaction_id} stockée avec succès`);

        } catch (error) {
          console.error('Erreur lors du stockage de la transaction:', error);
        }
      }
    });
  } catch (error) {
    console.error('Erreur lors du démarrage du service:', error);
    process.exit(1);
  }
}

// API REST pour accéder aux données
app.get('/api/transactions', async (req, res) => {
  try {
    const { client_id, start_date, end_date, classification } = req.query;
    console.log('Requête reçue avec les paramètres:', { client_id, start_date, end_date, classification });

    const query = {};

    if (client_id) query.client_id = client_id;
    if (classification) query.classifications = { $in: [classification] };
    if (start_date || end_date) {
      query.date = {};
      if (start_date) query.date.$gte = new Date(start_date);
      if (end_date) query.date.$lte = new Date(end_date);
    }

    console.log('Requête MongoDB:', JSON.stringify(query));

    const db = client.db(dbName);
    const transactions = await db.collection('transactions')
      .find(query)
      .sort({ date: -1 })
      .limit(100)
      .toArray();

    console.log(`Nombre de transactions trouvées: ${transactions.length}`);
    res.json(transactions);
  } catch (err) {
    console.error('Erreur détaillée:', err);
    res.status(500).json({
      error: err.message,
      details: err.stack,
      query: req.query
    });
  }
});

app.get('/api/client-stats/:client_id', async (req, res) => {
  try {
    const stats = await client.db(dbName)
      .collection('client_stats')
      .findOne({ client_id: req.params.client_id });

    if (!stats) {
      return res.status(404).json({ error: 'Client non trouvé' });
    }

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/classification-stats', async (req, res) => {
  try {
    const stats = await client.db(dbName)
      .collection('transactions')
      .aggregate([
        { $unwind: '$classifications' },
        {
          $group: {
            _id: '$classifications',
            count: { $sum: 1 },
            total_amount: { $sum: '$montant' }
          }
        },
        {
          $project: {
            classification: '$_id',
            count: 1,
            total_amount: 1,
            average_amount: { $divide: ['$total_amount', '$count'] }
          }
        }
      ]).toArray();

    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Démarrage du service
async function startService() {
  try {
    await processTransactions();
    app.listen(8001, () => {
      console.log('Service de stockage démarré sur http://localhost:8001');
    });
  } catch (error) {
    console.error('Erreur lors du démarrage du service:', error);
    process.exit(1);
  }
}

startService();