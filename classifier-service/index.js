const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

// Configuration Kafka
const kafka = new Kafka({
  clientId: 'classifier-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'classifier-group' });
const producer = kafka.producer();

// Configuration MongoDB
const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'commercial-classification';
const client = new MongoClient(mongoUrl);

// Règles de classification
const classificationRules = {
  isImportantPurchase: (transaction) => transaction.montant > 800,
  isRecurringPurchase: async (transaction, db) => {
    const lastMonth = new Date();
    lastMonth.setMonth(lastMonth.getMonth() - 1);

    const previousPurchases = await db.collection('transactions')
      .find({
        client_id: transaction.client_id,
        produit: transaction.produit,
        date: { $gte: lastMonth }
      })
      .toArray();

    return previousPurchases.length > 0;
  },
  isRiskyTransaction: (transaction) => {
    return transaction.type_paiement === 'carte' &&
      transaction.lieu !== 'Tunis' &&
      transaction.montant > 500;
  },
  isOutlierTransaction: async (transaction, db) => {
    const clientStats = await db.collection('client_stats')
      .findOne({ client_id: transaction.client_id });

    if (!clientStats) return false;

    const threshold = clientStats.average_amount + (3 * clientStats.std_dev);
    return transaction.montant > threshold;
  }
};

async function classifyTransaction(transaction, db) {
  const classifications = [];

  if (classificationRules.isImportantPurchase(transaction)) {
    classifications.push('Achat important');
  }

  if (await classificationRules.isRecurringPurchase(transaction, db)) {
    classifications.push('Achat récurrent');
  }

  if (classificationRules.isRiskyTransaction(transaction)) {
    classifications.push('Vente à risque');
  }

  if (await classificationRules.isOutlierTransaction(transaction, db)) {
    classifications.push('Vente hors norme');
  }

  if (transaction.montant < 100) {
    classifications.push('Achat promotionnel');
  }

  return classifications.length > 0 ? classifications : ['Transaction standard'];
}

async function processTransactions() {
  try {
    await consumer.connect();
    await producer.connect();
    await client.connect();

    const db = client.db(dbName);

    await consumer.subscribe({ topic: 'commercial-transactions', fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const transaction = JSON.parse(message.value.toString());
          const classifications = await classifyTransaction(transaction, db);

          // Enregistrer la transaction classifiée
          await db.collection('classified_transactions').insertOne({
            ...transaction,
            classifications,
            classified_at: new Date()
          });

          // Publier le résultat de classification
          await producer.send({
            topic: 'classified-transactions',
            messages: [{
              value: JSON.stringify({
                ...transaction,
                classifications,
                classified_at: new Date().toISOString()
              })
            }]
          });

        } catch (error) {
          console.error('Erreur lors du traitement de la transaction:', error);
        }
      }
    });
  } catch (error) {
    console.error('Erreur lors du démarrage du service:', error);
    process.exit(1);
  }
}

processTransactions();