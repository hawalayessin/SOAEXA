const express = require('express');
const { ApolloServer, gql } = require('apollo-server-express');
const { MongoClient } = require('mongodb');

const app = express();
const port = 4000;

// Configuration MongoDB
const mongoUrl = 'mongodb://localhost:27017';
const dbName = 'commercial-classification';
const client = new MongoClient(mongoUrl);

// Définition du schéma GraphQL
const typeDefs = gql`
  type Transaction {
    transaction_id: String!
    client_id: String!
    montant: Float!
    produit: String!
    type_paiement: String!
    lieu: String!
    date: String!
    classifications: [String!]!
    classified_at: String!
  }

  type ClientStats {
    client_id: String!
    total_transactions: Int!
    total_amount: Float!
    average_amount: Float!
    std_dev: Float!
    last_transaction: String!
  }

  type Query {
    transactions(
      client_id: String
      classification: String
      startDate: String
      endDate: String
    ): [Transaction!]!
    
    clientStats(client_id: String!): ClientStats
    
    classificationStats: [ClassificationStats!]!
  }

  type ClassificationStats {
    classification: String!
    count: Int!
    total_amount: Float!
    average_amount: Float!
  }
`;

// Résolveurs GraphQL
const resolvers = {
  Query: {
    transactions: async (_, { client_id, classification, startDate, endDate }, { db }) => {
      const query = {};

      if (client_id) query.client_id = client_id;
      if (classification) query.classifications = classification;
      if (startDate || endDate) {
        query.date = {};
        if (startDate) query.date.$gte = new Date(startDate);
        if (endDate) query.date.$lte = new Date(endDate);
      }

      return await db.collection('transactions').find(query).toArray();
    },

    clientStats: async (_, { client_id }, { db }) => {
      return await db.collection('client_stats').findOne({ client_id });
    },

    classificationStats: async (_, __, { db }) => {
      const stats = await db.collection('transactions').aggregate([
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

      return stats;
    }
  }
};

async function startServer() {
  try {
    await client.connect();
    const db = client.db(dbName);

    const server = new ApolloServer({
      typeDefs,
      resolvers,
      context: { db }
    });

    await server.start();
    server.applyMiddleware({ app });

    app.listen(port, () => {
      console.log(`Query service démarré sur http://localhost:${port}${server.graphqlPath}`);
    });
  } catch (error) {
    console.error('Erreur lors du démarrage du service:', error);
    process.exit(1);
  }
}

startServer();