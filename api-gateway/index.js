const express = require('express');
const { ApolloServer, gql } = require('apollo-server-express');
const { Kafka } = require('kafkajs');
const { GraphQLJSON } = require('graphql-type-json');
const axios = require('axios');

const app = express();
app.use(express.json());

// Configuration Kafka
const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

// REST Endpoint pour les transactions
app.post('/api/transactions', async (req, res) => {
  try {
    const transaction = req.body;

    // Validation basique de la transaction
    if (!transaction.transaction_id || !transaction.client_id || !transaction.montant) {
      return res.status(400).json({ error: 'Données de transaction invalides' });
    }

    await producer.connect();
    await producer.send({
      topic: 'commercial-transactions',
      messages: [{
        value: JSON.stringify(transaction),
        headers: {
          'source': 'api-gateway',
          'timestamp': new Date().toISOString()
        }
      }]
    });
    res.json({
      message: "Transaction reçue",
      status: "SUCCESS",
      transaction_id: transaction.transaction_id
    });
  } catch (err) {
    console.error('Erreur lors du traitement de la transaction:', err);
    res.status(500).json({ error: err.message });
  }
});

// GraphQL Setup
const typeDefs = gql`
  scalar JSON

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
  total_transactions: Int
  total_amount: Float
  average_amount: Float
  std_dev: Float
  last_transaction: String
}

  type ClassificationStats {
    classification: String!
    count: Int!
    total_amount: Float!
    average_amount: Float!
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
`;

const resolvers = {
  JSON: GraphQLJSON,
  Query: {
    transactions: async (_, { client_id, classification, startDate, endDate }) => {
      try {
        console.log('Requête GraphQL reçue avec les paramètres:', { client_id, classification, startDate, endDate });

        const response = await axios.get('http://127.0.0.1:8001/api/transactions', {
          params: {
            client_id,
            classification,
            start_date: startDate,
            end_date: endDate
          }
        });

        console.log('Réponse du storage-service:', response.data);
        return response.data;
      } catch (error) {
        console.error('Erreur détaillée lors de la récupération des transactions:', {
          message: error.message,
          response: error.response?.data,
          status: error.response?.status
        });
        throw new Error(`Échec de la récupération des transactions: ${error.message}`);
      }
    },

    clientStats: async (_, { client_id }) => {
      try {
        const response = await axios.get(`http://127.0.0.1:8001/api/client-stats/${client_id}`);
        return response.data;
      } catch (error) {
        console.error('Erreur lors de la récupération des statistiques client:', error);
        throw new Error('Échec de la récupération des statistiques client');
      }
    },

    classificationStats: async () => {
      try {
        const response = await axios.get('http://127.0.0.1:8001/api/classification-stats');
        return response.data;
      } catch (error) {
        console.error('Erreur lors de la récupération des statistiques de classification:', error);
        throw new Error('Échec de la récupération des statistiques de classification');
      }
    }
  }
};

async function startServer() {
  try {
    // Connect Kafka producer first
    await producer.connect();
    console.log('Kafka producer connecté');

    // Then start Apollo Server
    const server = new ApolloServer({ typeDefs, resolvers });
    await server.start();
    server.applyMiddleware({ app });

    app.listen(8000, () => {
      console.log('API Gateway démarré sur http://localhost:8000');
      console.log(`GraphQL disponible sur http://localhost:8000${server.graphqlPath}`);
    });
  } catch (err) {
    console.error('Erreur lors du démarrage du serveur:', err);
    process.exit(1);
  }
}

startServer();