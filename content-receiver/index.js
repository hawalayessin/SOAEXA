const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');
const path = require('path');

// Configuration Kafka
const kafka = new Kafka({
  clientId: 'transaction-receiver-service',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

// Configuration gRPC
const PROTO_PATH = path.join(__dirname, 'content_receiver.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const transactionReceiver = protoDescriptor.transactionreceiver;

// Fonction pour afficher les logs de test
function logTestConsole(transaction, status, message) {
  console.log('\n=== TEST CONSOLE ===');
  console.log('Timestamp:', new Date().toISOString());
  console.log('Transaction ID:', transaction.transaction_id);
  console.log('Client ID:', transaction.client_id);
  console.log('Montant:', transaction.montant);
  console.log('Produit:', transaction.produit);
  console.log('Type de paiement:', transaction.type_paiement);
  console.log('Lieu:', transaction.lieu);
  console.log('Date:', transaction.date);
  console.log('Metadata:', JSON.stringify(transaction.metadata, null, 2));
  console.log('Status:', status);
  console.log('Message:', message);
  console.log('===================\n');
}

// Fonction de traitement des transactions
async function processTransaction(call, callback) {
  try {
    const transaction = call.request;
    console.log('\nNouvelle requête gRPC reçue:', JSON.stringify(transaction, null, 2));

    // Validation de la transaction
    if (!transaction.transaction_id || !transaction.client_id || !transaction.montant) {
      const errorMessage = 'Données de transaction invalides';
      logTestConsole(transaction, 'ERROR', errorMessage);
      callback({
        code: grpc.status.INVALID_ARGUMENT,
        message: errorMessage
      });
      return;
    }

    // Connexion à Kafka
    console.log('Connexion à Kafka...');
    await producer.connect();
    console.log('Connecté à Kafka avec succès');

    // Envoi de la transaction à Kafka
    console.log('Envoi de la transaction à Kafka...');
    await producer.send({
      topic: 'commercial-transactions',
      messages: [{
        value: JSON.stringify(transaction),
        headers: {
          'source': 'transaction-receiver',
          'timestamp': new Date().toISOString()
        }
      }]
    });
    console.log('Transaction envoyée à Kafka avec succès');

    // Réponse de succès
    const response = {
      status: 'SUCCESS',
      transaction_id: transaction.transaction_id,
      message: 'Transaction reçue et en cours de traitement',
      classifications: [], // Sera rempli par le service de classification
      processed_at: new Date().toISOString()
    };

    logTestConsole(transaction, 'SUCCESS', 'Transaction traitée avec succès');
    callback(null, response);

  } catch (error) {
    console.error('Erreur lors du traitement de la transaction:', error);
    logTestConsole(call.request, 'ERROR', error.message);
    callback({
      code: grpc.status.INTERNAL,
      message: 'Erreur lors du traitement de la transaction'
    });
  }
}

// Création du serveur gRPC
const server = new grpc.Server();
server.addService(transactionReceiver.TransactionReceiver.service, {
  ProcessTransaction: processTransaction
});

// Démarrage du serveur
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  console.log('\n=== SERVICE DE RÉCEPTION DES TRANSACTIONS ===');
  console.log('Démarré sur http://localhost:50051');
  console.log('En attente de requêtes gRPC...');
  console.log('===========================================\n');
  server.start();
});