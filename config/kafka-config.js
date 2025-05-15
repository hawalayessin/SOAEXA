const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'commercial pipline',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
    factor: 0.2,
    multiplier: 1.5,
    randomize: true
  },
  connectionTimeout: 3000,
  authenticationTimeout: 3000,
  maxInFlightRequests: 1,
  allowAutoTopicCreation: true,
  createPartitioner: Partitioners.LegacyPartitioner // Addresses the v2.0.0 warning
});

// Error handling for Kafka connection
kafka.on('error', (error) => {
  console.error('Kafka connection error:', error);
});

module.exports = {
  kafka,
  TOPICS: {
    INCOMING: 'incoming-medical-content',
    CLASSIFIED: 'classified-content',
    ERRORS: 'processing-errors'
  }
};