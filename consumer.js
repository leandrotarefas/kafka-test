const { Kafka } = require('kafkajs');
const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;
const topic = 'test';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });

app.use(bodyParser.json());

const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('Received message:', message.value.toString());
      await consumer.stop();
      console.log('Topic', topic, 'has been deleted');
      process.exit(0);
    },
  });
};

startConsumer();

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});
