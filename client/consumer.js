const { Kafka } = require('kafkajs');
const argv = require('yargs').argv;

const topic = argv.TOPIC_NAME || "teste";

const kafka = new Kafka({
  clientId: 'my-client',
  brokers: ['localhost:29092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });


const startClient = async () => {
  
  await consumer.connect();

  await consumer.subscribe({ topic });

  await consumer.run({

    eachMessage: async ({ topic, partition, message }) => {
      
      console.log('Received message:', message.value.toString());
      console.log('Imagine that I\'m processing something...');

      setTimeout(async () => {      

        await consumer.stop();
        console.log('Topic', topic, 'has been deleted');

        console.log('Wait for 2 seconds before I die...');

        setTimeout(async () => {
          process.exit(0);
        },2000);

      },5000);
      
    },
  }); //ver se o topic expira, validar fifo da fila , vaklidar se topic existe

};

startClient();

