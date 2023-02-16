const express = require('express')
const { Kafka } = require('kafkajs')

const app = express()
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
})

app.use(express.json())

app.post('/messages', async (req, res) => {
  const producer = kafka.producer()

  await producer.connect()
  await producer.send({
    topic: 'test-topic',
    messages: [{ value: req.body.message }],
  })
  await producer.disconnect()

  res.json({ message: 'Mensagem enviada com sucesso!' })
})

async function run() {
  const consumer = kafka.consumer({ groupId: 'test-group' })

  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  })
}

run().catch(console.error)

app.listen(3000, () => {
  console.log('Servidor escutando na porta 3000')
})
