const express = require('express')
const { Kafka } = require('kafkajs')

const app = express()
app.use(express.json())

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:29092'],
})


app.post('/publish', async (req, res) => {

  const { message, topic } = req.body;

  const producer = kafka.producer()

  await producer.connect()
  await producer.send({
    topic,
    messages: [{ value: message }],
  })
  await producer.disconnect()  
  res.json({ message: 'Mensagem enviada com sucesso!' })
})


app.listen(3000, () => {
  console.log('Gerenciador subiu na porta 3000')
})
