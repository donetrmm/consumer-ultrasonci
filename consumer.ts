import amqp from 'amqplib';
import dotenv from 'dotenv';

dotenv.config();

async function consumeMessages() {
  const options = {
    vhost: process.env.AMQP_VHOST,
    username: process.env.AMQP_USERNAME,
    password: process.env.AMQP_PASSWORD,
    port: Number(process.env.AMQP_PORT),
  };

  const url = process.env.AMQP_URL || '';
  const queue = process.env.AMQP_QUEUE || '';
  const maxReconnectAttempts = 10;
  const initialReconnectDelay = 1000; // 1 segundo
  let reconnectAttempts = 0;

  async function connectAndConsume() {
    try {
      console.log('Intentando conectar a RabbitMQ...');
      const connection = await amqp.connect(url, options);
      const channel = await connection.createChannel();

      await channel.assertQueue(queue, { durable: true });

      console.log(`Conectado a RabbitMQ y escuchando mensajes en la cola ${queue}`);

      channel.consume(queue, async (msg) => {
        if (msg !== null) {
          try {
            await enviarMensajeALaAPI(msg.content.toString());
            channel.ack(msg); // Confirmar el mensaje solo si se procesa con éxito
          } catch (error: any) {
            console.error('Error al procesar el mensaje:', error.message);
            channel.nack(msg, false, true); // Reenviar el mensaje a la cola
          }
        }
      });

      // Reiniciar contador de intentos después de una conexión exitosa
      reconnectAttempts = 0;

      // Manejar cierre inesperado de conexión
      connection.on('close', async () => {
        console.error('Conexión cerrada inesperadamente. Intentando reconectar...');
        await reconnectWithDelay();
      });

      connection.on('error', async (error) => {
        console.error('Error en la conexión:', error.message);
        await reconnectWithDelay();
      });

    } catch (error: any) {
      console.error('Error al conectar a RabbitMQ:', error.message);
      await reconnectWithDelay();
    }
  }

  async function reconnectWithDelay() {
    if (reconnectAttempts >= maxReconnectAttempts) {
      console.error('Número máximo de intentos de reconexión alcanzado. Abortando...');
      process.exit(1);
    }

    const delay = initialReconnectDelay * Math.pow(2, reconnectAttempts); // Retraso exponencial
    console.log(`Intentando reconectar en ${delay / 1000} segundos...`);
    await new Promise((resolve) => setTimeout(resolve, delay));
    reconnectAttempts += 1;
    await connectAndConsume();
  }

  await connectAndConsume();
}

async function enviarMensajeALaAPI(message: any) {
  const apiUrl = 'http://107.23.14.43/registro';
  const messageJSON = JSON.parse(message);

  const registroPersonasAdentro = {
    fecha: messageJSON.fecha,
    hora: messageJSON.hora,
    numero_personas: messageJSON.numero_personas,
    lugar: messageJSON.lugar,
    idKit: messageJSON.idKit,
  };

  console.log('Lo que se envía al endpoint:', registroPersonasAdentro);

  const requestOptions = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(registroPersonasAdentro),
  };

  console.log('Cuerpo de la solicitud:', requestOptions.body);

  try {
    const response = await fetch(apiUrl, requestOptions);
    if (!response.ok) {
      throw new Error(`Error al enviar mensaje a la API: ${response.status} - ${response.statusText}`);
    }
    console.log('Mensaje enviado a la API con éxito.');
  } catch (err: any) {
    console.error('Error al comunicar con la API:', err.message);
    // No volver a lanzar el error aquí para evitar detener el consumidor
  }
}

// Iniciar el consumidor
consumeMessages().catch((error) => {
  console.error('Error inesperado en el consumidor:', error.message);
  process.exit(1);
});
