#!/usr/bin/env node

const amqp = require('amqplib/callback_api');

const RABBIT_MQ_URL = 'amqp://localhost';
const RECONNECT_INTERVAL = 5000;

const EXCHANGE_NAME = 'topic_skilift'
const EXCHANGE_TYPE = 'topic'


// listen to everything
const ROUTING_KEY = 'skilift.#'

// listen to a everything from a specific ski lift
// (in this case lift with id `lift1`)
// const ROUTING_KEY = 'skilift.lift1.#'

// listen to sensor events from all the lifts
// const ROUITNG_KEY = 'skilift.*.logs.sensor.#

// etc.
// Refer to the PDF documentation for details on the message types
// and their corresponding routing keys.




function startConsuming() {
  amqp.connect(RABBIT_MQ_URL, function (error0, connection) {
    if (error0) {
      console.error('Connection failed, retrying in 5 seconds...', error0.message);
      return setTimeout(startConsuming, RECONNECT_INTERVAL);
    }

    console.log('Connected to RabbitMQ');
    connection.on('error', (err) => {
      console.error('Connection error:', err.message);
      connection.close();
    });

    connection.on('close', () => {
      console.error('Connection closed, attempting to reconnect...');
      setTimeout(startConsuming, RECONNECT_INTERVAL);
    });

    connection.createChannel(function (error1, channel) {
      if (error1) {
        console.error('Failed to create channel:', error1.message);
        return connection.close();
      }


      channel.assertExchange(EXCHANGE_NAME, EXCHANGE_TYPE, {
        durable: false,
      });

      channel.assertQueue(
        '',
        {
          exclusive: true,
        },
        function (error2, q) {
          if (error2) {
            console.error('Failed to assert queue:', error2.message);
            return connection.close();
          }
          console.log(' [*] Waiting for logs. To exit press CTRL+C');

          channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEY);

          channel.consume(
            q.queue,
            function (msg) {
              try {
                const content = JSON.parse(msg.content.toString());
                console.log(" [x] %s:\n%s\n", msg.fields.routingKey, JSON.stringify(content, null, 2));
              } catch (error) {
                console.error("Failed to parse JSON content:", msg.content.toString());
              }
            },
            {
              noAck: true,
            }
          );
        }
      );
    });
  });
}

startConsuming();