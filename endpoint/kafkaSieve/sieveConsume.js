var kafka = require('kafka-node')
Consumer = kafka.Consumer
// client = new kafka.KafkaClient()

const consom = async (client) => {
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    consumer = new Consumer(
        client,
        [
            { topic: 'results', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );
    const consumerPromise = new Promise((resolve, reject) => {
        consumer.on('message', async function (message) {
            resolve(JSON.parse(message.value));
        });
    })
    await consumerPromise;
    const sol = {}
    consumerPromise.then((data) => {
        sol.data = data.md
        sol.msg = data.message
    })
    const consoomClose = new Promise((resolve, reject) => {
        consumer.close(true, () => resolve("done okchamp"))

    })
    await consoomClose
    consoomClose.then((str) => console.log(str))
    return sol
}

module.exports = {
    consom
}