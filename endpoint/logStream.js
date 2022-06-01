const kafka = require('kafka-node')
Consumer = kafka.Consumer
const {
    addSieveLogs
} = require('./utils/UtilsData')
const {storeLogs} = require('./db')
const { countLogs, insertLog } = require('./db')

function freeze(time) {
    const stop = new Date().getTime() + time;
    while(new Date().getTime() < stop);       
}
const consom = async (client) => {
    // var counter = 0
    Consumer = kafka.Consumer,
    client = new kafka.KafkaClient(),
    consumer = new Consumer(
        client,
        [
            { topic: 'logResults', partition: 0 }
        ],
        {
            autoCommit: false,
        }
    );
    console.log('listening to logResults')
    consumer.on('message', async function (message) {
        const logM = JSON.parse(message.value);
        // console.log(logM)
        // counter = counter + 1
        // console.log('counter: ', counter)
        // console.log("freeze 3s");
        // freeze(3000);
        // console.log("done");
        await storeLogs(logM.querier, logM.log)
        // console.log('out of await')
        // if(result.success) {
        //     console.log('yay')
        //     console.log(counter)
        // } else {
        //     console.log('nay')
        // }
    });
}


const getCountLogs = async () => {
    var cnt = new Promise(function (resolve, reject) {
        countLogs(resolve, reject)
    })
    await cnt
    var count = 0
    await cnt.then(res => {
        // console.log('res in getcount server', res)
        count = res
    })
    // console.log('count in server function: ', count)
    return count
}

const syncLogs = async () => { 
    console.log('called')
    var s = setInterval(async () => {
        var count = await getCountLogs()
        console.log('countin sync log: ', count)
        // while (count !== 0) {
        if (count !== 0) {
            await insertLog()
        }
        count = await getCountLogs()
        console.log('countin whilesync log: ', count)
        // }
    }, 1500)
    
}

const main = async () => {
    client = new kafka.KafkaClient()
    consom(client)
    // console.log('sync')
    // syncLogs()
}

main()