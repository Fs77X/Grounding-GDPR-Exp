const express = require('express')
const router = express.Router()
const {
    addSieveLogs
} = require('../utils/UtilsData')
const { prod } = require('../kafkaSieve/sieveProduce')
// const { consom } = require('../kafkaSieve/sieveConsume')
const kafka = require('kafka-node')

const buff = []

const get_data = function (limit, uid, qid, callback) {
    var i = 0
    return new Promise(function (resolve, reject) {
        var get_data = setInterval(function () {
            // console.log('Loop: ' + i);
            var result = {}
            if (i === limit - 1) {
                clearInterval(get_data)
                reject(result)
            }
            for (var j = 0; j < buff.length; j++) {
                if (buff[j].uid == uid && buff[j].qid == qid) {
                    result = {
                        'mallData': buff[j].mallData,
                        'metaData': buff[j].metaData,
                        'timestamp': buff[j].timestamp,
                        'qid': buff[j].qid
                    }
                    buff.splice(j, 1)
                    clearInterval(get_data)
                    resolve(result)
                    break
                }
            }
            i++;
        }, 10)
    });
}

router.post("/data", async (req, res) => {
    buff.push({
        uid: req.body.uid,
        qid: req.body.qid,
        msg: req.body.msg,
        mallData: req.body.md,
        metaData: req.body.metaData,
        timestamp: Date.now()
    })
    res.status(200).send()
})

router.get("/dude", (req, res) => {
    uid = req.headers.uid
    qid = req.headers.qid

    get_data(1000, uid, qid).then((result) => {
        res.status(200).send(result)
    })
})

router.post("/mget_obj", async (req, res) => {
    data = req.body
    id = data.id
    prop = data.prop
    info = data.info
    query = data.query
    qid = "mget_obj" + id + Date.now()
    updateKey = ""
    // const result = addSieveLogs("admin", data.id, JSON.stringify(data))
    client = new kafka.KafkaClient()
    await prod(client, id, prop, info, query, updateKey, '', null, null, qid)
    // const sol = await consom(client)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        console.log("NONONO")
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.post('/madd_obj/:device_id', async (req, res) => {
    const data = req.body
    const id = req.params.device_id
    prop = ""
    info = ""
    query = "madd_obj"
    qid = query + id + Date.now()
    updateKey = ""
    const mallData = data.mallData
    const reformatMallData = {
        id: mallData.id,
        shop_name: mallData.shopName,
        obs_date: mallData.obsDate,
        obs_time: mallData.obsTime,
        user_interest: mallData.userInterest,
        device_id: mallData.deviceID
    }
    const metaData = data.metaData
    const reformattedMetaData = {
        id: metaData.id,
        querier: metaData.querier,
        purpose: metaData.purpose,
        ttl: metaData.ttl,
        origin: metaData.origin,
        objection: metaData.objection,
        sharing: metaData.sharing,
        key: metaData.key,
        policy_id: metaData.policyID,
        enforcement_action: metaData.enforcementAction,
        inserted_at: metaData.insertedAt,
        device_id: metaData.deviceID
    }
    client = new kafka.KafkaClient()
    await prod(client, id, prop, info, query, updateKey, '', reformatMallData, reformattedMetaData, qid)
    // const sol = await consom(client)
    get_data(1000, id, qid).then((result) => {
        res.status(201).send(result)
    }).catch(() => {
        console.log('epic fail in madd_obj')
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.get('/mget_entry/:device_id/:id', async (req, res) => {
    const entry = req.params.id
    const device_id = req.params.device_id
    prop = "id"
    info = entry
    query = "mget_entry"
    updateKey = ""
    qid = "mget_entry" + device_id + Date.now()
    client = new kafka.KafkaClient()
    await prod(client, device_id, prop, info, query, updateKey, '', null, null, qid)
    get_data(1000, device_id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        console.log('mgetnetry timeout')
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.put('/mmodify_obj/:device_id/:id', async (req, res) => {
    const updateKey = req.params.id
    const data = req.body
    const id = req.params.device_id
    prop = data.prop
    info = data.info
    query = "mmodify_obj"
    qid = "mmodify_obj" + id + Date.now()
    client = new kafka.KafkaClient()
    // console.log(id, prop, info, query, updateKey, '', null, null, qid)
    await prod(client, id, prop, info, query, updateKey, '', null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        console.log('timeout')
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.put('/mmodify_metaobj/:device_id/:id', async (req, res) => {
    const updateKey = req.params.id
    const data = req.body
    const id = req.params.device_id
    prop = data.prop
    info = data.info
    query = "mmodify_metaobj"
    client = new kafka.KafkaClient()
    qid = "mmodify_metaobj" + id + Date.now()
    await prod(client, id, prop, info, query, updateKey, "", null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.put('/mmodify_metaController', async (req, res) => {
    const data = req.body
    const id = data.id
    prop = data.prop
    info = data.info
    changeVal = data.changeVal
    query = "mmodify_metaController"
    client = new kafka.KafkaClient()
    qid = "mmodify_metaController" + id + Date.now()
    await prod(client, id, prop, info, query, "", changeVal, null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        console.log('epic fail in mmodify_metaController')
        res.status(500).send({msg: "Ruh roh"});
    })
})


router.delete('/mdelete_obj/:device_id/:id', async (req, res) => {
    const updateKey = req.params.id
    const id = req.params.device_id
    prop = ''
    info = ''
    query = 'mdelete_obj'
    client = new kafka.KafkaClient()
    qid = "mdelete_obj" + id + Date.now()
    await prod(client, id, prop, info, query, updateKey, '', null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        console.log('epic fail in mdelete_obj')
        res.status(500).send({msg: "Ruh roh"});
    })


})

router.get('/mget_objUSR/:device_id', async (req, res) => {
    const id = req.params.device_id
    const updateKey = ""
    prop = ''
    info = ''
    query = 'mget_objUSR'
    client = new kafka.KafkaClient()
    qid = query + id + Date.now()
    await prod(client, id, prop, info, query, updateKey, '', null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        res.status(500).send({msg: "Ruh roh"});
    })
})

router.get('/mget_metaEntry/:device_id/:id', async (req, res) => {
    const updateKey = req.params.id
    const id = req.params.device_id
    prop = ''
    info = ''
    query = 'mget_metaEntry'
    client = new kafka.KafkaClient()
    qid = "mget_metaEntry" + id + Date.now()
    await prod(client, id, prop, info, query, updateKey, '', null, null, qid)
    get_data(1000, id, qid).then((result) => {
        res.status(200).send(result)
    }).catch(() => {
        res.status(500).send({msg: "Ruh roh"});
    })
})

module.exports = router