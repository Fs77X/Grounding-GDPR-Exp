const express = require('express')
const router = express.Router()
const {
    getSieveLogsByUser,
    addSieveLogs
} = require('../utils/UtilsData')

// add sieve logs
router.post('/addsievelog/:uname', async (req, res, next) => {
    data = req.body
    const result = await addSieveLogs(
        req.params.uname, 
        data["id"],
        data["data"]
        )
    if(result) {
        res.send(result)
    } else {
        res.send(result)
    }
})


// get all logs of a user
router.get('/all/:uname', async (req, res, next) => {
    const logs = await getSieveLogsByUser(req.params.uname)
    if(logs) {
        res.send(logs)
    } else {
        res.send(logs)
    }
})


module.exports = router
