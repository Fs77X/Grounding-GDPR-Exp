const express = require('express')
const router = express.Router()
const { 
    isValidAdmin, 
    addNewUser 
} = require('../utils/UtilsAdmin')

// check if admin is valid
router.get('/isvalid/:adminname', async (req, res, next) => {
    const is_valid_admin = await isValidAdmin(req.params.adminname)
    if(is_valid_admin) {
        res.send('VALID ADMIN')
    } else {
        res.send('ERROR: NOT A VALID ADMIN')
    }
})

// use this first to create user
router.get('/adduser/:admin/:user', async (req, res) => {
    const admin = req.params.admin
    const user = req.params.user
    const msg = await addNewUser(admin, user)
    if(msg) {
        res.send(msg)
    } else {
        res.send('Unknown error')
    }
})

router.delete('/userid', async (req, res) => {
})

module.exports = router
