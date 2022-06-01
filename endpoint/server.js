const express = require('express')
const app = express()
const routesAdmin = require('./routes/routesAdmin')
const routesData = require('./routes/routesData')
const routesSieve = require('./routes/routesSieve')
// const { registerDefaultAdmin } = require('./utils/UtilsAdmin')
// const { countLogs, insertLog } = require('./db')
const cors = require('cors')
app.use(cors())
app.use(express.json())

// ========= ADMIN =========
app.use('/admin', routesAdmin)
app.use('/log', routesData)
app.use('/sieve', routesSieve)


// ========= USER =========



app.listen(5344, async () => {
    console.log('listening on port 5344')
    // console.log('Enroll Admin!')
    // res = await registerDefaultAdmin()
    // console.log(res)
})
