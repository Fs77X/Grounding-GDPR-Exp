var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('./dblogs/dblogs.db');
const {
    addSieveLogs
} = require('./utils/UtilsData')
const storeLogs = async (id, log) => {
    db.serialize(function () {
        // db.run("CREATE TABLE lorem (info TEXT)");
        db.run("CREATE TABLE if not exists logs (sqn INTEGER PRIMARY KEY autoincrement, id TEXT, log TEXT, time TIMESTAMP default current_timestamp)")

        // var stmt = db.prepare("INSERT INTO logs (?, ?)");
        // stmt.run(id, log);
        // console.log(id, log)
        // console.log('inside storelogs')
        db.run(`INSERT INTO logs(id, log) VALUES(?, ?)`, [id, log])
    })
}

const countLogs = async (resolve, reject) => {
    // console.log('countlogs called')
    db.serialize(async function () {
        db.run("CREATE TABLE if not exists logs (sqn INTEGER PRIMARY KEY autoincrement, id TEXT, log TEXT, time TIMESTAMP default current_timestamp)")
        const res = new Promise(async (resolve, reject) => {
            db.get("SELECT COUNT(*) as count FROM logs", [], (err, row) => {
                // console.log(row.count)
                // console.log(row)
                if (typeof row === 'undefined' || row === null) {
                    // console.log('resolving here')
                    resolve(0)
                }
                // console.log('false')
                // console.log('resloving theerrere')
                resolve(row.count)
            })
        })
        await res
        res.then((count) => {
            // console.log('count in res.then: ', count)
            resolve(count)
        })

    })
}

function freeze(time) {
    const stop = new Date().getTime() + time;
    while (new Date().getTime() < stop);
}

const performInsert = async (entries) => {
    await addSieveLogs("admin", entries).then((body) => { //row.id, row.log
        console.log("YEYEYEYEEYE")
        const sqnList = []
        for (let i = 0; i < entries.length; i++) {
            sqnList.push(entries[i].sqn)
        }
        db.run("DELETE from logs where sqn in (" + sqnList.join(", ") + ")")
    }).catch((body) => console.log(body))
}

const insertLog = async () => {

    // const res = new Promise(async (resolve, reject) => {
    var entries = []
    db.each("SELECT * FROM logs order by time limit 1000", async (err, row) => {
        // console.log(row.id, row.log)
        entries.push(row)
    }, async () => {
        await performInsert(entries)
    })
    // console.log("DELETE from logs where sqn in (" + resList.join(", ") + ")")
    // db.run("DELETE from logs where sqn in (" + resList.join(", ") + ")")
    // console.log(resList)
    // resolve(resList)
    // })
    // await res
    // res.then(list => console.log("DELETE from logs where sqn in (" + list.join(", ") + ")"))

    // db.run("DELETE from logs where sqn in (" + list.join(", ") + ")")
}
module.exports = {
    storeLogs,
    countLogs,
    insertLog
}