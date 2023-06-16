const { createPool } = require("mysql");
const express = require("express");
const cronJob = require("cron").CronJob;

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const pool = createPool({
  host: "127.0.0.1",
  user: "wxomi",
  password: "wxomi",
  database: "kvStore",
  connectionLimit: 20, // connection pool limit for handling concurrent requests
});

app.get("/:key", (req, res) => {
  try {
    pool.getConnection((err, connection) => {
      if (err) throw err;
      connection.beginTransaction((err) => {
        if (err) throw err;
        //using epoch time for ttl
        //using For Update to prevent locking of transaction and handling the case of concurrent update of same key
        const selectQuery = `SELECT kv_key,CONVERT(value USING utf8) AS value,from_unixtime(ttl) AS ttl FROM kv_store WHERE kv_key=? AND from_unixtime(ttl)>NOW() FOR UPDATE`;
        connection.query(selectQuery, [req.params.key], (err, result) => {
          if (err) {
            connection.rollback(() => {
              connection.release();
              throw err;
            });
          }
          connection.commit((err) => {
            if (err) {
              connection.rollback(() => {
                connection.release();
                throw err;
              });
            }
            connection.release();
            res.status(200).send(result);
          });
        });
      });
    });
  } catch (error) {
    res.status(500).send(error.message);
  }
});

app.put("/:key", (req, res) => {
  const value = req.body.value;
  const ttl = req.body.ttl;
  try {
    pool.getConnection((err, connection) => {
      if (err) throw err;
      connection.beginTransaction((err) => {
        if (err) throw err;
        //using epoch time for ttl
        //using replace to update if key already exists saving sql query i.e extra network call
        //Format of ttl is YYYY-MM-DD HH:MM:SS
        //For update is locking the transaction and handling the case of concurrent update of same key
        //NO Wait is used to throw error if transaction is locked and not wait for it to be unlocked
        const insertQuery = `REPLACE INTO kv_store (kv_key,value,ttl) VALUES (?,?,unix_timestamp(?))`;
        //Note: For Update does not work with Replace,insert,update it only works with select
        //MySQL does not support NOWAIT after For Update
        connection.query(
          insertQuery,
          [req.params.key, value, ttl],
          (err, result) => {
            if (err) {
              connection.rollback(() => {
                connection.release();
                throw err;
              });
            }
            connection.commit((err) => {
              if (err) {
                connection.rollback(() => {
                  connection.release();
                  throw err;
                });
              }
              connection.release();
              res.status(200).send("Inserted");
            });
          }
        );
      });
    });
  } catch (error) {
    console.log("hi");
    res.status(500).send(error.sqlMessage);
  }
});

app.delete("/:key", (req, res) => {
  try {
    pool.getConnection((err, connection) => {
      if (err) throw err;
      connection.beginTransaction((err) => {
        if (err) throw err;
        const deleteQuery = `UPDATE kv_store SET ttl=-1 Where kv_key=? AND from_unixtime(ttl)>NOW()`;
        connection.query(deleteQuery, [req.params.key], (err, result) => {
          if (err) {
            connection.rollback(() => {
              connection.release();
              throw err;
            });
          }
          connection.commit((err) => {
            if (err) {
              connection.rollback(() => {
                connection.release();
                throw err;
              });
            }
            connection.release();
            res.status(200).send("Deleted");
          });
        });
      });
    });
  } catch (error) {
    res.status(500).send(error.message);
  }
});

//Cron job to delete expired keys
//Runs every day at 12:00 AM
//Doing Batch delete to reduce the number of queries
//Reducing number of Tree Balance operations
const job = new cronJob("0 0 * * *", () => {
  try {
    pool.getConnection((err, connection) => {
      if (err) throw err;
      connection.beginTransaction((err) => {
        if (err) throw err;
        const deleteQuery = `DELETE FROM kv_store WHERE ttl<=unix_timestamp()`;
        connection.query(deleteQuery, (err, result) => {
          if (err) {
            connection.rollback(() => {
              throw err;
            });
          }
          connection.commit((err) => {
            if (err) {
              connection.rollback(() => {
                throw err;
              });
            }
            console.log("Deleted");
          });
        });
      });
    });
  } catch (error) {
    console.log(error.message);
  }
});

app.listen(3000, () => {
  console.log("Server is running on port 3000.");
});

//Todo : Scaling By Sharding
