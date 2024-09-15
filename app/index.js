require("dotenv").config();
const fastify = require("fastify")({ logger: true });
const mysql = require("mysql2/promise");
const { Pool } = require("pg");

// MySQL connection (Percona)
const mysqlConnection = mysql.createPool({
  host: "percona",
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
});

// PostgreSQL connection
const pgPool = new Pool({
  user: process.env.POSTGRES_USER,
  host: "postgres",
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

// Utility function for logging and benchmarking
function logBenchmark(startTime, description) {
  const endTime = Date.now();
  const elapsed = endTime - startTime;
  console.log(`${description} - Execution time: ${elapsed}ms`);
  return elapsed;
}

// Utility function for detailed logs
function logExplanation(
  testType,
  isolationLevel,
  description,
  expected,
  actual
) {
  console.log(`\n--- ${testType} (${isolationLevel}) ---`);
  console.log(`Test description: ${description}`);
  console.log(`Expected result: ${expected}`);
  console.log(`Actual result: ${actual}`);
  console.log("--------------------------------\n");
}

const isolationLevelsMySQL = [
  "READ UNCOMMITTED",
  "READ COMMITTED",
  "REPEATABLE READ",
  "SERIALIZABLE",
];
const isolationLevelsPostgres = [
  "READ COMMITTED",
  "REPEATABLE READ",
  "SERIALIZABLE",
];

//////////////////////////
// MySQL (Percona) Tests //
//////////////////////////

// MySQL Lost Update with all isolation levels
async function testLostUpdateMySQL() {
  for (const isolationLevel of isolationLevelsMySQL) {
    const startTime = Date.now();
    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      await connection1.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );
      await connection2.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );

      await connection1.query("START TRANSACTION;");
      await connection2.query("START TRANSACTION;");

      await connection1.query(
        "UPDATE users SET balance = balance + 10 WHERE id = 1;"
      );
      await connection2.query(
        "UPDATE users SET balance = balance + 20 WHERE id = 1;"
      );

      await connection1.query("COMMIT;");
      await connection2.query("COMMIT;");

      const [rows] = await connection1.query(
        "SELECT balance FROM users WHERE id = 1;"
      );
      logExplanation(
        "Lost Update",
        isolationLevel,
        "Both transactions update the same row without seeing each other’s updates, leading to lost changes.",
        "The final balance should reflect both updates (i.e., balance + 30).",
        rows[0].balance
      );
    } catch (err) {
      console.error(`Error in testLostUpdateMySQL (${isolationLevel}):`, err);
    } finally {
      connection1.release();
      connection2.release();
      logBenchmark(startTime, `MySQL Lost Update (${isolationLevel})`);
    }
  }
}

// MySQL Dirty Read with all isolation levels
async function testDirtyReadMySQL() {
  for (const isolationLevel of isolationLevelsMySQL) {
    const startTime = Date.now();
    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      await connection1.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );
      await connection2.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );

      await connection1.query("START TRANSACTION;");
      await connection1.query("UPDATE users SET balance = 1000 WHERE id = 1;");

      await connection2.query("START TRANSACTION;");
      const [rows] = await connection2.query(
        "SELECT balance FROM users WHERE id = 1;"
      );

      logExplanation(
        "Dirty Read",
        isolationLevel,
        "One transaction reads data that has been modified by another uncommitted transaction.",
        "The second transaction sees the uncommitted balance of 1000.",
        rows[0].balance
      );

      await connection1.query("ROLLBACK;");
      await connection2.query("COMMIT;");
    } catch (err) {
      console.error(`Error in testDirtyReadMySQL (${isolationLevel}):`, err);
    } finally {
      connection1.release();
      connection2.release();
      logBenchmark(startTime, `MySQL Dirty Read (${isolationLevel})`);
    }
  }
}

// MySQL Non-Repeatable Read with all isolation levels
async function testNonRepeatableReadMySQL() {
  for (const isolationLevel of isolationLevelsMySQL) {
    const startTime = Date.now();
    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      await connection1.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );
      await connection2.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );

      await connection1.query("START TRANSACTION;");
      const [initialRead] = await connection1.query(
        "SELECT balance FROM users WHERE id = 1;"
      );
      console.log("MySQL Initial read balance:", initialRead[0].balance);

      await connection2.query("START TRANSACTION;");
      await connection2.query(
        "UPDATE users SET balance = balance + 50 WHERE id = 1;"
      );
      await connection2.query("COMMIT;");

      const [secondRead] = await connection1.query(
        "SELECT balance FROM users WHERE id = 1;"
      );
      logExplanation(
        "Non-Repeatable Read",
        isolationLevel,
        "One transaction reads data multiple times, but the data has changed due to another committed transaction.",
        "The first read and second read should show different values.",
        `Initial: ${initialRead[0].balance}, After Update: ${secondRead[0].balance}`
      );

      await connection1.query("COMMIT;");
    } catch (err) {
      console.error(
        `Error in testNonRepeatableReadMySQL (${isolationLevel}):`,
        err
      );
    } finally {
      connection1.release();
      connection2.release();
      logBenchmark(startTime, `MySQL Non-Repeatable Read (${isolationLevel})`);
    }
  }
}

// MySQL Phantom Read with all isolation levels
async function testPhantomReadMySQL() {
  for (const isolationLevel of isolationLevelsMySQL) {
    const startTime = Date.now();
    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      await connection1.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );
      await connection2.query(
        `SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`
      );

      await connection1.query("START TRANSACTION;");
      const [initialRead] = await connection1.query(
        "SELECT COUNT(*) AS count FROM users;"
      );
      console.log("MySQL Initial user count:", initialRead[0].count);

      await connection2.query("START TRANSACTION;");
      await connection2.query(
        'INSERT INTO users (name, balance) VALUES ("Charlie", 300);'
      );
      await connection2.query("COMMIT;");

      const [secondRead] = await connection1.query(
        "SELECT COUNT(*) AS count FROM users;"
      );
      logExplanation(
        "Phantom Read",
        isolationLevel,
        "A transaction reads a set of rows that satisfies a condition, but another transaction inserts rows that satisfy the condition after the first read.",
        "The first count and second count should be different.",
        `Initial Count: ${initialRead[0].count}, After Insert: ${secondRead[0].count}`
      );

      await connection1.query("COMMIT;");
    } catch (err) {
      console.error(`Error in testPhantomReadMySQL (${isolationLevel}):`, err);
    } finally {
      connection1.release();
      connection2.release();
      logBenchmark(startTime, `MySQL Phantom Read (${isolationLevel})`);
    }
  }
}

//////////////////////////
// PostgreSQL Tests //
//////////////////////////

// PostgreSQL Lost Update with all isolation levels
async function testLostUpdatePostgres() {
  for (const isolationLevel of isolationLevelsPostgres) {
    const startTime = Date.now();
    const client1 = await pgPool.connect();
    const client2 = await pgPool.connect();

    try {
      await client1.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`);
      await client2.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`);

      await client1.query("BEGIN;");
      await client2.query("BEGIN;");

      await client1.query(
        "UPDATE users SET balance = balance + 10 WHERE id = 1;"
      );
      await client2.query(
        "UPDATE users SET balance = balance + 20 WHERE id = 1;"
      );

      await client1.query("COMMIT;");
      await client2.query("COMMIT;");

      const res = await client1.query(
        "SELECT balance FROM users WHERE id = 1;"
      );
      logExplanation(
        "Lost Update",
        isolationLevel,
        "Both transactions update the same row without seeing each other’s updates, leading to lost changes.",
        "The final balance should reflect both updates (i.e., balance + 30).",
        res.rows[0].balance
      );
    } catch (err) {
      console.error(
        `Error in testLostUpdatePostgres (${isolationLevel}):`,
        err
      );
    } finally {
      client1.release();
      client2.release();
      logBenchmark(startTime, `PostgreSQL Lost Update (${isolationLevel})`);
    }
  }
}

// Similar functions for Dirty Read, Non-Repeatable Read, and Phantom Read for PostgreSQL can be added here

//////////////////////////
// Fastify Routes //
//////////////////////////

// MySQL Routes (test with all isolation levels in a single request)
fastify.get("/mysql/lost-update", async (request, reply) => {
  await testLostUpdateMySQL();
  reply.send({ status: "MySQL Lost update scenario completed" });
});

fastify.get("/mysql/dirty-read", async (request, reply) => {
  await testDirtyReadMySQL();
  reply.send({ status: "MySQL Dirty read scenario completed" });
});

fastify.get("/mysql/non-repeatable-read", async (request, reply) => {
  await testNonRepeatableReadMySQL();
  reply.send({ status: "MySQL Non-repeatable read scenario completed" });
});

fastify.get("/mysql/phantom-read", async (request, reply) => {
  await testPhantomReadMySQL();
  reply.send({ status: "MySQL Phantom read scenario completed" });
});

// PostgreSQL Routes (test with all isolation levels in a single request)
fastify.get("/postgres/lost-update", async (request, reply) => {
  await testLostUpdatePostgres();
  reply.send({ status: "PostgreSQL Lost update scenario completed" });
});

// Other PostgreSQL routes can be added similarly

//////////////////////////
// Start Fastify Server //
//////////////////////////

const start = async () => {
  try {
    await fastify.listen(3000, "0.0.0.0");
    fastify.log.info("Server running at http://localhost:3000");
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
