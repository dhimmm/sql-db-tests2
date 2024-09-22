const { Pool } = require("pg");

const pgPool = new Pool({
  user: process.env.POSTGRES_USER,
  host: "postgres",
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432,
});

// List of isolation levels
const isolationLevels = ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"];

// Test cases

// Lost Update Test
async function testLostUpdate() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Lost Update with isolation level: ${isolationLevel}`
    );

    const client1 = await pgPool.connect();
    const client2 = await pgPool.connect();

    try {
      // Start transactions
      await client1.query("BEGIN");
      await client2.query("BEGIN");

      // Set lock timeout to 5 seconds
      await client1.query("SET lock_timeout = '5s';");
      await client2.query("SET lock_timeout = '5s';");

      // Set isolation level after starting the transaction
      await client1.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
      await client2.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

      // Fetch initial balance
      let initialBalance;
      try {
        const initialRes = await client1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        initialBalance = initialRes.rows[0].balance;
        console.log(`Initial balance: ${initialBalance}`);
      } catch (err) {
        console.error(
          `Error fetching initial balance from client1: ${err.message}`
        );
      }

      // Perform updates
      try {
        await client1.query(
          "UPDATE users SET balance = balance + 10 WHERE id = 1"
        );
      } catch (err) {
        console.error(`Error updating balance in client1: ${err.message}`);
      }

      try {
        await client2.query(
          "UPDATE users SET balance = balance + 20 WHERE id = 1"
        );
      } catch (err) {
        if (err.message.includes("lock timeout")) {
          console.error(`Lock timeout exceeded for client2: ${err.message}`);
        } else {
          console.error(`Error updating balance in client2: ${err.message}`);
        }
      }

      // Commit transactions
      try {
        await client1.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in client1: ${err.message}`
        );
      }

      try {
        await client2.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in client2: ${err.message}`
        );
      }

      // Fetch final balance
      let finalBalance;
      try {
        const finalRes = await client1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        finalBalance = finalRes.rows[0].balance;
        console.log(`Final balance after updates: ${finalBalance}`);
      } catch (err) {
        console.error(
          `Error fetching final balance from client1: ${err.message}`
        );
      }
    } catch (error) {
      console.error(`General error during Lost Update test: ${error.message}`);
    } finally {
      client1.release();
      client2.release();
    }
  }
}

// Dirty Read Test
async function testDirtyRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(`\nTesting Dirty Read with isolation level: ${isolationLevel}`);

    const client1 = await pgPool.connect();
    const client2 = await pgPool.connect();

    try {
      // Start transactions
      await client1.query("BEGIN");
      await client2.query("BEGIN");

      // Set lock timeout to 5 seconds
      await client1.query("SET lock_timeout = '5s';");
      await client2.query("SET lock_timeout = '5s';");

      // Set isolation level after starting the transaction
      await client1.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
      await client2.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

      // Update balance in Transaction 1 but do not commit
      await client1.query("UPDATE users SET balance = 1000 WHERE id = 1");

      // Read balance in Transaction 2
      let dirtyReadBalance;
      try {
        const dirtyReadRes = await client2.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        dirtyReadBalance = dirtyReadRes.rows[0].balance;
        console.log(`Dirty read balance: ${dirtyReadBalance}`);
      } catch (err) {
        if (err.message.includes("lock timeout")) {
          console.error(`Lock timeout exceeded for client2: ${err.message}`);
        } else {
          console.error(`Error reading balance in client2: ${err.message}`);
        }
      }

      // Rollback Transaction 1
      await client1.query("ROLLBACK");

      // Commit Transaction 2
      await client2.query("COMMIT");
    } catch (error) {
      console.error(`Error during Dirty Read test: ${error.message}`);
    } finally {
      client1.release();
      client2.release();
    }
  }
}

// Non-Repeatable Read Test
async function testNonRepeatableRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Non-Repeatable Read with isolation level: ${isolationLevel}`
    );

    const client1 = await pgPool.connect();
    const client2 = await pgPool.connect();

    try {
      // Start transactions
      await client1.query("BEGIN");
      await client2.query("BEGIN");

      // Set lock timeout to 5 seconds
      await client1.query("SET lock_timeout = '5s';");
      await client2.query("SET lock_timeout = '5s';");

      // Set isolation level after starting the transaction
      await client1.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
      await client2.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

      // Read balance in Transaction 1
      let initialBalance;
      try {
        const initialRes = await client1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        initialBalance = initialRes.rows[0].balance;
        console.log(`Initial balance in Transaction 1: ${initialBalance}`);
      } catch (err) {
        console.error(
          `Error fetching initial balance from client1: ${err.message}`
        );
      }

      // Update balance in Transaction 2 and commit
      try {
        await client2.query(
          "UPDATE users SET balance = balance + 50 WHERE id = 1"
        );
        await client2.query("COMMIT");
      } catch (err) {
        console.error(`Error updating balance in client2: ${err.message}`);
      }

      // Re-read balance in Transaction 1
      let nonRepeatableBalance;
      try {
        const nonRepeatableRes = await client1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        nonRepeatableBalance = nonRepeatableRes.rows[0].balance;
        console.log(`Balance after concurrent update: ${nonRepeatableBalance}`);
      } catch (err) {
        console.error(
          `Error fetching non-repeatable balance from client1: ${err.message}`
        );
      }

      // Commit Transaction 1
      await client1.query("COMMIT");
    } catch (error) {
      console.error(`Error during Non-Repeatable Read test: ${error.message}`);
    } finally {
      client1.release();
      client2.release();
    }
  }
}

// Phantom Read Test
async function testPhantomRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Phantom Read with isolation level: ${isolationLevel}`
    );

    const client1 = await pgPool.connect();
    const client2 = await pgPool.connect();

    try {
      // Start transactions
      await client1.query("BEGIN");
      await client2.query("BEGIN");

      // Set lock timeout to 5 seconds
      await client1.query("SET lock_timeout = '5s';");
      await client2.query("SET lock_timeout = '5s';");

      // Set isolation level after starting the transaction
      await client1.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
      await client2.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);

      // Get initial row count in Transaction 1
      let initialCount;
      try {
        const initialRes = await client1.query(
          "SELECT COUNT(*) AS count FROM users"
        );
        initialCount = initialRes.rows[0].count;
        console.log(`Initial row count in Transaction 1: ${initialCount}`);
      } catch (err) {
        console.error(
          `Error fetching initial row count from client1: ${err.message}`
        );
      }

      // Insert new row in Transaction 2 and commit
      try {
        await client2.query(
          "INSERT INTO users (name, balance) VALUES ('Charlie', 300)"
        );
        await client2.query("COMMIT");
      } catch (err) {
        console.error(`Error inserting row in client2: ${err.message}`);
      }

      // Re-query row count in Transaction 1 (phantom read check)
      let phantomCount;
      try {
        const phantomRes = await client1.query(
          "SELECT COUNT(*) AS count FROM users"
        );
        phantomCount = phantomRes.rows[0].count;
        console.log(`Row count after concurrent insert: ${phantomCount}`);
      } catch (err) {
        console.error(
          `Error fetching phantom row count from client1: ${err.message}`
        );
      }

      // Commit Transaction 1
      await client1.query("COMMIT");
    } catch (error) {
      console.error(`Error during Phantom Read test: ${error.message}`);
    } finally {
      client1.release();
      client2.release();
    }
  }
}

module.exports = {
  testLostUpdate,
  testDirtyRead,
  testNonRepeatableRead,
  testPhantomRead,
};
