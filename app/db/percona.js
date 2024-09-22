const mysql = require("mysql2/promise");

const mysqlConnection = mysql.createPool({
  host: "percona",
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
});

// List of isolation levels
const isolationLevels = [
  "READ UNCOMMITTED",
  "READ COMMITTED",
  "REPEATABLE READ",
  "SERIALIZABLE",
];

// Test cases

// Lost Update Test
async function testLostUpdate() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Lost Update with isolation level: ${isolationLevel}`
    );

    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      // Set isolation level before starting the transaction
      try {
        await connection1.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection1: ${err.message}`
        );
      }

      try {
        await connection2.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection2: ${err.message}`
        );
      }

      // Start transactions
      try {
        await connection1.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection1: ${err.message}`
        );
      }

      try {
        await connection2.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection2: ${err.message}`
        );
      }

      // Fetch initial balance
      let initialBalance;
      try {
        const [initialRows] = await connection1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        initialBalance = initialRows[0].balance;
        console.log(`Initial balance: ${initialBalance}`);
      } catch (err) {
        console.error(
          `Error fetching initial balance from connection1: ${err.message}`
        );
      }

      // Perform updates
      try {
        await connection1.query(
          "UPDATE users SET balance = balance + 10 WHERE id = 1"
        );
      } catch (err) {
        console.error(`Error updating balance in connection1: ${err.message}`);
      }

      try {
        await connection2.query(
          "UPDATE users SET balance = balance + 20 WHERE id = 1"
        );
      } catch (err) {
        console.error(`Error updating balance in connection2: ${err.message}`);
      }

      // Commit transactions
      try {
        await connection1.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection1: ${err.message}`
        );
      }

      try {
        await connection2.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection2: ${err.message}`
        );
      }

      // Fetch final balance
      let finalBalance;
      try {
        const [finalRows] = await connection1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        finalBalance = finalRows[0].balance;
        console.log(`Final balance after updates: ${finalBalance}`);
      } catch (err) {
        console.error(
          `Error fetching final balance from connection1: ${err.message}`
        );
      }
    } catch (error) {
      console.error(`General error during Lost Update test: ${error.message}`);
    } finally {
      connection1.release();
      connection2.release();
    }
  }
}

// Dirty Read Test
async function testDirtyRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(`\nTesting Dirty Read with isolation level: ${isolationLevel}`);

    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      // Set isolation level before starting the transaction
      try {
        await connection1.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection1: ${err.message}`
        );
      }

      try {
        await connection2.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection2: ${err.message}`
        );
      }

      // Start Transaction 1
      try {
        await connection1.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection1: ${err.message}`
        );
      }

      // Update balance but do not commit
      try {
        await connection1.query("UPDATE users SET balance = 1000 WHERE id = 1");
      } catch (err) {
        console.error(`Error updating balance in connection1: ${err.message}`);
      }

      // Start Transaction 2
      try {
        await connection2.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection2: ${err.message}`
        );
      }

      // Read balance in Transaction 2
      let dirtyReadBalance;
      try {
        const [dirtyReadRows] = await connection2.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        dirtyReadBalance = dirtyReadRows[0].balance;
        console.log(`Dirty read balance: ${dirtyReadBalance}`);
      } catch (err) {
        console.error(`Error reading balance in connection2: ${err.message}`);
      }

      // Rollback Transaction 1
      try {
        await connection1.query("ROLLBACK");
      } catch (err) {
        console.error(
          `Error rolling back transaction in connection1: ${err.message}`
        );
      }

      // Commit Transaction 2
      try {
        await connection2.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection2: ${err.message}`
        );
      }
    } catch (error) {
      console.error(`General error during Dirty Read test: ${error.message}`);
    } finally {
      connection1.release();
      connection2.release();
    }
  }
}

// Non-Repeatable Read Test
async function testNonRepeatableRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Non-Repeatable Read with isolation level: ${isolationLevel}`
    );

    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      // Set isolation level before starting the transaction
      try {
        await connection1.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection1: ${err.message}`
        );
      }

      try {
        await connection2.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection2: ${err.message}`
        );
      }

      // Start Transaction 1
      try {
        await connection1.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection1: ${err.message}`
        );
      }

      // Read balance in Transaction 1
      let initialBalance;
      try {
        const [initialRows] = await connection1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        initialBalance = initialRows[0].balance;
        console.log(`Initial balance in Transaction 1: ${initialBalance}`);
      } catch (err) {
        console.error(
          `Error fetching initial balance from connection1: ${err.message}`
        );
      }

      // Start Transaction 2
      try {
        await connection2.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection2: ${err.message}`
        );
      }

      // Update balance in Transaction 2 and commit
      try {
        await connection2.query(
          "UPDATE users SET balance = balance + 50 WHERE id = 1"
        );
      } catch (err) {
        console.error(`Error updating balance in connection2: ${err.message}`);
      }

      try {
        await connection2.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection2: ${err.message}`
        );
      }

      // Re-read balance in Transaction 1
      let nonRepeatableBalance;
      try {
        const [nonRepeatableRows] = await connection1.query(
          "SELECT balance FROM users WHERE id = 1"
        );
        nonRepeatableBalance = nonRepeatableRows[0].balance;
        console.log(`Balance after concurrent update: ${nonRepeatableBalance}`);
      } catch (err) {
        console.error(
          `Error fetching non-repeatable balance from connection1: ${err.message}`
        );
      }

      // Commit Transaction 1
      try {
        await connection1.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection1: ${err.message}`
        );
      }
    } catch (error) {
      console.error(
        `General error during Non-Repeatable Read test: ${error.message}`
      );
    } finally {
      connection1.release();
      connection2.release();
    }
  }
}

// Phantom Read Test
async function testPhantomRead() {
  for (const isolationLevel of isolationLevels) {
    console.log(
      `\nTesting Phantom Read with isolation level: ${isolationLevel}`
    );

    const connection1 = await mysqlConnection.getConnection();
    const connection2 = await mysqlConnection.getConnection();

    try {
      // Set isolation level before starting the transaction
      try {
        await connection1.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection1: ${err.message}`
        );
      }

      try {
        await connection2.query(
          `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`
        );
      } catch (err) {
        console.error(
          `Error setting isolation level for connection2: ${err.message}`
        );
      }

      // Start Transaction 1
      try {
        await connection1.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection1: ${err.message}`
        );
      }

      // Get initial row count in Transaction 1
      let initialCount;
      try {
        const [initialRows] = await connection1.query(
          "SELECT COUNT(*) AS count FROM users"
        );
        initialCount = initialRows[0].count;
        console.log(`Initial row count in Transaction 1: ${initialCount}`);
      } catch (err) {
        console.error(
          `Error fetching initial row count from connection1: ${err.message}`
        );
      }

      // Start Transaction 2
      try {
        await connection2.query("START TRANSACTION");
      } catch (err) {
        console.error(
          `Error starting transaction for connection2: ${err.message}`
        );
      }

      // Insert new row in Transaction 2 and commit
      try {
        await connection2.query(
          "INSERT INTO users (name, balance) VALUES ('Charlie', 300)"
        );
      } catch (err) {
        console.error(`Error inserting row in connection2: ${err.message}`);
      }

      try {
        await connection2.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection2: ${err.message}`
        );
      }

      // Re-query row count in Transaction 1
      let phantomCount;
      try {
        const [phantomRows] = await connection1.query(
          "SELECT COUNT(*) AS count FROM users"
        );
        phantomCount = phantomRows[0].count;
        console.log(`Row count after concurrent insert: ${phantomCount}`);
      } catch (err) {
        console.error(
          `Error fetching phantom row count from connection1: ${err.message}`
        );
      }

      // Commit Transaction 1
      try {
        await connection1.query("COMMIT");
      } catch (err) {
        console.error(
          `Error committing transaction in connection1: ${err.message}`
        );
      }
    } catch (error) {
      console.error(`General error during Phantom Read test: ${error.message}`);
    } finally {
      connection1.release();
      connection2.release();
    }
  }
}

module.exports = {
  testLostUpdate,
  testDirtyRead,
  testNonRepeatableRead,
  testPhantomRead,
};
