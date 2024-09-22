require("dotenv").config();
const fastify = require("fastify")({ logger: true });
const perconaTests = require("./db/percona");
const postgresTests = require("./db/postgres");

// MySQL (Percona) Routes
fastify.get("/mysql/lost-update", async (_, reply) => {
  await perconaTests.testLostUpdate();
  reply.send({ status: "MySQL Lost update scenario completed" });
});

fastify.get("/mysql/dirty-read", async (_, reply) => {
  await perconaTests.testDirtyRead();
  reply.send({ status: "MySQL Dirty read scenario completed" });
});

fastify.get("/mysql/non-repeatable-read", async (_, reply) => {
  await perconaTests.testNonRepeatableRead();
  reply.send({ status: "MySQL Non-repeatable read scenario completed" });
});

fastify.get("/mysql/phantom-read", async (_, reply) => {
  await perconaTests.testPhantomRead();
  reply.send({ status: "MySQL Phantom read scenario completed" });
});

// PostgreSQL Routes
fastify.get("/postgres/lost-update", async (_, reply) => {
  await postgresTests.testLostUpdate();
  reply.send({ status: "PostgreSQL Lost update scenario completed" });
});

fastify.get("/postgres/dirty-read", async (_, reply) => {
  await postgresTests.testDirtyRead();
  reply.send({ status: "PostgreSQL Dirty read scenario completed" });
});

fastify.get("/postgres/non-repeatable-read", async (_, reply) => {
  await postgresTests.testNonRepeatableRead();
  reply.send({ status: "PostgreSQL Non-repeatable read scenario completed" });
});

fastify.get("/postgres/phantom-read", async (_, reply) => {
  await postgresTests.testPhantomRead();
  reply.send({ status: "PostgreSQL Phantom read scenario completed" });
});

// Start Fastify server
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
