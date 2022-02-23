const { Client } = require("pg");
const { z } = require("zod");
const pgFormat = require("pg-format");

class RotationError extends Error {}
exports.RotationError = RotationError;

exports.handleUpdate = async function (body) {
  const params = z
    .object({
      type: z.literal("update"),
      host: z.string(),
      port: z.number(),
      database: z.string(),
      primaryUser: z.object({
        user: z.string(),
        password: z.string(),
      }),
      rotateUser: z.object({
        user: z.string(),
        newPassword: z.string(),
      }),
    })
    .parse(body);

  const client = new Client({
    host: params.host,
    port: params.port,
    database: params.database,
    user: params.primaryUser.user,
    password: params.primaryUser.password,
  });

  try {
    await client.connect();
  } catch (error) {
    throw new RotationError("Connection failed");
  }

  // pgsql can't parameterize utility queries, unfortunately
  const query = pgFormat("ALTER USER %I WITH PASSWORD %L", params.rotateUser.user, params.rotateUser.newPassword);

  try {
    await client.query(query);
  } catch (error) {
    throw new RotationError("ALTER USER failed");
  }
};

exports.handleTest = async function (body) {
  const params = z
    .object({
      type: z.literal("test"),
      host: z.string(),
      port: z.number(),
      database: z.string(),
      user: z.string(),
      password: z.string(),
    })
    .parse(body);

  const client = new Client({
    host: params.host,
    port: params.port,
    database: params.database,
    user: params.user,
    password: params.password,
  });

  try {
    await client.connect();
  } catch (error) {
    throw new RotationError("Connection failed");
  }

  try {
    await client.query("SELECT NOW() as now");
  } catch (error) {
    throw new RotationError("test SELECT failed");
  }
};
