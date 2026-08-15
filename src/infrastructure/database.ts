import pg from 'pg';

const { Pool } = pg;

export function createPool(connectionString = databaseUrl()): pg.Pool {
  return new Pool({
    connectionString,
    max: 25,
    connectionTimeoutMillis: 5_000,
  });
}

export function databaseUrl(): string {
  return process.env.DATABASE_URL ?? 'postgres://assessment:assessment@127.0.0.1:55432/signal_assessment';
}
