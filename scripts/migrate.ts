import { readdir, readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { createPool } from '../src/infrastructure/database.js';

const migrationsDirectory = resolve(process.cwd(), 'migrations');
const pool = createPool();

try {
  await pool.query(`
    create table if not exists schema_migrations (
      filename text primary key,
      applied_at timestamptz not null default clock_timestamp()
    )
  `);

  const files = (await readdir(migrationsDirectory))
    .filter((file) => /^\d+.*\.sql$/.test(file))
    .sort((left, right) => left.localeCompare(right));

  for (const filename of files) {
    const alreadyApplied = await pool.query(
      'select 1 from schema_migrations where filename = $1',
      [filename],
    );
    if ((alreadyApplied.rowCount ?? 0) > 0) {
      continue;
    }

    const sql = await readFile(resolve(migrationsDirectory, filename), 'utf8');
    const client = await pool.connect();
    try {
      await client.query('begin');
      await client.query(sql);
      await client.query('insert into schema_migrations (filename) values ($1)', [filename]);
      await client.query('commit');
      process.stdout.write(`applied ${filename}\n`);
    } catch (error) {
      await client.query('rollback');
      throw error;
    } finally {
      client.release();
    }
  }
} finally {
  await pool.end();
}
