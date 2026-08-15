import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createPool } from '../../src/infrastructure/database.js';
import { SignalRepository } from '../../src/infrastructure/signal-repository.js';
import { SignalService } from '../../src/application/signal-service.js';
import { RULE_ALPHA } from '../../src/domain/types.js';
import { eventAt, truncateAssessmentTables } from '../helpers.js';

const pool = createPool();
const service = new SignalService(new SignalRepository(pool));

beforeAll(async () => {
  await pool.query('select 1');
});

beforeEach(async () => {
  await truncateAssessmentTables(pool);
  await service.publishRule(RULE_ALPHA);
});

afterAll(async () => {
  await pool.end();
});

describe('PostgreSQL ingestion and idempotency', () => {
  it('stores one ordinary event and creates a current projection', async () => {
    const result = await service.ingest(eventAt(0, 60), RULE_ALPHA);
    expect(result.status).toBe('ACCEPTED');
    expect(result.outcome).toBe('UNCONFIRMED');

    const counts = await pool.query<{
      events: string;
      decisions: string;
      states: string;
    }>(`
      select
        (select count(*) from signal_events)::text as events,
        (select count(*) from decision_records)::text as decisions,
        (select count(*) from current_signal_state)::text as states
    `);
    expect(counts.rows[0]).toEqual({ events: '1', decisions: '1', states: '1' });
  });

  it('treats the same eventId and same content as a successful duplicate', async () => {
    const event = eventAt(0);
    const first = await service.ingest(event, RULE_ALPHA);
    const second = await service.ingest(
      { ...event, receivedAt: new Date(Date.parse(event.receivedAt) + 5_000).toISOString() },
      RULE_ALPHA,
    );

    expect(first.status).toBe('ACCEPTED');
    expect(second.status).toBe('DUPLICATE');
    const count = await pool.query<{ count: string }>('select count(*)::text as count from signal_events');
    expect(count.rows[0]?.count).toBe('1');
  });

  it('rejects the same eventId when immutable content differs', async () => {
    const event = eventAt(0);
    await service.ingest(event, RULE_ALPHA);

    await expect(
      service.ingest({ ...event, value: 999 }, RULE_ALPHA),
    ).rejects.toThrow(/EVENT_ID_CONFLICT/);

    const stored = await pool.query<{ value: string }>('select value from signal_events');
    expect(stored.rows).toEqual([{ value: '71' }]);
  });

  it('handles 20 concurrent retries with one event, one new decision and one confirmed outbox command', async () => {
    for (const offset of [0, 60_000, 120_000, 180_000]) {
      await service.ingest(eventAt(offset), RULE_ALPHA);
    }

    const before = await pool.query<{ decisions: string }>(
      'select count(*)::text as decisions from decision_records',
    );
    const finalEvent = eventAt(181_000);
    const results = await Promise.all(
      Array.from({ length: 20 }, () => service.ingest(finalEvent, RULE_ALPHA)),
    );

    expect(results.filter((result) => result.status === 'ACCEPTED')).toHaveLength(1);
    expect(results.filter((result) => result.status === 'DUPLICATE')).toHaveLength(19);

    const accepted = results.find((result) => result.status === 'ACCEPTED');
    const evidence = await pool.query<{
      rule_id: string;
      rule_version: number;
      input_event_ids: string[];
      window_started_at: Date;
      window_ended_at: Date;
      outcome: string;
      decision_fingerprint: string;
      supersedes_decision_id: string | null;
    }>(
      `select rule_id, rule_version, input_event_ids, window_started_at,
              window_ended_at, outcome, decision_fingerprint, supersedes_decision_id
       from decision_records where id = $1`,
      [accepted?.decisionId],
    );
    expect(evidence.rows[0]).toMatchObject({
      rule_id: RULE_ALPHA.ruleId,
      rule_version: RULE_ALPHA.version,
      input_event_ids: [0, 60_000, 120_000, 180_000, 181_000].map((offset) => eventAt(offset).eventId),
      outcome: 'CONFIRMED',
      supersedes_decision_id: null,
    });
    expect(evidence.rows[0]?.window_started_at.toISOString()).toBe(eventAt(0).observedAt);
    expect(evidence.rows[0]?.window_ended_at.toISOString()).toBe(eventAt(181_000).observedAt);
    expect(evidence.rows[0]?.decision_fingerprint).toMatch(/^[0-9a-f]{64}$/);

    const counts = await pool.query<{
      events: string;
      decisions: string;
      outbox: string;
    }>(`
      select
        (select count(*) from signal_events)::text as events,
        (select count(*) from decision_records)::text as decisions,
        (select count(*) from command_outbox)::text as outbox
    `);
    expect(counts.rows[0]?.events).toBe('5');
    expect(Number(counts.rows[0]?.decisions) - Number(before.rows[0]?.decisions)).toBe(1);
    expect(counts.rows[0]?.outbox).toBe('1');
  });

  it('does not merge otherwise identical events from different series', async () => {
    const first = eventAt(0, 71, { eventId: 'shared-event' });
    const second = { ...first, scopeId: 'scope-b' };

    await service.ingest(first, RULE_ALPHA);
    await service.ingest(second, RULE_ALPHA);

    const grouped = await pool.query<{ scope_id: string; count: string }>(
      `select scope_id, count(*)::text as count
       from signal_events
       group by scope_id
       order by scope_id`,
    );
    expect(grouped.rows).toEqual([
      { scope_id: 'scope-a', count: '1' },
      { scope_id: 'scope-b', count: '1' },
    ]);
  });

  it.each([
    ['scopeId', { scopeId: 'scope-a', sourceId: 'source-a' }, { scopeId: 'scope-b', sourceId: 'source-a' }],
    ['sourceId', { scopeId: 'scope-a', sourceId: 'source-a' }, { scopeId: 'scope-a', sourceId: 'source-b' }],
  ])('keeps durations isolated when only %s differs', async (_field, firstKey, secondKey) => {
    for (const offset of [0, 120_000]) {
      await service.ingest(eventAt(offset, 71, firstKey), RULE_ALPHA);
    }
    for (const offset of [60_000, 180_000, 181_000]) {
      await service.ingest(eventAt(offset, 71, secondKey), RULE_ALPHA);
    }

    const states = await pool.query<{ outcome: string }>(
      `select outcome
       from current_signal_state
       order by scope_id, source_id`,
    );
    expect(states.rows).toHaveLength(2);
    expect(states.rows.every((row) => row.outcome === 'UNCONFIRMED')).toBe(true);
  });
});
