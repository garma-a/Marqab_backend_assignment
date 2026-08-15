import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SignalService } from '../../src/application/signal-service.js';
import { RULE_ALPHA, type RuleDefinition } from '../../src/domain/types.js';
import { createPool } from '../../src/infrastructure/database.js';
import { SignalRepository } from '../../src/infrastructure/signal-repository.js';
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

describe('historical replay', () => {
  it('reproduces the stored decision with its historical rule and creates no side effects', async () => {
    let decisionId: string | null = null;
    for (const offset of [0, 60_000, 120_000, 180_000, 181_000]) {
      const result = await service.ingest(eventAt(offset), RULE_ALPHA);
      decisionId = result.decisionId;
    }
    expect(decisionId).not.toBeNull();

    const stored = await pool.query<{
      outcome: 'UNCONFIRMED' | 'CONFIRMED';
      decision_fingerprint: string;
    }>(
      'select outcome, decision_fingerprint from decision_records where id = $1',
      [decisionId],
    );
    const countsBefore = await tableCounts();

    const versionTwo: RuleDefinition = {
      ...RULE_ALPHA,
      version: 2,
      threshold: 90,
    };
    await service.publishRule(versionTwo);

    const firstReplay = await service.replay(decisionId as string);
    const secondReplay = await service.replay(decisionId as string);
    expect(firstReplay).toEqual(secondReplay);
    expect(firstReplay).toMatchObject({
      decisionId,
      outcome: stored.rows[0]?.outcome,
      decisionFingerprint: stored.rows[0]?.decision_fingerprint,
      matchedStoredDecision: true,
    });

    const countsAfter = await tableCounts();
    expect(countsAfter).toEqual({
      events: countsBefore.events,
      decisions: countsBefore.decisions,
      outbox: countsBefore.outbox,
    });
  });

  it('recomputes evidence instead of echoing a stored outcome and fingerprint', async () => {
    const offsets = [0, 60_000, 120_000, 180_000, 181_000];
    for (const offset of offsets) {
      await service.ingest(eventAt(offset), RULE_ALPHA);
    }

    const fakeFingerprint = '0'.repeat(64);
    const fabricated = await pool.query<{ id: string }>(
      `insert into decision_records (
         tenant_id, scope_id, source_id, signal_code,
         rule_id, rule_version, input_event_ids,
         window_started_at, window_ended_at, outcome,
         decision_fingerprint, supersedes_decision_id, decision_kind
       ) values ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9, $10, $11, null, 'ORIGINAL')
       returning id`,
      [
        'tenant-a',
        'scope-a',
        'source-a',
        'signal-a',
        RULE_ALPHA.ruleId,
        RULE_ALPHA.version,
        offsets.map((offset) => eventAt(offset).eventId),
        eventAt(0).observedAt,
        eventAt(181_000).observedAt,
        'UNCONFIRMED',
        fakeFingerprint,
      ],
    );
    const fabricatedId = fabricated.rows[0]?.id;
    expect(fabricatedId).toBeTruthy();
    const countsBefore = await tableCounts();

    const replay = await service.replay(fabricatedId as string);
    expect(replay).toMatchObject({
      decisionId: fabricatedId,
      outcome: 'CONFIRMED',
      matchedStoredDecision: false,
    });
    expect(replay.decisionFingerprint).not.toBe(fakeFingerprint);
    expect(replay.decisionFingerprint).toMatch(/^[0-9a-f]{64}$/);
    expect(await tableCounts()).toEqual(countsBefore);
  });
});

async function tableCounts(): Promise<{ events: string; decisions: string; outbox: string }> {
  const result = await pool.query<{ events: string; decisions: string; outbox: string }>(`
    select
      (select count(*) from signal_events)::text as events,
      (select count(*) from decision_records)::text as decisions,
      (select count(*) from command_outbox)::text as outbox
  `);
  const row = result.rows[0];
  if (!row) {
    throw new Error('COUNT_QUERY_RETURNED_NO_ROW');
  }
  return row;
}
