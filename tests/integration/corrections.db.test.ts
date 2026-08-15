import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { SignalService } from '../../src/application/signal-service.js';
import { RULE_ALPHA } from '../../src/domain/types.js';
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

describe('late events and append-only corrections', () => {
  it('a late in-window event can break a previously confirmed streak without rewriting the old decision', async () => {
    for (const offset of [0, 60_000, 120_000, 180_000, 181_000]) {
      await service.ingest(eventAt(offset), RULE_ALPHA);
    }

    const confirmed = await pool.query<{ id: string; outcome: string; decision_fingerprint: string }>(
      `select id, outcome, decision_fingerprint
       from decision_records
       where outcome = 'CONFIRMED'
       order by created_at desc
       limit 1`,
    );
    const confirmedDecision = confirmed.rows[0];
    expect(confirmedDecision?.outcome).toBe('CONFIRMED');

    const lateBreaker = eventAt(150_000, 70, {
      eventId: 'late-breaker',
      receivedAt: eventAt(240_000).receivedAt,
    });
    const correctionResult = await service.ingest(lateBreaker, RULE_ALPHA);
    expect(correctionResult.status).toBe('ACCEPTED');
    expect(correctionResult.outcome).toBe('UNCONFIRMED');

    const lateStatus = await pool.query<{ lateness_status: string }>(
      `select lateness_status from signal_events where event_id = 'late-breaker'`,
    );
    expect(lateStatus.rows[0]?.lateness_status).toBe('LATE');

    const latest = await pool.query<{
      id: string;
      outcome: string;
      decision_kind: string;
      supersedes_decision_id: string | null;
    }>(
      `select id, outcome, decision_kind, supersedes_decision_id
       from decision_records
       where id = $1`,
      [correctionResult.decisionId],
    );
    expect(latest.rows[0]).toMatchObject({
      outcome: 'UNCONFIRMED',
      decision_kind: 'CORRECTION',
      supersedes_decision_id: confirmedDecision?.id,
    });

    const oldDecision = await pool.query<{ outcome: string; decision_fingerprint: string }>(
      'select outcome, decision_fingerprint from decision_records where id = $1',
      [confirmedDecision?.id],
    );
    expect(oldDecision.rows[0]).toEqual({
      outcome: confirmedDecision?.outcome,
      decision_fingerprint: confirmedDecision?.decision_fingerprint,
    });

    const current = await pool.query<{ decision_id: string; outcome: string }>(
      'select decision_id, outcome from current_signal_state',
    );
    expect(current.rows[0]).toMatchObject({
      decision_id: latest.rows[0]?.id,
      outcome: 'UNCONFIRMED',
    });
  });

  it('a late in-window event can bridge a continuity gap and create one confirmed correction', async () => {
    for (const offset of [0, 60_000, 120_001, 180_001, 181_001]) {
      await service.ingest(eventAt(offset), RULE_ALPHA);
    }

    const before = await pool.query<{ decision_id: string; outcome: string }>(
      'select decision_id, outcome from current_signal_state',
    );
    expect(before.rows[0]?.outcome).toBe('UNCONFIRMED');

    const bridge = eventAt(120_000, 71, {
      eventId: 'late-bridge',
      receivedAt: eventAt(240_000).receivedAt,
    });
    const result = await service.ingest(bridge, RULE_ALPHA);
    expect(result).toMatchObject({ status: 'ACCEPTED', outcome: 'CONFIRMED' });

    const storedEvent = await pool.query<{ lateness_status: string }>(
      `select lateness_status from signal_events where event_id = 'late-bridge'`,
    );
    expect(storedEvent.rows[0]?.lateness_status).toBe('LATE');

    const correction = await pool.query<{
      outcome: string;
      decision_kind: string;
      supersedes_decision_id: string | null;
    }>(
      `select outcome, decision_kind, supersedes_decision_id
       from decision_records where id = $1`,
      [result.decisionId],
    );
    expect(correction.rows[0]).toEqual({
      outcome: 'CONFIRMED',
      decision_kind: 'CORRECTION',
      supersedes_decision_id: before.rows[0]?.decision_id,
    });

    const outbox = await pool.query<{ count: string }>(
      'select count(*)::text as count from command_outbox',
    );
    expect(outbox.rows[0]?.count).toBe('1');
  });

  it('accepts a late event at exactly the allowed-lateness boundary', async () => {
    await service.ingest(eventAt(600_000), RULE_ALPHA);
    const boundaryEvent = eventAt(300_000, 71, {
      eventId: 'late-at-boundary',
      receivedAt: eventAt(700_000).receivedAt,
    });

    const result = await service.ingest(boundaryEvent, RULE_ALPHA);
    expect(result.status).toBe('ACCEPTED');
    expect(result.decisionId).not.toBeNull();

    const stored = await pool.query<{ lateness_status: string }>(
      `select lateness_status from signal_events where event_id = 'late-at-boundary'`,
    );
    expect(stored.rows[0]?.lateness_status).toBe('LATE');
  });

  it('stores an event older than allowed lateness but does not mutate live state', async () => {
    await service.ingest(eventAt(600_000), RULE_ALPHA);
    const before = await pool.query<{ decision_id: string }>(
      'select decision_id from current_signal_state',
    );
    const countsBefore = await pool.query<{ decisions: string; outbox: string }>(`
      select
        (select count(*) from decision_records)::text as decisions,
        (select count(*) from command_outbox)::text as outbox
    `);

    const tooLate = eventAt(299_999, 999, {
      eventId: 'too-late-event',
      receivedAt: eventAt(700_000).receivedAt,
    });
    const result = await service.ingest(tooLate, RULE_ALPHA);
    expect(result.status).toBe('TOO_LATE');
    expect(result.decisionId).toBeNull();

    const stored = await pool.query<{ lateness_status: string }>(
      `select lateness_status from signal_events where event_id = 'too-late-event'`,
    );
    expect(stored.rows[0]?.lateness_status).toBe('TOO_LATE');

    const after = await pool.query<{ decision_id: string }>(
      'select decision_id from current_signal_state',
    );
    expect(after.rows[0]?.decision_id).toBe(before.rows[0]?.decision_id);
    const countsAfter = await pool.query<{ decisions: string; outbox: string }>(`
      select
        (select count(*) from decision_records)::text as decisions,
        (select count(*) from command_outbox)::text as outbox
    `);
    expect(countsAfter.rows[0]).toEqual(countsBefore.rows[0]);
  });
});
