import type pg from 'pg';
import type { SignalEvent } from '../src/domain/types.js';

export const BASE_TIME_MS = Date.parse('2030-01-01T00:00:00.000Z');

export function eventAt(
  observedOffsetMs: number,
  value = 71,
  overrides: Partial<SignalEvent> = {},
): SignalEvent {
  const observedAt = new Date(BASE_TIME_MS + observedOffsetMs).toISOString();
  return {
    tenantId: 'tenant-a',
    scopeId: 'scope-a',
    sourceId: 'source-a',
    signalCode: 'signal-a',
    eventId: `event-${observedOffsetMs}-${value}`,
    observedAt,
    receivedAt: observedAt,
    value,
    ...overrides,
  };
}

export async function truncateAssessmentTables(pool: pg.Pool): Promise<void> {
  await pool.query(`
    truncate table
      command_outbox,
      current_signal_state,
      decision_records,
      signal_events,
      series_heads,
      rule_versions
    restart identity cascade
  `);
}
