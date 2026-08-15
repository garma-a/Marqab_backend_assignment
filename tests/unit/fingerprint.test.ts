import { describe, expect, it } from 'vitest';
import { decisionFingerprint } from '../../src/domain/fingerprint.js';
import { RULE_ALPHA } from '../../src/domain/types.js';

const base = {
  seriesKey: {
    tenantId: 'tenant-a',
    scopeId: 'scope-a',
    sourceId: 'source-a',
    signalCode: 'signal-a',
  },
  rule: RULE_ALPHA,
  evaluation: {
    outcome: 'CONFIRMED' as const,
    inputEventIds: ['event-a', 'event-b'],
    windowStartedAt: '2030-01-01T00:00:00.000Z',
    windowEndedAt: '2030-01-01T00:03:01.000Z',
    durationMs: 181_000,
  },
};

describe('decisionFingerprint', () => {
  it('is stable for the same logical evidence regardless of input array order', () => {
    const permuted = {
      ...base,
      evaluation: {
        ...base.evaluation,
        inputEventIds: ['event-b', 'event-a'],
      },
    };
    expect(decisionFingerprint(base)).toBe(decisionFingerprint(permuted));
  });

  it('is a 64-character lowercase hexadecimal SHA-256 value', () => {
    expect(decisionFingerprint(base)).toMatch(/^[0-9a-f]{64}$/);
  });

  it.each([
    ['event evidence', { ...base, evaluation: { ...base.evaluation, inputEventIds: ['event-a', 'event-c'] } }],
    ['rule version', { ...base, rule: { ...base.rule, version: 2 } }],
    ['outcome', { ...base, evaluation: { ...base.evaluation, outcome: 'UNCONFIRMED' as const } }],
    ['series key', { ...base, seriesKey: { ...base.seriesKey, sourceId: 'source-b' } }],
  ])('changes when %s changes', (_label, changed) => {
    expect(decisionFingerprint(changed)).not.toBe(decisionFingerprint(base));
  });
});
