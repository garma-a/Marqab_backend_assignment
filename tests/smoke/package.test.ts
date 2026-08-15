import { describe, expect, it } from 'vitest';
import { eventContentHash } from '../../src/infrastructure/signal-repository.js';
import { evaluateTemporalSeries } from '../../src/domain/temporal-engine.js';
import { RULE_ALPHA } from '../../src/domain/types.js';
import { eventAt } from '../helpers.js';

describe('starter package smoke checks', () => {
  it('loads the temporal engine and accepts an empty series', () => {
    expect(evaluateTemporalSeries([], RULE_ALPHA).outcome).toBe('UNCONFIRMED');
  });

  it('does not treat transport retry time as immutable event content', () => {
    const event = eventAt(0);
    const retry = {
      ...event,
      receivedAt: new Date(Date.parse(event.receivedAt) + 10_000).toISOString(),
    };
    expect(eventContentHash(event)).toBe(eventContentHash(retry));
  });
});
