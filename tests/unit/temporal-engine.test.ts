import { describe, expect, it } from 'vitest';
import { evaluateTemporalSeries } from '../../src/domain/temporal-engine.js';
import { RULE_ALPHA } from '../../src/domain/types.js';
import { eventAt } from '../helpers.js';

describe('evaluateTemporalSeries', () => {
  it('returns an empty unconfirmed result for no events', () => {
    expect(evaluateTemporalSeries([], RULE_ALPHA)).toEqual({
      outcome: 'UNCONFIRMED',
      inputEventIds: [],
      windowStartedAt: null,
      windowEndedAt: null,
      durationMs: 0,
    });
  });

  it('does not confirm at exactly 180 seconds when the duration comparator is GT', () => {
    const result = evaluateTemporalSeries(
      [eventAt(0), eventAt(60_000), eventAt(120_000), eventAt(180_000)],
      RULE_ALPHA,
    );
    expect(result.durationMs).toBe(180_000);
    expect(result.outcome).toBe('UNCONFIRMED');
  });

  it('confirms once the eligible continuous window exceeds 180 seconds', () => {
    const result = evaluateTemporalSeries(
      [eventAt(0), eventAt(60_000), eventAt(120_000), eventAt(180_000), eventAt(181_000)],
      RULE_ALPHA,
    );
    expect(result.durationMs).toBe(181_000);
    expect(result.outcome).toBe('CONFIRMED');
  });

  it('treats value 70 as ineligible and as a break in the current streak', () => {
    const result = evaluateTemporalSeries(
      [
        eventAt(0),
        eventAt(60_000),
        eventAt(119_000),
        eventAt(120_000, 70),
        eventAt(179_000),
        eventAt(181_000),
      ],
      RULE_ALPHA,
    );
    expect(result.outcome).toBe('UNCONFIRMED');
    expect(result.inputEventIds).not.toContain(eventAt(120_000, 70).eventId);
  });

  it('keeps a gap of exactly 60 seconds connected', () => {
    const result = evaluateTemporalSeries(
      [eventAt(0), eventAt(60_000), eventAt(120_000), eventAt(180_000), eventAt(181_000)],
      RULE_ALPHA,
    );
    expect(result.outcome).toBe('CONFIRMED');
  });

  it('breaks the streak when a gap is 60.001 seconds', () => {
    const result = evaluateTemporalSeries(
      [eventAt(0), eventAt(60_000), eventAt(120_001), eventAt(180_001), eventAt(181_001)],
      RULE_ALPHA,
    );
    expect(result.outcome).toBe('UNCONFIRMED');
  });

  it('uses observedAt and remains deterministic when arrival order differs', () => {
    const chronological = [
      eventAt(0, 71, { receivedAt: eventAt(400_000).receivedAt }),
      eventAt(60_000, 71, { receivedAt: eventAt(300_000).receivedAt }),
      eventAt(120_000, 71, { receivedAt: eventAt(200_000).receivedAt }),
      eventAt(180_000, 71, { receivedAt: eventAt(100_000).receivedAt }),
      eventAt(181_000, 71, { receivedAt: eventAt(0).receivedAt }),
    ];
    const events = [chronological[2]!, chronological[4]!, chronological[0]!, chronological[3]!, chronological[1]!];
    const result = evaluateTemporalSeries(events, RULE_ALPHA);
    expect(result.outcome).toBe('CONFIRMED');
    expect(result.inputEventIds).toEqual(chronological.map((event) => event.eventId));
    expect(result.windowStartedAt).toBe(eventAt(0).observedAt);
    expect(result.windowEndedAt).toBe(eventAt(181_000).observedAt);
  });

  it('uses eventId as the deterministic tie-breaker when observedAt is equal', () => {
    const sameTime = eventAt(0, 71, { eventId: 'z-event' });
    const result = evaluateTemporalSeries(
      [{ ...sameTime, eventId: 'z-event' }, { ...sameTime, eventId: 'a-event' }],
      RULE_ALPHA,
    );
    expect(result.inputEventIds).toEqual(['a-event', 'z-event']);
  });

  it('takes every boundary from a different rule definition instead of hard-coding RULE_ALPHA', () => {
    const alternateRule = {
      ...RULE_ALPHA,
      ruleId: 'RULE_BETA',
      version: 3,
      operator: 'GTE' as const,
      threshold: 5,
      requiredDurationMs: 1_000,
      durationComparator: 'GTE' as const,
      maxGapMs: 500,
      allowedLatenessMs: 10_000,
    };
    const connected = [eventAt(0, 5), eventAt(500, 5), eventAt(1_000, 5)];
    expect(evaluateTemporalSeries(connected, alternateRule)).toMatchObject({
      durationMs: 1_000,
      outcome: 'CONFIRMED',
    });

    const brokenByGap = [eventAt(0, 5), eventAt(500, 5), eventAt(1_001, 5)];
    expect(evaluateTemporalSeries(brokenByGap, alternateRule).outcome).toBe('UNCONFIRMED');

    const brokenByValue = [eventAt(0, 5), eventAt(500, 5), eventAt(501, 4.999), eventAt(1_000, 5)];
    expect(evaluateTemporalSeries(brokenByValue, alternateRule).outcome).toBe('UNCONFIRMED');
  });
});
