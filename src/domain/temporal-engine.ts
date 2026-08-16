import type { RuleDefinition, SignalEvent, TemporalEvaluation } from './types.js';

/**
 * Starter implementation from the current backend path.
 *
 * This implementation is intentionally incomplete. The assessment requires
 * repairing it in place and proving the corrected behavior with tests.
 */
export function evaluateTemporalSeries(
  events: readonly SignalEvent[],
  rule: RuleDefinition,
): TemporalEvaluation {
  if (events.length === 0) {
    return emptyEvaluation();
  }

  // TODO: This currently follows transport order instead of event time.
  // i just changed the sort process to be based on observedAt instead of recievedAt
  const ordered = [...events].sort(
    (left, right) =>
      Date.parse(left.observedAt) - Date.parse(right.observedAt) ||
      left.eventId.localeCompare(right.eventId),
  );
  // the current for loop handle the descision of the rule operator and the split of the events
  const segments: SignalEvent[][] = [];
  let current: SignalEvent[] = [];

  for (const event of ordered) {
    const eligible =
      rule.operator === 'GTE' ? event.value >= rule.threshold : event.value > rule.threshold;

    const gapMs =
      current.length > 0
        ? Date.parse(event.observedAt) - Date.parse(current.at(-1)!.observedAt)
        : 0;

    const breaksWindow = !eligible || gapMs > rule.maxGapMs;

    if (breaksWindow && current.length > 0) {
      segments.push(current);
      current = [];
    }

    if (eligible) {
      current.push(event);
    }
  }

  if (current.length > 0) segments.push(current);

  if (segments.length === 0) {
    return emptyEvaluation();
  }

  const activeChain = segments.at(-1)!;
  const first = activeChain[0]!;
  const last = activeChain.at(-1)!;
  if (!first || !last) {
    return emptyEvaluation();
  }

  // TODO: The duration and continuity-gap rules are not fully implemented.
  // i just changed the duration to be based on the observedAt instead of recievedAt
  // and the continuity-gap to be based on the observedAt instead of recievedAt
  const durationMs = Date.parse(last.observedAt) - Date.parse(first.observedAt);
  const confirmed =
    rule.durationComparator === 'GTE'
      ? durationMs >= rule.requiredDurationMs
      : durationMs > rule.requiredDurationMs;

  return {
    outcome: confirmed ? 'CONFIRMED' : 'UNCONFIRMED',
    inputEventIds: activeChain.map((event) => event.eventId),
    windowStartedAt: first.observedAt,
    windowEndedAt: last.observedAt,
    durationMs,
  };
}

function emptyEvaluation(): TemporalEvaluation {
  return {
    outcome: 'UNCONFIRMED',
    inputEventIds: [],
    windowStartedAt: null,
    windowEndedAt: null,
    durationMs: 0,
  };
}
