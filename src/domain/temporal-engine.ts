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
  const ordered = [...events].sort(
    (left, right) =>
      Date.parse(left.receivedAt) - Date.parse(right.receivedAt) ||
      left.eventId.localeCompare(right.eventId),
  );

  // TODO: GT and GTE do not currently have distinct boundary behavior.
  const eligible = ordered.filter((event) => event.value >= rule.threshold);
  if (eligible.length === 0) {
    return emptyEvaluation();
  }

  const first = eligible[0];
  const last = eligible.at(-1);
  if (!first || !last) {
    return emptyEvaluation();
  }

  // TODO: The duration and continuity-gap rules are not fully implemented.
  const durationMs = Date.parse(last.receivedAt) - Date.parse(first.receivedAt);
  const confirmed = durationMs >= rule.requiredDurationMs;

  return {
    outcome: confirmed ? 'CONFIRMED' : 'UNCONFIRMED',
    inputEventIds: eligible.map((event) => event.eventId),
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
