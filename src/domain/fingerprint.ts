import { createHash } from 'node:crypto';
import type { RuleDefinition, SeriesKey, TemporalEvaluation } from './types.js';

export type FingerprintInput = {
  seriesKey: SeriesKey;
  rule: Pick<RuleDefinition, 'ruleId' | 'version'>;
  evaluation: TemporalEvaluation;
};

/**
 * Produces the starter fingerprint. The candidate must verify that its input
 * is canonical and independent from request-arrival order.
 */
export function decisionFingerprint(input: FingerprintInput): string {
  const canonical = {
    seriesKey: {
      tenantId: input.seriesKey.tenantId,
      scopeId: input.seriesKey.scopeId,
      sourceId: input.seriesKey.sourceId,
      signalCode: input.seriesKey.signalCode,
    },
    rule: {
      ruleId: input.rule.ruleId,
      version: input.rule.version,
    },
    evaluation: {
      outcome: input.evaluation.outcome,
      inputEventIds: [...input.evaluation.inputEventIds].sort(),
      windowStartedAt: input.evaluation.windowStartedAt,
      windowEndedAt: input.evaluation.windowEndedAt,
      durationMs: input.evaluation.durationMs,
    },
  };
  return createHash('sha256').update(JSON.stringify(canonical)).digest('hex');
}
