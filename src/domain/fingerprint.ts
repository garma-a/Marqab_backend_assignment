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
  return createHash('sha256').update(JSON.stringify(input)).digest('hex');
}
