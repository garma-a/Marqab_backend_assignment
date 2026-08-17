export type SignalEvent = {
  tenantId: string;
  scopeId: string;
  sourceId: string;
  signalCode: string;
  eventId: string;
  observedAt: string;
  receivedAt: string;
  value: number;
};

export type RuleOperator = 'GT' | 'GTE';
export type DurationComparator = 'GT' | 'GTE';
export type LatenessStatus = 'ON_TIME' | 'LATE' | 'TOO_LATE';

export type RuleDefinition = {
  ruleId: string;
  version: number;
  operator: RuleOperator;
  threshold: number;
  requiredDurationMs: number;
  durationComparator: DurationComparator;
  maxGapMs: number;
  allowedLatenessMs: number;
};

export const RULE_ALPHA: Readonly<RuleDefinition> = Object.freeze({
  ruleId: 'RULE_ALPHA',
  version: 1,
  operator: 'GT',
  threshold: 70,
  requiredDurationMs: 180_000,
  durationComparator: 'GT',
  maxGapMs: 60_000,
  allowedLatenessMs: 300_000,
});

export type DecisionOutcome = 'UNCONFIRMED' | 'CONFIRMED';

export type TemporalEvaluation = {
  outcome: DecisionOutcome;
  inputEventIds: string[];
  windowStartedAt: string | null;
  windowEndedAt: string | null;
  durationMs: number;
};

export type IngestStatus = 'ACCEPTED' | 'DUPLICATE' | 'TOO_LATE';

export type IngestResult = {
  status: IngestStatus;
  eventRowId: string;
  decisionId: string | null;
  outcome: DecisionOutcome | null;
};

export type ReplayResult = {
  decisionId: string;
  outcome: DecisionOutcome;
  decisionFingerprint: string;
  matchedStoredDecision: boolean;
};

export type SeriesKey = Pick<SignalEvent, 'tenantId' | 'scopeId' | 'sourceId' | 'signalCode'>;

export function seriesKeyOf(event: SignalEvent): SeriesKey {
  return {
    tenantId: event.tenantId,
    scopeId: event.scopeId,
    sourceId: event.sourceId,
    signalCode: event.signalCode,
  };
}
