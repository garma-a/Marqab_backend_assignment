import type { SignalRepository } from '../infrastructure/signal-repository.js';
import type { IngestResult, ReplayResult, RuleDefinition, SignalEvent } from '../domain/types.js';

export class SignalService {
  public constructor(private readonly repository: SignalRepository) {}

  public async publishRule(rule: RuleDefinition): Promise<void> {
    validateRule(rule);
    await this.repository.publishRule(rule);
  }

  public async ingest(event: SignalEvent, rule: RuleDefinition): Promise<IngestResult> {
    validateEvent(event);
    validateRule(rule);
    return this.repository.ingest(event, rule);
  }

  public async replay(decisionId: string): Promise<ReplayResult> {
    if (!decisionId.trim()) {
      throw new Error('decisionId is required');
    }
    return this.repository.replay(decisionId);
  }
}

function validateEvent(event: SignalEvent): void {
  for (const [name, value] of Object.entries({
    tenantId: event.tenantId,
    scopeId: event.scopeId,
    sourceId: event.sourceId,
    signalCode: event.signalCode,
    eventId: event.eventId,
  })) {
    if (!value.trim()) {
      throw new Error(`${name} is required`);
    }
  }
  if (!Number.isFinite(event.value)) {
    throw new Error('value must be finite');
  }
  if (
    !Number.isFinite(Date.parse(event.observedAt)) ||
    !Number.isFinite(Date.parse(event.receivedAt))
  ) {
    throw new Error('observedAt and receivedAt must be valid ISO timestamps');
  }
}

function validateRule(rule: RuleDefinition): void {
  if (!rule.ruleId.trim() || !Number.isInteger(rule.version) || rule.version <= 0) {
    throw new Error('ruleId and a positive integer version are required');
  }
  for (const [name, value] of Object.entries({
    threshold: rule.threshold,
    requiredDurationMs: rule.requiredDurationMs,
    maxGapMs: rule.maxGapMs,
    allowedLatenessMs: rule.allowedLatenessMs,
  })) {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error(`${name} must be a non-negative finite number`);
    }
  }
}
