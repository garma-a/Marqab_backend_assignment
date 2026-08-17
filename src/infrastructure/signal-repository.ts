import { createHash } from 'node:crypto';
import type pg from 'pg';
import { decisionFingerprint } from '../domain/fingerprint.js';
import { evaluateTemporalSeries } from '../domain/temporal-engine.js';
import {
  LatenessStatus,
  seriesKeyOf,
  type IngestResult,
  type ReplayResult,
  type RuleDefinition,
  type SignalEvent,
} from '../domain/types.js';

type StoredSignalEvent = SignalEvent & {
  eventRowId: string;
  contentHash: string;
};

export class SignalRepository {
  public constructor(private readonly pool: pg.Pool) {}

  public async publishRule(rule: RuleDefinition): Promise<void> {
    await this.pool.query(
      `insert into rule_versions (rule_id, version, definition, status)
       values ($1, $2, $3::jsonb, 'PUBLISHED')
       on conflict (rule_id, version) do nothing`,
      [rule.ruleId, rule.version, JSON.stringify(rule)],
    );
  }

  public async ingest(event: SignalEvent, rule: RuleDefinition): Promise<IngestResult> {
    const client = await this.pool.connect();
    try {
      await client.query('begin');

      const contentHash = eventContentHash(event);
      const inserted = await client.query<{ id: string }>(
        `insert into signal_events (
           tenant_id, scope_id, source_id, signal_code, event_id,
           observed_at, received_at, value, content_hash
         ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         on conflict (tenant_id, scope_id, source_id, signal_code, event_id)
         do nothing
         returning id`,
        [
          event.tenantId,
          event.scopeId,
          event.sourceId,
          event.signalCode,
          event.eventId,
          event.observedAt,
          event.receivedAt,
          event.value,
          contentHash,
        ],
      );

      if (inserted.rowCount === 0) {
        // TODO: Compare the stored content hash before treating the request as
        // a successful retry. A conflicting payload must not be hidden.
        const existing = await this.findEvent(client, event);
        if (!existing) {
          throw new Error('EVENT_INSERT_CONFLICT_WITHOUT_ROW');
        }
        if (existing.contentHash !== contentHash) {
          throw new Error(
            `EVENT_ID_CONFLICT: eventId ${event.eventId} exists with different immutable content`,
          );
        }
        await client.query('commit');
        return {
          status: 'DUPLICATE',
          eventRowId: existing.eventRowId,
          decisionId: null,
          outcome: null,
        };
      }

      const eventRowId = inserted.rows[0]?.id;
      if (!eventRowId) {
        throw new Error('EVENT_INSERT_DID_NOT_RETURN_ID');
      }
      // i need to make sure the series heads exist before i can insert the events
      await client.query(
        `insert into series_heads (tenant_id, scope_id, source_id, signal_code, watermark_at)
                values ($1, $2, $3, $4, null)
                on conflict (tenant_id, scope_id, source_id, signal_code) do nothing`,
        [event.tenantId, event.scopeId, event.sourceId, event.signalCode],
      );
      // now i need to retrieve the watermark timestamp to decide based on it the late and on time status
      const headResult = await client.query<{ watermark_at: Date | null }>(
        `select watermark_at from series_heads
                where tenant_id = $1 and scope_id = $2 and source_id = $3 and signal_code = $4
                for update`,
        [event.tenantId, event.scopeId, event.sourceId, event.signalCode],
      );
      const watermarkAt = headResult.rows[0]?.watermark_at ?? null;
      const observedMs = Date.parse(event.observedAt);
      let latenessStatus: LatenessStatus = 'ON_TIME';

      if (watermarkAt != null) {
        const watermarkMs = watermarkAt.getTime();
        if (observedMs < watermarkMs) {
          const latenessMs = watermarkMs - observedMs;
          if (latenessMs <= rule.allowedLatenessMs) {
            latenessStatus = 'LATE';
          } else {
            latenessStatus = 'TOO_LATE';
          }
        }
      }
      // update lateness status on the event row
      await client.query(`update signal_events set lateness_status = $1 where id = $2`, [
        latenessStatus,
        eventRowId,
      ]);

      // if TOO_LATE: commit and return early no live state
      if (latenessStatus === 'TOO_LATE') {
        await client.query('commit');
        return {
          status: 'TOO_LATE',
          eventRowId,
          decisionId: null,
          outcome: null,
        };
      }

      // if ON_TIME  update the watermark timestamp to reflect the new timestamp
      if (latenessStatus === 'ON_TIME') {
        await client.query(
          `update series_heads
                  set watermark_at = $1, updated_at = clock_timestamp()
                  where tenant_id = $2 and scope_id = $3 and source_id = $4 and signal_code = $5`,
          [event.observedAt, event.tenantId, event.scopeId, event.sourceId, event.signalCode],
        );
      }

      const seriesEvents = await this.listSeriesEvents(client, event);
      const evaluation = evaluateTemporalSeries(seriesEvents, rule);
      const fingerprint = decisionFingerprint({
        seriesKey: seriesKeyOf(event),
        rule,
        evaluation,
      });

      // i need the decision id to decide if i need to create a correction or not
      const prevState = await client.query<{ decision_id: string }>(
        `select decision_id from current_signal_state
              where tenant_id = $1 and scope_id = $2 and source_id = $3 and signal_code = $4`,
        [event.tenantId, event.scopeId, event.sourceId, event.signalCode],
      );

      const previousDecisionId = prevState.rows[0]?.decision_id ?? null;
      const decisionKind =
        latenessStatus === 'LATE' && previousDecisionId != null ? 'CORRECTION' : 'ORIGINAL';
      const supersedesDecisionId = decisionKind === 'CORRECTION' ? previousDecisionId : null;

      // TODO: Re-evaluation and correction semantics are incomplete. The
      // starter stores every new result as an original decision.
      const decision = await client.query<{ id: string }>(
        `insert into decision_records (
           tenant_id, scope_id, source_id, signal_code,
           rule_id, rule_version, input_event_ids,
           window_started_at, window_ended_at, outcome,
           decision_fingerprint, supersedes_decision_id, decision_kind
         ) values ($1, $2, $3, $4, $5, $6, $7::text[], $8, $9, $10, $11, $12, $13)
         returning id`,
        [
          event.tenantId,
          event.scopeId,
          event.sourceId,
          event.signalCode,
          rule.ruleId,
          rule.version,
          evaluation.inputEventIds,
          evaluation.windowStartedAt,
          evaluation.windowEndedAt,
          evaluation.outcome,
          fingerprint,
          supersedesDecisionId,
          decisionKind,
        ],
      );

      const decisionId = decision.rows[0]?.id;
      if (!decisionId) {
        throw new Error('DECISION_INSERT_DID_NOT_RETURN_ID');
      }

      await client.query(
        `insert into current_signal_state (
           tenant_id, scope_id, source_id, signal_code, decision_id, outcome, updated_at
         ) values ($1, $2, $3, $4, $5, $6, clock_timestamp())
         on conflict (tenant_id, scope_id, source_id, signal_code)
         do update set
           decision_id = excluded.decision_id,
           outcome = excluded.outcome,
           updated_at = excluded.updated_at`,
        [
          event.tenantId,
          event.scopeId,
          event.sourceId,
          event.signalCode,
          decisionId,
          evaluation.outcome,
        ],
      );

      if (evaluation.outcome === 'CONFIRMED') {
        await client.query(
          `insert into command_outbox (decision_id, command_type, payload)
           values ($1, 'STATE_CONFIRMED', $2::jsonb)`,
          [decisionId, JSON.stringify({ outcome: evaluation.outcome })],
        );
      }

      await client.query('commit');
      return {
        status: 'ACCEPTED',
        eventRowId,
        decisionId,
        outcome: evaluation.outcome,
      };
    } catch (error) {
      await client.query('rollback');
      throw error;
    } finally {
      client.release();
    }
  }

  public async replay(decisionId: string): Promise<ReplayResult> {
    const client = await this.pool.connect();
    try {
      // get back the decision record with the series key and the rule definition on that time
      const decResult = await client.query<{
        tenant_id: string;
        scope_id: string;
        source_id: string;
        signal_code: string;
        rule_id: string;
        rule_version: number;
        input_event_ids: string[];
        outcome: 'UNCONFIRMED' | 'CONFIRMED';
        decision_fingerprint: string;
        window_ended_at: Date | null;
      }>(
        `select tenant_id, scope_id, source_id, signal_code,
                       rule_id, rule_version, input_event_ids,
                       outcome, decision_fingerprint, window_ended_at
                from decision_records
                where id = $1`,
        [decisionId],
      );
      const storedDecision = decResult.rows[0];
      if (!storedDecision) {
        throw new Error(`DECISION_NOT_FOUND: ${decisionId}`);
      }
      // i need the rule that was applied on the time of the decision
      const ruleResult = await client.query<{ definition: RuleDefinition }>(
        `select definition from rule_versions
                where rule_id = $1 and version = $2`,
        [storedDecision.rule_id, storedDecision.rule_version],
      );

      const historicalRule = ruleResult.rows[0]?.definition;
      if (!historicalRule) {
        throw new Error(
          `RULE_VERSION_NOT_FOUND: ${storedDecision.rule_id} v${storedDecision.rule_version}`,
        );
      }

      // i need the events that were stored for this series with input_event_ids
      const eventsResult = await client.query<{
        tenant_id: string;
        scope_id: string;
        source_id: string;
        signal_code: string;
        event_id: string;
        observed_at: Date;
        received_at: Date;
        value: string;
      }>(
        `select tenant_id, scope_id, source_id, signal_code, event_id,
                       observed_at, received_at, value
                from signal_events
                where tenant_id = $1 and scope_id = $2 and source_id = $3 and signal_code = $4
                  and event_id = any($5::text[])  
                order by observed_at asc, event_id asc`,
        [
          storedDecision.tenant_id,
          storedDecision.scope_id,
          storedDecision.source_id,
          storedDecision.signal_code,
          storedDecision.input_event_ids,
        ],
      );
      const events: SignalEvent[] = eventsResult.rows.map((row) => ({
        tenantId: row.tenant_id,
        scopeId: row.scope_id,
        sourceId: row.source_id,
        signalCode: row.signal_code,
        eventId: row.event_id,
        observedAt: row.observed_at.toISOString(),
        receivedAt: row.received_at.toISOString(),
        value: Number(row.value),
      }));

      // recompute the evaluation
      const evaluation = evaluateTemporalSeries(events, historicalRule);

      // compute the canonical fingerprint
      const computedFingerprint = decisionFingerprint({
        seriesKey: {
          tenantId: storedDecision.tenant_id,
          scopeId: storedDecision.scope_id,
          sourceId: storedDecision.source_id,
          signalCode: storedDecision.signal_code,
        },
        rule: historicalRule,
        evaluation,
      });
      return {
        decisionId,
        outcome: evaluation.outcome,
        decisionFingerprint: computedFingerprint,
        matchedStoredDecision:
          evaluation.outcome === storedDecision.outcome &&
          computedFingerprint === storedDecision.decision_fingerprint,
      };
    } finally {
      client.release();
    }
  }

  private async findEvent(
    client: pg.PoolClient,
    event: SignalEvent,
  ): Promise<StoredSignalEvent | null> {
    const result = await client.query<{
      id: string;
      tenant_id: string;
      scope_id: string;
      source_id: string;
      signal_code: string;
      event_id: string;
      observed_at: Date;
      received_at: Date;
      value: string;
      content_hash: string;
    }>(
      `select id, tenant_id, scope_id, source_id, signal_code, event_id,
              observed_at, received_at, value, content_hash
       from signal_events
       where tenant_id = $1 and scope_id = $2 and source_id = $3
         and signal_code = $4 and event_id = $5`,
      [event.tenantId, event.scopeId, event.sourceId, event.signalCode, event.eventId],
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }
    return {
      eventRowId: row.id,
      tenantId: row.tenant_id,
      scopeId: row.scope_id,
      sourceId: row.source_id,
      signalCode: row.signal_code,
      eventId: row.event_id,
      observedAt: row.observed_at.toISOString(),
      receivedAt: row.received_at.toISOString(),
      value: Number(row.value),
      contentHash: row.content_hash,
    };
  }

  private async listSeriesEvents(
    client: pg.PoolClient,
    event: SignalEvent,
  ): Promise<SignalEvent[]> {
    const result = await client.query<{
      tenant_id: string;
      scope_id: string;
      source_id: string;
      signal_code: string;
      event_id: string;
      observed_at: Date;
      received_at: Date;
      value: string;
    }>(
      `select tenant_id, scope_id, source_id, signal_code, event_id,
              observed_at, received_at, value
       from signal_events
       where tenant_id = $1 and scope_id = $2 and source_id = $3 and signal_code = $4
and lateness_status in ('ON_TIME', 'LATE')
       order by observed_at asc, event_id asc`,
      [event.tenantId, event.scopeId, event.sourceId, event.signalCode],
    );

    return result.rows.map((row) => ({
      tenantId: row.tenant_id,
      scopeId: row.scope_id,
      sourceId: row.source_id,
      signalCode: row.signal_code,
      eventId: row.event_id,
      observedAt: row.observed_at.toISOString(),
      receivedAt: row.received_at.toISOString(),
      value: Number(row.value),
    }));
  }
}

export function eventContentHash(event: SignalEvent): string {
  const content = {
    tenantId: event.tenantId,
    scopeId: event.scopeId,
    sourceId: event.sourceId,
    signalCode: event.signalCode,
    eventId: event.eventId,
    observedAt: new Date(event.observedAt).toISOString(),
    value: event.value,
  };
  return createHash('sha256').update(JSON.stringify(content)).digest('hex');
}
