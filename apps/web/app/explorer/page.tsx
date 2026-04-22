import CandidateCard from "@/components/CandidateCard";
import { getMarketExplorer } from "@/lib/api";
import { formatNumber } from "@/lib/format";
import { mapParticipantFlags } from "@/lib/marketNarrative";

type ExplorerMarket = {
  market_id: string;
  question?: string | null;
  title?: string | null;
  category?: string | null;
  url?: string | null;

  structural_day?: string | null;
  social_day?: string | null;
  alignment_day?: string | null;

  structural_score?: number | null;
  structural_state?: string | null;

  social_score?: number | null;
  social_state?: string | null;

  alignment_state?: string | null;
  contextual_summary?: string | null;

  participation_quality_score?: number | null;
  liquidity_durability_score?: number | null;
  concentration_hhi?: number | null;

  intervention_needed?: boolean | null;
  recommended_action?: string | null;
  action_priority?: string | null;
  action_reason?: string | null;
  incentive_dependency?: string | null;
  activity_quality?: string | null;
  intervention_intelligence?: {
    expected_failure_mode?: string | null;
  } | null;

  neutral_share?: number | null;
  whale_share?: number | null;
  speculative_share?: number | null;

  flags?: string[] | null;
};

type MarketExplorerResponse = {
  day?: string | null;
  rows?: ExplorerMarket[];
};

function getDisplayTitle(item: ExplorerMarket): string {
  return item.question || item.title || item.market_id;
}

function triagePriorityScore(m: ExplorerMarket): number {
  let s = 0;

  if (m.intervention_needed === true) s += 100;

  if (m.action_priority === "high") s += 40;
  if (m.action_priority === "medium") s += 20;
  if (m.action_priority === "low") s += 5;

  if (m.alignment_state === "conviction_mismatch") s += 50;
  if (m.alignment_state === "weak") s += 20;

  if (m.structural_state === "weak") s += 30;
  if (m.structural_state === "moderate") s += 10;

  if (m.social_state === "strong") s += 20;
  if (m.social_state === "established") s += 15;
  if (m.social_state === "building") s += 8;

  if (m.activity_quality === "distorted") s += 40;
  if (m.activity_quality === "supported") s += 10;

  if (m.incentive_dependency === "high") s += 25;
  if (m.incentive_dependency === "moderate") s += 10;

  if (
    m.intervention_intelligence?.expected_failure_mode === "one_sided_liquidity"
  ) {
    s += 20;
  }

  if (
    m.intervention_intelligence?.expected_failure_mode === "mercenary_capital"
  ) {
    s += 25;
  }

  if (
    m.intervention_intelligence?.expected_failure_mode === "fake_volume"
  ) {
    s += 20;
  }

  return s;
}

export default async function HomePage() {
  let pageError: string | null = null;

  let markets: Array<
    ExplorerMarket & {
      structuralState: string | null;
      socialState: string | null;
      alignmentState: string | null;
      summary: string | null;
      hasContextualSummary: boolean;
      displayTitle: string;
    }
  > = [];

  try {
    const explorer = (await getMarketExplorer(30)) as MarketExplorerResponse;

    markets = (explorer.rows ?? []).map((item) => {
      const structuralState = item.structural_state ?? null;
      const socialState = item.social_state ?? null;
      const alignmentState = item.alignment_state ?? null;
      const summary = item.contextual_summary ?? null;
      const hasContextualSummary = Boolean(summary);

      return {
        ...item,
        structuralState,
        socialState,
        alignmentState,
        summary,
        hasContextualSummary,
        displayTitle: getDisplayTitle(item),
      };
    });
  } catch (err) {
    pageError =
      err instanceof Error ? err.message : "Failed to load market explorer";
  }

  return (
    <div className="mx-auto max-w-7xl space-y-8 px-4 sm:px-6">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">
          Prediction Market Intelligence
        </p>

        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          Markets ranked across market structure and external demand signals
        </h1>

        <p className="mt-2 max-w-3xl text-sm text-zinc-600">
          A real-time intelligence view for evaluating market structure, external demand, and alignment. 
          The markets displayed here are a random sample for demonstration purposes.
          <br />
          <span className="italic">
            Click “View detail” on any market to explore deeper structural, demand, and alignment diagnostics.
          </span>
        </p>
      </section>

      {pageError ? (
        <section className="rounded-2xl border border-red-200 bg-red-50 p-5 text-red-700 shadow-sm">
          {pageError}
        </section>
      ) : markets.length === 0 ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 text-zinc-600 shadow-sm">
          No markets found.
        </section>
      ) : (
        <section className="space-y-10">
          {/* COUNTS */}
          {(() => {
            const counts = {
              confirmed: 0,
              conviction_mismatch: 0,
              structure_led: 0,
              weak: 0,
            };

            const remainingMarkets = markets.filter(
              (m) => m.intervention_needed !== true
            );

            remainingMarkets.forEach((m) => {
              const key = m.alignmentState || "weak";
              if (counts[key as keyof typeof counts] !== undefined) {
                counts[key as keyof typeof counts]++;
              }
            });

            return (
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                {Object.entries(counts).map(([k, v]) => {
                  const sectionId =
                    k === "confirmed"
                      ? "confirmed"
                      : k === "conviction_mismatch"
                      ? "conviction_mismatch"
                      : k === "structure_led"
                      ? "structure_led"
                      : "weak";

                  return (
                    <a
                      key={k}
                      href={`#${sectionId}`}
                      className="rounded-xl border border-zinc-200 bg-white px-4 py-3 shadow-sm transition hover:bg-zinc-50"
                    >
                      <div className="text-sm font-semibold text-zinc-900">
                        {k === "confirmed" && "Confirmed"}
                        {k === "conviction_mismatch" && "Demand ahead of structure"}
                        {k === "structure_led" && "Structure ahead of demand"}
                        {k === "weak" && "Weak"}
                      </div>
                      <div className="mt-1 text-sm text-zinc-500">{v} markets</div>
                    </a>
                  );
                })}
              </div>
            );
          })()}

          {/* GROUPED SECTIONS */}
          {(() => {
            const order = [
              "confirmed",
              "conviction_mismatch",
              "structure_led",
              "weak",
            ];

            const remainingMarkets = markets.filter(
              (m) => m.intervention_needed !== true
            );

            const grouped: Record<string, typeof markets> = {};

            order.forEach((k) => (grouped[k] = []));

            remainingMarkets.forEach((m) => {
              const key = m.alignmentState || "weak";
              if (!grouped[key]) grouped[key] = [];
              grouped[key].push(m);
            });

            Object.keys(grouped).forEach((k) => {
              grouped[k].sort((a, b) => {
                return (
                  triagePriorityScore(b) - triagePriorityScore(a) ||
                  (b.structural_score ?? 0) - (a.structural_score ?? 0) ||
                  (b.social_score ?? 0) - (a.social_score ?? 0)
                );
              });
            });

            return order.map((groupKey) => {
              const group = grouped[groupKey];
              if (!group || group.length === 0) return null;

              return (
                <div key={groupKey} id={groupKey} className="space-y-4">
                  <div className="space-y-1">
                    <h2 className="text-2xl font-semibold text-zinc-900">
                      {groupKey === "confirmed" && "Confirmed"}
                      {groupKey === "conviction_mismatch" && "Demand ahead of structure"}
                      {groupKey === "structure_led" && "Structure ahead of demand"}
                      {groupKey === "weak" && "Weak"}
                    </h2>

                    <p className="text-sm text-zinc-600">
                      {groupKey === "confirmed" &&
                        "Demand and participation are reinforcing each other."}
                      {groupKey === "conviction_mismatch" &&
                        "Attention is present, but structure or participation is not keeping up."}
                      {groupKey === "structure_led" &&
                        "Structure is present, but demand or activation is lagging."}
                      {groupKey === "weak" &&
                        "These markets show limited signal, weak quality, or low conviction."}
                    </p>
                  </div>

                  <div className="grid gap-8 lg:grid-cols-2">
                    {group.map((item) => (
                      <CandidateCard
                        key={item.market_id}
                        marketId={item.market_id}
                        title={item.displayTitle}
                        category={item.category}
                        structuralState={item.structuralState}
                        socialSignal={item.socialState}
                        alignmentState={item.alignmentState}
                        interventionNeeded={item.intervention_needed}
                        recommendedAction={item.recommended_action}
                        actionPriority={item.action_priority}
                        actionReason={item.action_reason}
                        incentiveDependency={item.incentive_dependency}
                        activityQuality={item.activity_quality}
                        expectedFailureMode={
                          item.intervention_intelligence
                            ?.expected_failure_mode ?? null
                        }
                        hasContextualSummary={item.hasContextualSummary}
                        scoreLabel="Structural quality score"
                        scoreValue={
                          item.structural_score !== null &&
                          item.structural_score !== undefined
                            ? formatNumber(item.structural_score)
                            : "—"
                        }
                        summary={item.summary}
                        flags={mapParticipantFlags({
                          neutralShare: item.neutral_share ?? null,
                          whaleShare: item.whale_share ?? null,
                          speculativeShare: item.speculative_share ?? null,
                          participationQuality:
                            item.participation_quality_score ?? null,
                        })}
                        url={item.url}
                      />
                    ))}
                  </div>
                </div>
              );
            });
          })()}
        </section>
      )}
    </div>
  );
}
