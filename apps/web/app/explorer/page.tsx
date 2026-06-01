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
  if (m.intervention_intelligence?.expected_failure_mode === "one_sided_liquidity") s += 20;
  if (m.intervention_intelligence?.expected_failure_mode === "mercenary_capital") s += 25;
  if (m.intervention_intelligence?.expected_failure_mode === "fake_volume") s += 20;
  return s;
}

// ─── Maintenance Banner ───────────────────────────────────────────────────────
// Rendered when the data layer is unavailable. Designed to look deliberate
// and professional — appropriate for technical reviewers.

function MaintenanceBanner() {
  const layers = [
    {
      label: "Market Structure Engine",
      desc: "Structural scoring, state classification, and participation quality metrics",
      status: "seeding",
      dot: "bg-amber-400",
    },
    {
      label: "Social Signal Pipeline",
      desc: "External demand indexing and social conviction scoring",
      status: "seeding",
      dot: "bg-amber-400",
    },
    {
      label: "Alignment Diagnostics",
      desc: "Cross-signal alignment computation and intervention triggers",
      status: "seeding",
      dot: "bg-amber-400",
    },
    {
      label: "Liquidity Intelligence",
      desc: "HHI concentration, durability scoring, and CLOB spread proxies",
      status: "queued",
      dot: "bg-zinc-300",
    },
  ];

  const capabilities = [
    "Structural quality scoring across active markets",
    "External demand signal indexing from social corpora",
    "Conviction alignment diagnostics and mismatch detection",
    "Intervention triage with priority ranking and failure mode prediction",
    "Liquidity durability scoring and participant segmentation",
    "Category-level HHI concentration analysis",
  ];

  return (
    <div className="mx-auto max-w-4xl space-y-6 px-4 py-8 sm:px-6">

      {/* Status header */}
      <div className="rounded-2xl border border-zinc-200 bg-white shadow-sm overflow-hidden">
        <div className="border-b border-zinc-100 bg-zinc-50 px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            {/* Animated pulse indicating active work */}
            <span className="relative flex h-2.5 w-2.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-60" />
              <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-amber-500" />
            </span>
            <span className="text-xs font-semibold uppercase tracking-widest text-zinc-500">
              Data Layer · Seeding in Progress
            </span>
          </div>
          <span className="rounded-full bg-amber-50 border border-amber-200 px-3 py-1 text-[10px] font-bold uppercase tracking-widest text-amber-700">
            Pre-Production
          </span>
        </div>

        <div className="px-6 py-6 sm:py-8">
          <h1 className="text-2xl font-semibold text-zinc-900 sm:text-3xl">
            Market Intelligence Explorer
          </h1>
          <p className="mt-2 max-w-2xl text-sm leading-relaxed text-zinc-500">
            A real-time intelligence view for evaluating market structure, external demand, and alignment. 
            The underlying data store is currently being seeded.
          </p>
        </div>
      </div>

      {/* Pipeline layer status */}
      <div className="rounded-2xl border border-zinc-200 bg-white shadow-sm overflow-hidden">
        <div className="border-b border-zinc-100 px-6 py-4">
          <h2 className="text-sm font-semibold text-zinc-700">Pipeline Layers</h2>
          <p className="mt-0.5 text-xs text-zinc-400">Real-time status across intelligence subsystems</p>
        </div>
        <div className="divide-y divide-zinc-50">
          {layers.map((layer, i) => (
            <div key={i} className="flex items-center gap-4 px-6 py-4">
              <div className={`h-2 w-2 rounded-full shrink-0 ${layer.dot}`} />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-zinc-800">{layer.label}</p>
                <p className="text-xs text-zinc-400 mt-0.5 leading-relaxed">{layer.desc}</p>
              </div>
              <span className={`shrink-0 rounded-full px-2.5 py-1 text-[10px] font-bold uppercase tracking-wider
                ${layer.status === "seeding"
                  ? "bg-amber-50 border border-amber-100 text-amber-700"
                  : "bg-zinc-50 border border-zinc-200 text-zinc-500"}`}>
                {layer.status}
              </span>
            </div>
          ))}
        </div>
      </div>

      {/* Two-column: architecture note + capabilities */}
      <div className="grid gap-5 sm:grid-cols-2">

        {/* Architecture note */}
        <div className="rounded-2xl border border-zinc-200 bg-white shadow-sm p-6">
          <h2 className="text-sm font-semibold text-zinc-700 mb-3">Architecture Overview</h2>
          <div className="space-y-3 text-xs text-zinc-500 leading-relaxed">
            <p>
              The explorer is backed by a multi-signal scoring pipeline that 
              continuously indexes live market protocols, scoring each market 
              across structural, demand, and alignment dimensions.
            </p>
            <p>
              Every tracked configuration is systematically evaluated against 
              standardized operational parameters and risk dimensions to establish 
              clear telemetry baselines.
            </p>
            <p>
              The current environment is undergoing a scheduled index synchronization.
            </p>
          </div>
        </div>

        {/* Capabilities */}
        <div className="rounded-2xl border border-zinc-200 bg-white shadow-sm p-6">
          <h2 className="text-sm font-semibold text-zinc-700 mb-3">Capabilities in Scope</h2>
          <ul className="space-y-2">
            {capabilities.map((c, i) => (
              <li key={i} className="flex items-start gap-2.5 text-xs text-zinc-500 leading-relaxed">
                <svg className="mt-0.5 h-3.5 w-3.5 shrink-0 text-zinc-300" fill="none" stroke="currentColor" strokeWidth={2.5} viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                </svg>
                {c}
              </li>
            ))}
          </ul>
        </div>
      </div>


    </div>
  );
}

// ─── Page ─────────────────────────────────────────────────────────────────────

export default async function HomePage() {
  let pageError: string | null = null;
  let isDataUnavailable = false;

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
    pageError = err instanceof Error ? err.message : "Failed to load market explorer";

    // Detect infrastructure-level failures (DB suspended, DNS, connection refused)
    // and route them to the maintenance view instead of showing raw error strings.
    const msg = pageError.toLowerCase();
    if (
      msg.includes("failed to resolve host") ||
      msg.includes("econnrefused") ||
      msg.includes("enotfound") ||
      msg.includes("internal server error") ||
      msg.includes("503") ||
      msg.includes("502") ||
      msg.includes("supabase") ||
      msg.includes("database") ||
      msg.includes("errno")
    ) {
      isDataUnavailable = true;
    }
  }

  // Infrastructure down → render polished maintenance view
  if (isDataUnavailable) {
    return <MaintenanceBanner />;
  }

  return (
    <div className="mx-auto max-w-7xl space-y-8 px-4 sm:px-6">
      <section className="rounded-2xl border border-zinc-200 bg-white p-4 shadow-sm sm:p-6">
        <p className="text-sm font-medium text-zinc-500">
          Prediction Market Intelligence
        </p>

        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          Markets ranked across market structure and external demand signals
        </h1>

        <p className="mt-2 max-w-3xl text-sm text-zinc-600">
          A real-time intelligence view for evaluating market structure, external demand, and alignment.
          The markets displayed here are a random sample for demonstration purposes.
        </p>

        <div className="mt-3 rounded-md border border-blue-100 bg-blue-50 px-3 py-2 text-sm leading-6 text-blue-700 sm:inline-block">
          Click <span className="font-medium">&quot;View detail&quot;</span> on any market to explore deeper structural, demand, and alignment diagnostics.
        </div>
      </section>

      {/* Application-level error (non-infrastructure) — subtle, non-alarming */}
      {pageError && !isDataUnavailable && (
        <section className="rounded-2xl border border-zinc-200 bg-zinc-50 px-5 py-4 shadow-sm">
          <div className="flex items-center gap-2.5">
            <div className="h-2 w-2 rounded-full bg-amber-400 shrink-0" />
            <p className="text-sm font-medium text-zinc-600">
              Market data is temporarily unavailable. Refreshing shortly.
            </p>
          </div>
        </section>
      )}

      {markets.length === 0 && !pageError ? (
        <section className="rounded-2xl border border-zinc-200 bg-white p-5 text-zinc-600 shadow-sm">
          No markets found.
        </section>
      ) : (
        <section className="space-y-8 sm:space-y-10">
          {(() => {
            const counts = {
              confirmed: 0,
              conviction_mismatch: 0,
              structure_led: 0,
              weak: 0,
            };

            markets.forEach((m) => {
              const key = m.alignmentState || "weak";
              if (counts[key as keyof typeof counts] !== undefined) {
                counts[key as keyof typeof counts]++;
              }
            });

            return (
              <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
                {Object.entries(counts).map(([k, v]) => {
                  const sectionId =
                    k === "confirmed" ? "confirmed"
                    : k === "conviction_mismatch" ? "conviction_mismatch"
                    : k === "structure_led" ? "structure_led"
                    : "weak";

                  return (
                    <a
                      key={k}
                      href={`#${sectionId}`}
                      className="rounded-xl border border-zinc-200 bg-white px-3 py-3 shadow-sm transition hover:bg-zinc-50 sm:px-4"
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

          {(() => {
            const order = ["confirmed", "conviction_mismatch", "structure_led", "weak"];
            const grouped: Record<string, typeof markets> = {};
            order.forEach((k) => (grouped[k] = []));

            markets.forEach((m) => {
              const key = m.alignmentState || "weak";
              if (!grouped[key]) grouped[key] = [];
              grouped[key].push(m);
            });

            Object.keys(grouped).forEach((k) => {
              grouped[k].sort((a, b) =>
                triagePriorityScore(b) - triagePriorityScore(a) ||
                (b.structural_score ?? 0) - (a.structural_score ?? 0) ||
                (b.social_score ?? 0) - (a.social_score ?? 0)
              );
            });

            return order.map((groupKey) => {
              const group = grouped[groupKey];
              if (!group || group.length === 0) return null;

              return (
                <div key={groupKey} id={groupKey} className="space-y-3 sm:space-y-4">
                  <div className="space-y-1">
                    <h2 className="text-xl font-semibold text-zinc-900 sm:text-2xl">
                      {groupKey === "confirmed" && "Confirmed"}
                      {groupKey === "conviction_mismatch" && "Demand ahead of structure"}
                      {groupKey === "structure_led" && "Structure ahead of demand"}
                      {groupKey === "weak" && "Weak"}
                    </h2>
                    <p className="text-sm leading-6 text-zinc-600">
                      {groupKey === "confirmed" && "Demand and participation are reinforcing each other."}
                      {groupKey === "conviction_mismatch" && "Attention is present, but structure or participation is not keeping up."}
                      {groupKey === "structure_led" && "Structure is present, but demand or activation is lagging."}
                      {groupKey === "weak" && "These markets show limited signal, weak quality, or low conviction."}
                    </p>
                  </div>

                  <div className="grid gap-6 sm:gap-8 lg:grid-cols-2">
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
                        expectedFailureMode={item.intervention_intelligence?.expected_failure_mode ?? null}
                        hasContextualSummary={item.hasContextualSummary}
                        scoreLabel="Structural quality score"
                        scoreValue={
                          item.structural_score !== null && item.structural_score !== undefined
                            ? formatNumber(item.structural_score)
                            : "—"
                        }
                        summary={item.summary}
                        flags={mapParticipantFlags({
                          neutralShare: item.neutral_share ?? null,
                          whaleShare: item.whale_share ?? null,
                          speculativeShare: item.speculative_share ?? null,
                          participationQuality: item.participation_quality_score ?? null,
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
