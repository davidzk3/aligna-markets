import Link from "next/link";
import SectionCard from "@/components/SectionCard";
import KeyValueTable from "@/components/KeyValueTable";
import type { AiContextDriver } from "@/lib/types";
import { getMarketSnapshot } from "@/lib/api";
import { formatPercent01, formatMetricValue } from "@/lib/format";
import {
  mapParticipantFlags,
  mapFlagsForDisplay,
} from "@/lib/marketNarrative";

type PageProps = {
  params: Promise<{
    marketId: string;
  }>;
  searchParams: Promise<{
    title?: string;
    url?: string;
  }>;
};

type CohortItem = {
  cohort?: string;
  traders?: number;
  unique_traders?: number;
  trader_count?: number;
  trades?: number;
  trade_count?: number;
  notional_total?: number;
  notional?: number;
  avg_trade_size?: number;
  average_trade_size?: number;
};

function getStatusPillClass(
  value?: string | null,
  type?: "structural" | "social" | "alignment",
  emphasis?: "primary" | "secondary"
) {
  if (type === "alignment") {
    switch (value) {
      case "confirmed":
        return emphasis === "primary"
          ? "bg-emerald-100 text-emerald-800 ring-1 ring-emerald-200"
          : "bg-emerald-50 text-emerald-700 ring-1 ring-emerald-100";
      case "structure_led":
        return emphasis === "primary"
          ? "bg-blue-100 text-blue-800 ring-1 ring-blue-200"
          : "bg-blue-50 text-blue-700 ring-1 ring-blue-100";
      case "conviction_mismatch":
        return emphasis === "primary"
          ? "bg-amber-100 text-amber-800 ring-1 ring-amber-200"
          : "bg-amber-50 text-amber-700 ring-1 ring-amber-100";
      case "weak":
        return emphasis === "primary"
          ? "bg-zinc-200 text-zinc-800 ring-1 ring-zinc-300"
          : "bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200";
      default:
        return "bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200";
    }
  }

  if (type === "social") {
    switch (value) {
      case "strong":
        return "bg-violet-200 text-violet-900 ring-1 ring-violet-300";
      case "established":
        return "bg-violet-100 text-violet-800 ring-1 ring-violet-200";
      case "building":
        return "bg-purple-100 text-purple-800 ring-1 ring-purple-200";
      case "limited":
        return "bg-fuchsia-100 text-fuchsia-800 ring-1 ring-fuchsia-200";
      case "absent":
        return "bg-zinc-100 text-zinc-500 ring-1 ring-zinc-200";
      default:
        return "bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200";
    }
  }

  switch (value) {
    case "strong":
      return "bg-emerald-50 text-emerald-700 ring-1 ring-emerald-100";
    case "moderate":
      return "bg-amber-50 text-amber-700 ring-1 ring-amber-100";
    case "weak":
      return "bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200";
    default:
      return "bg-zinc-100 text-zinc-700 ring-1 ring-zinc-200";
  }
}

function StatusPill({
  label,
  value,
  type = "structural",
  emphasis = "secondary",
}: {
  label: string;
  value?: string | null;
  type?: "structural" | "social" | "alignment";
  emphasis?: "primary" | "secondary";
}) {
  if (!value) return null;

  const sizeClass =
    emphasis === "primary"
      ? "h-10 rounded-xl px-4 text-sm font-semibold"
      : "h-8 rounded-lg px-3 text-xs font-medium";

  return (
    <span
      className={`inline-flex items-center whitespace-nowrap ${sizeClass} ${getStatusPillClass(
        value,
        type,
        emphasis
      )}`}
    >
      {label}: {value}
    </span>
  );
}

function asCohortArray(value: unknown): CohortItem[] {
  if (!Array.isArray(value)) return [];
  return value.filter(
    (item): item is CohortItem => typeof item === "object" && item !== null
  );
}

function normalizeCohortName(value: string): string {
  const v = value.toLowerCase();
  if (v === "speculator") return "speculative";
  return v;
}

function mergeCohorts(
  sameDayValue: unknown,
  rollingWindowValue: unknown
): Map<
  string,
  { traders: number; trades: number; notional: number; avgTradeSize: number }
> {
  const combined = [
    ...asCohortArray(sameDayValue),
    ...asCohortArray(rollingWindowValue),
  ];

  const merged = new Map<
    string,
    { traders: number; trades: number; notional: number; avgTradeSize: number }
  >();

  for (const item of combined) {
    const cohortRaw = item.cohort || "unknown";
    const cohort = normalizeCohortName(String(cohortRaw));

    const traders =
      Number(item.traders ?? item.unique_traders ?? item.trader_count ?? 0) || 0;

    const trades = Number(item.trades ?? item.trade_count ?? 0) || 0;

    const notional = Number(item.notional_total ?? item.notional ?? 0) || 0;

    const avgTradeSize =
      Number(item.avg_trade_size ?? item.average_trade_size ?? 0) || 0;

    const current = merged.get(cohort) || {
      traders: 0,
      trades: 0,
      notional: 0,
      avgTradeSize: 0,
    };

    current.traders = Math.max(current.traders, traders);
    current.trades = Math.max(current.trades, trades);
    current.notional = Math.max(current.notional, notional);
    current.avgTradeSize = Math.max(current.avgTradeSize, avgTradeSize);

    merged.set(cohort, current);
  }

  return merged;
}

function formatCohortSummary(
  merged: Map<
    string,
    { traders: number; trades: number; notional: number; avgTradeSize: number }
  >
): string {
  if (merged.size === 0) return "—";

  const ordered = Array.from(merged.entries()).sort((a, b) => {
    const sizeDiff = b[1].avgTradeSize - a[1].avgTradeSize;
    if (sizeDiff !== 0) return sizeDiff;
    return b[1].traders - a[1].traders;
  });

  return ordered
    .map(([cohort, stats]) => {
      const traders = stats.traders.toLocaleString();
      const avgSize = stats.avgTradeSize.toLocaleString(undefined, {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      });
      return `${cohort}: ${traders} traders, avg size ${avgSize}`;
    })
    .join(" • ");
}

function cohortShare(
  merged: Map<
    string,
    { traders: number; trades: number; notional: number; avgTradeSize: number }
  >,
  cohortName: string
): number | null {
  const totalTraders = Array.from(merged.values()).reduce(
    (sum, item) => sum + item.traders,
    0
  );

  if (totalTraders <= 0) return null;

  const cohort = merged.get(cohortName);
  if (!cohort) return 0;

  return cohort.traders / totalTraders;
}

function formatLabel(value?: string | null): string {
  if (!value) return "—";
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatAiLabel(value?: string | null): string {
  if (!value) return "—";
  const normalized = value.replaceAll("_", " ").replaceAll(":", " • ");
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function formatAiStringList(values?: string[] | null): string {
  if (!values || values.length === 0) return "—";
  return values.map((value) => formatAiLabel(value)).join(", ");
}

function formatAiConfidence(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(2);
}

function getInterventionType(action?: string | null): string {
  if (!action) return "Review needed";

  if (
    action.includes("liquidity") ||
    action.includes("market_maker") ||
    action.includes("maker")
  ) {
    return "Structure support";
  }

  if (
    action.includes("subsidize") ||
    action.includes("points") ||
    action.includes("referral")
  ) {
    return "Growth support";
  }

  if (action.includes("redesign") || action.includes("tighten")) {
    return "Design correction";
  }

  if (action.includes("monitor")) {
    return "Observation";
  }

  return "Operator review";
}

function getExpectedEffect(
  action?: string | null,
  alignmentState?: string | null
): string {
  if (!action) return "No explicit expected effect available";

  if (action.includes("liquidity")) {
    return "Improve structure and convert attention into broader participation";
  }

  if (action.includes("market_maker") || action.includes("maker")) {
    return "Tighten quoting quality and reduce one-sided market fragility";
  }

  if (action.includes("subsidize")) {
    return "Avoid premature support until external demand strengthens";
  }

  if (action.includes("redesign") || action.includes("tighten")) {
    return "Reduce ambiguity or distortion before stronger market support";
  }

  if (action.includes("monitor")) {
    return alignmentState === "structure_led"
      ? "Observe whether demand catches up before using incentives"
      : "Observe whether current signals strengthen or decay";
  }

  return "Review current market state before choosing a support lever";
}

function formatAiDriverDirection(
  value?: "positive" | "negative" | "fragility" | "uncertainty" | null
): string {
  if (!value) return "—";
  return formatAiLabel(value);
}

function formatAiDriverImpact(
  value?: "high" | "medium" | "low" | null
): string {
  if (!value) return "—";
  return formatAiLabel(value);
}

function renderAiDriverPanel(drivers?: AiContextDriver[] | null) {
  if (!drivers || drivers.length === 0) {
    return <span>—</span>;
  }

  const impactRank: Record<string, number> = {
    high: 3,
    medium: 2,
    low: 1,
  };

  const sorted = [...drivers].sort(
    (a, b) => (impactRank[b?.impact ?? ""] || 0) - (impactRank[a?.impact ?? ""] || 0)
  );

  const primary = sorted[0];
  const secondary = sorted[1];
  const supporting = sorted.slice(2);

  const directionColor = (d?: string | null) => {
    if (d === "positive") return "text-emerald-700";
    if (d === "negative") return "text-red-700";
    if (d === "fragility") return "text-amber-700";
    return "text-zinc-600";
  };

  const renderCore = (label: string, driver?: AiContextDriver) => {
    if (!driver) return null;

    return (
      <div className="flex flex-col gap-1">
        <div className="text-[11px] font-semibold uppercase text-zinc-500 tracking-wide">
          {label}
        </div>

        <div className="text-sm font-medium text-zinc-900">
          {driver.label}
        </div>

        <div className="flex gap-3 text-xs">
          <span className="text-zinc-500">
            impact: {formatAiDriverImpact(driver.impact)}
          </span>

          <span className={directionColor(driver.direction)}>
            direction: {formatAiDriverDirection(driver.direction)}
          </span>
        </div>
      </div>
    );
  };

  return (
    <div className="flex flex-col gap-4">
      {renderCore("Primary Signal", primary)}
      {renderCore("Secondary Signal", secondary)}

      {supporting.length > 0 && (
        <div className="flex flex-col gap-2">
          <div className="text-[11px] font-semibold uppercase text-zinc-500 tracking-wide">
            Supporting Signals
          </div>

          <div className="flex flex-wrap gap-2">
            {supporting.map((d, i) => (
              <span
                key={`${d.key || d.label}-${i}`}
                className={`rounded-md border px-2 py-1 text-xs ${directionColor(
                  d.direction
                )} border-zinc-200`}
              >
                {d.label}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function getTopLevelOrNested(
  snapshot: any,
  topLevelKey: string,
  nestedObj?: any,
  nestedKey?: string
) {
  if (
    snapshot?.[topLevelKey] !== undefined &&
    snapshot?.[topLevelKey] !== null
  ) {
    return snapshot[topLevelKey];
  }

  if (
    nestedObj &&
    nestedKey &&
    nestedObj[nestedKey] !== undefined &&
    nestedObj[nestedKey] !== null
  ) {
    return nestedObj[nestedKey];
  }

  return null;
}

export default async function MarketDetailPage({
  params,
  searchParams,
}: PageProps) {
  const { marketId } = await params;
  const resolvedSearchParams = await searchParams;

  let snapshot: any;
  let error: string | null = null;

  try {
    snapshot = await getMarketSnapshot(marketId);
  } catch (err) {
    error =
      err instanceof Error ? err.message : "Failed to load market snapshot";
  }

  if (error || !snapshot) {
    return (
      <div className="mx-auto max-w-5xl space-y-4">
<Link
  href="/explorer"
  className="text-sm font-medium text-blue-600 hover:underline"
>
  ← Back to explorer
</Link>

        <div className="rounded-2xl border border-red-200 bg-red-50 p-5 text-red-700">
          {error || "Unable to load market snapshot"}
        </div>
      </div>
    );
  }

  const market = snapshot.market || {};
  const social = snapshot.social_intelligence || {};
  const alignment = snapshot.alignment_intelligence || {};
  const aiContext = snapshot.ai_context || null;
  const sameDay = snapshot.traders?.same_day || {};
  const rollingWindow = snapshot.traders?.rolling_window || {};
  const snapshotMeta = snapshot.snapshot_meta || {};

  const structuralDay = snapshotMeta.structural_day || market.day || null;
  const socialDay = snapshotMeta.social_day || social.day || null;
  const alignmentDay = snapshotMeta.alignment_day || alignment.day || null;

  const displayTitle =
    market.question ||
    market.title ||
    resolvedSearchParams.title ||
    snapshot.question ||
    marketId;

  const displayUrl = market.url || resolvedSearchParams.url || "";

  const participationQuality =
    market.participation_quality_score !== null &&
    market.participation_quality_score !== undefined &&
    !Number.isNaN(Number(market.participation_quality_score))
      ? Number(market.participation_quality_score)
      : null;

  const liquidityDurability =
    market.liquidity_durability_score !== null &&
    market.liquidity_durability_score !== undefined &&
    !Number.isNaN(Number(market.liquidity_durability_score))
      ? Number(market.liquidity_durability_score)
      : null;

  const concentrationHHI =
    market.concentration_hhi !== null &&
    market.concentration_hhi !== undefined &&
    !Number.isNaN(Number(market.concentration_hhi))
      ? Number(market.concentration_hhi)
      : null;

  const structuralScore =
    snapshot.structural_score !== null &&
    snapshot.structural_score !== undefined &&
    !Number.isNaN(Number(snapshot.structural_score))
      ? Number(snapshot.structural_score)
      : market.market_quality_score !== null &&
        market.market_quality_score !== undefined &&
        !Number.isNaN(Number(market.market_quality_score))
      ? Number(market.market_quality_score)
      : null;

  const structuralRisk =
    market.concentration_risk_score !== null &&
    market.concentration_risk_score !== undefined &&
    !Number.isNaN(Number(market.concentration_risk_score))
      ? Number(market.concentration_risk_score)
      : null;

  const structuralState =
    getTopLevelOrNested(snapshot, "structural_state", market, "structural_state") ||
    null;

  const socialState =
    getTopLevelOrNested(snapshot, "social_state", social, "demand_state") ||
    null;

  const alignmentState = snapshot.alignment_state ?? alignment.alignment_state ?? null;

  const contextualSummary = snapshot.contextual_summary || null;
    const intervention = snapshot.intervention_intelligence || null;

  const interventionNeeded = snapshot.intervention_needed ?? null;
  const incentiveDependency = snapshot.incentive_dependency ?? null;
  const activityQuality = snapshot.activity_quality ?? null;

  const recommendedAction = snapshot.recommended_action ?? null;
  const actionPriority = snapshot.action_priority ?? null;
  const actionReason = snapshot.action_reason ?? null;
  const expectedFailureMode = intervention?.expected_failure_mode ?? null;

  const resolutionState = snapshot.resolution_state ?? null;
  const resolutionClarityScore =
    snapshot.resolution_clarity_score ?? null;
  const oracleRiskScore = snapshot.oracle_risk_score ?? null;

  const auditId = snapshot.audit_id ?? null;
  const cacheHit = snapshot.cache_hit ?? null;

  const mergedCohorts = mergeCohorts(
    sameDay.cohorts_summary,
    rollingWindow.cohorts_summary
  );

  const combinedCohorts = formatCohortSummary(mergedCohorts);

  const neutralShare = cohortShare(mergedCohorts, "neutral");
  const whaleShare = cohortShare(mergedCohorts, "whale");
  const speculativeShare = cohortShare(mergedCohorts, "speculative");

  const totalTraders = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.traders,
    0
  );

  const totalTrades = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.trades,
    0
  );

  const totalVolume = Array.from(mergedCohorts.values()).reduce(
    (sum, item) => sum + item.notional,
    0
  );

  const formattedTotalTraders =
    totalTraders > 0 ? totalTraders.toLocaleString() : "Not available";

  const formattedTotalTrades =
    totalTrades > 0 ? totalTrades.toLocaleString() : "Not available";

  const formattedTotalVolume =
    totalVolume > 0
      ? totalVolume.toLocaleString(undefined, {
          minimumFractionDigits: 0,
          maximumFractionDigits: 2,
        })
      : "Not available";

  const participantFlags = mapParticipantFlags({
    neutralShare,
    whaleShare,
    speculativeShare,
    participationQuality,
  });

  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <div>
<Link
  href="/explorer"
  className="text-sm font-medium text-blue-600 hover:underline"
>
  ← Back to explorer
</Link>
      </div>

      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Market Detail</p>

        <div className="mt-1 flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-zinc-900">
              {displayTitle}
            </h1>

            <p className="mt-2 text-sm text-zinc-600">
              {market.protocol || "—"} · {market.chain || "—"}
            </p>

            {displayUrl ? (
              <div className="mt-3">
                <a
                  href={displayUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="text-sm font-medium text-blue-600 hover:underline"
                >
                  Open external market
                </a>
              </div>
            ) : null}
          </div>

          <div className="flex flex-col items-end gap-2">
            <div className="group relative">
              <StatusPill
                label="alignment"
                value={alignmentState}
                type="alignment"
                emphasis="primary"
              />

              <div className="absolute right-0 top-12 z-20 hidden w-72 rounded-xl border border-zinc-200 bg-white p-3 text-xs text-zinc-600 shadow-lg group-hover:block">
                <p className="font-semibold text-zinc-900">Alignment</p>
                <p className="mt-1 text-zinc-600">
                  Backend-owned interpretation of how market structure and external
                  demand relate.
                </p>
                <div className="mt-2 space-y-1">
                  <p>
                    <span className="font-medium text-zinc-900">confirmed</span> →
                    structure and demand reinforce each other
                  </p>
                  <p>
                    <span className="font-medium text-zinc-900">structure_led</span> →
                    structure is stronger than current external demand
                  </p>
                  <p>
                    <span className="font-medium text-zinc-900">
                      conviction_mismatch
                    </span>{" "}
                    → attention or demand is present but not well supported by
                    conviction or participation depth
                  </p>
                  <p>
                    <span className="font-medium text-zinc-900">weak</span> → the
                    market is not currently showing strong combined evidence
                  </p>
                </div>
              </div>
            </div>

            <div className="flex flex-wrap justify-end gap-2">
              <StatusPill
                label="structural"
                value={structuralState}
                type="structural"
              />
              <StatusPill label="demand" value={socialState} type="social" />
            </div>
          </div>
        </div>

        <div className="mt-6 space-y-4">
          <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-4">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-2">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-500">
                  Decision layer
                </p>

                <div className="flex flex-wrap gap-2">
                  <span className="rounded-md bg-white px-3 py-1 text-sm font-medium text-zinc-800">
                    State: {formatLabel(alignmentState)}
                  </span>

                  <span className="rounded-md bg-white px-3 py-1 text-sm font-medium text-zinc-800">
                    Integrity: {formatLabel(
                      market.integrity_band || snapshot.integrity_band
                    )}
                  </span>

                  <span className="rounded-md bg-white px-3 py-1 text-sm font-medium text-zinc-800">
                    Resolution: {formatLabel(resolutionState)}
                  </span>
                </div>
              </div>

              <div className="min-w-[280px] rounded-xl border border-amber-200 bg-amber-50 p-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700">
                  Recommended action
                </p>

                <div className="mt-1 text-lg font-semibold text-zinc-900">
                  {formatLabel(recommendedAction)}
                </div>

                <p className="mt-2 text-sm text-zinc-700">
                  {actionReason || "No action rationale available"}
                </p>

                <div className="mt-3 flex flex-wrap gap-2 text-xs">
                  <span className="rounded-md bg-white px-2 py-1 text-zinc-700">
                    Priority: {formatLabel(actionPriority)}
                  </span>

                  <span className="rounded-md bg-white px-2 py-1 text-zinc-700">
                    Risk: {formatLabel(expectedFailureMode)}
                  </span>
                </div>
              </div>
            </div>
          </div>

          <p className="max-w-3xl text-sm text-zinc-600">
            {contextualSummary ||
              "This page renders backend-generated market intelligence directly, without frontend normalization or commentary rebuilding."}
          </p>

          <div className="flex flex-wrap gap-2 text-xs">
            {structuralDay && (
              <span className="rounded-md bg-zinc-100 px-2 py-1 text-zinc-600">
                Structural day: {structuralDay}
              </span>
            )}

            {socialDay && (
              <span className="rounded-md bg-violet-50 px-2 py-1 text-violet-700">
                Social day: {socialDay}
              </span>
            )}

            {alignmentDay && (
              <span className="rounded-md bg-sky-50 px-2 py-1 text-sky-700">
                Alignment day: {alignmentDay}
              </span>
            )}

            {auditId ? (
              <span className="rounded-md bg-zinc-100 px-2 py-1 text-zinc-600">
                Audit ID: {String(auditId)}
              </span>
            ) : null}

            {cacheHit !== null ? (
              <span className="rounded-md bg-zinc-100 px-2 py-1 text-zinc-600">
                Cache hit: {cacheHit ? "Yes" : "No"}
              </span>
            ) : null}
          </div>

          {snapshot.is_mixed_horizon && (
            <div className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
              Mixed horizon: structure and demand signals are from different dates
            </div>
          )}
        </div>
      </section>

      <SectionCard
        title={`Decision Summary${
          structuralDay || socialDay
            ? ` (structural: ${structuralDay ?? "—"}, social: ${socialDay ?? "—"})`
            : ""
        }`}
      >
        <p className="mb-4 text-xs italic text-zinc-500">
          Backend-generated combined read of structure, demand, and alignment
        </p>

        <KeyValueTable
          rows={[
            { label: "Structural State", value: structuralState || "—" },
            { label: "Demand State", value: socialState || "—" },
            { label: "Alignment State", value: alignmentState || "—" },
            {
              label: "Structural Score",
              value: formatMetricValue(structuralScore),
            },
            {
              label: "Alignment Score",
              value: formatMetricValue(alignment.alignment_score),
            },
            {
              label: "Attention vs Structure Gap",
              value: formatMetricValue(alignment.attention_vs_structure_gap),
            },
            {
              label: "Contextual Summary",
              value: contextualSummary || "—",
            },
            {
              label: "Flags",
              value: mapFlagsForDisplay(alignment.flags).join(", ") || "—",
            },
          ]}
        />
      </SectionCard>

      <div className="grid gap-8 xl:grid-cols-2">
        <SectionCard
          title={`Structural Health${structuralDay ? ` (as of ${structuralDay})` : ""}`}
        >
          <p className="mb-4 text-xs italic text-zinc-500">
            Structural metrics and integrity inputs
          </p>

          <KeyValueTable
            rows={[
              { label: "Structural State", value: structuralState || "—" },
              {
                label: "Integrity Band",
                value: market.integrity_band || snapshot.integrity_band || "—",
              },
              { label: "Review Priority", value: market.review_priority || "—" },
              {
                label: "Spread Median",
                value: formatMetricValue(market.spread_median),
              },
              {
                label: "Depth 2pct Median",
                value: formatMetricValue(market.depth_2pct_median),
              },
              {
                label: "Concentration HHI",
                value: formatMetricValue(concentrationHHI),
              },
              { label: "Structural Score", value: formatMetricValue(structuralScore) },
              { label: "Structural Risk", value: formatMetricValue(structuralRisk) },
              {
                label: "Liquidity Durability",
                value: formatMetricValue(liquidityDurability),
              },
              {
                label: "Participation Quality",
                value: formatMetricValue(participationQuality),
              },
              {
                label: "Flags",
                value: mapFlagsForDisplay(market.flags).join(", ") || "—",
              },
            ]}
          />
        </SectionCard>

        <SectionCard
          title={`Demand Signals${socialDay ? ` (as of ${socialDay})` : ""}`}
        >
          <p className="mb-4 text-xs italic text-violet-600">
            External demand and attention signals from the backend demand layer
          </p>

          <KeyValueTable
            rows={[
              { label: "Demand State", value: socialState || "—" },
              { label: "Summary", value: social.summary || "—" },
              {
                label: "Attention Score",
                value: formatMetricValue(social.attention_score),
              },
              {
                label: "Demand Score",
                value: formatMetricValue(social.demand_score ?? snapshot.social_score),
              },
              {
                label: "Sentiment Score",
                value: formatMetricValue(social.sentiment_score),
              },
              {
                label: "Trend Velocity",
                value: formatMetricValue(social.trend_velocity),
              },
              {
                label: "Confidence Score",
                value: formatMetricValue(social.confidence_score),
              },
              {
                label: "Mention Count",
                value: formatMetricValue(social.mention_count),
              },
              {
                label: "Source Count",
                value: formatMetricValue(social.source_count),
              },
              {
                label: "Flags",
                value: mapFlagsForDisplay(social.flags).join(", ") || "—",
              },
            ]}
          />
        </SectionCard>
      </div>

      <SectionCard
        title={`Alignment Intelligence${alignmentDay ? ` (as of ${alignmentDay})` : ""}`}
      >
        <p className="mb-4 text-xs italic text-zinc-500">
          Backend-owned alignment state and related diagnostics
        </p>

<KeyValueTable
  rows={[
    {
      label: "Alignment Score",
      value: formatMetricValue(alignment.alignment_score),
    },
    {
      label: "Attention vs Structure Gap",
      value: formatMetricValue(alignment.attention_vs_structure_gap),
    },
    {
      label: "Flags",
      value: mapFlagsForDisplay(alignment.flags).join(", ") || "—",
    },
  ]}
/>
      </SectionCard>

      <SectionCard title="Contextual Interpretation">
        <p className="mb-4 text-xs italic text-zinc-500">
          Structured interpretation returned by the AI context layer
        </p>

<div className="space-y-5">
  <div className="space-y-2">
    <p className="text-sm text-zinc-600">
      {contextualSummary || "No summary available"}
    </p>

    {aiContext?.interpretation ? (
      <p className="text-sm font-medium text-zinc-900">
        {aiContext.interpretation}
      </p>
    ) : null}
  </div>

  <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-4">
    {renderAiDriverPanel(aiContext?.drivers)}
  </div>

  <div className="flex flex-wrap gap-3 text-xs text-zinc-600">
    <span>Type: {formatAiLabel(aiContext?.interpretation_type)}</span>
    <span>Confidence: {formatAiConfidence(aiContext?.confidence)}</span>
    {aiContext?.caution_flags?.length ? (
      <span>Caution: {formatAiStringList(aiContext.caution_flags)}</span>
    ) : null}
  </div>
</div>
      </SectionCard>

<SectionCard title="Intervention Intelligence">
  <p className="mb-4 text-xs italic text-zinc-500">
    First-pass intervention review derived from current structure, demand,
    and participation dynamics
  </p>

  <div className="mb-4 rounded-xl border border-amber-200 bg-amber-50 p-4">
    <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700">
      Intervention details
    </div>

    <div className="mt-2 grid gap-3 md:grid-cols-3">
      <div className="rounded-lg bg-white p-3">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Type
        </div>
        <div className="mt-1 text-sm font-semibold text-zinc-900">
          {getInterventionType(recommendedAction)}
        </div>
      </div>

      <div className="rounded-lg bg-white p-3 md:col-span-2">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Expected effect
        </div>
        <div className="mt-1 text-sm text-zinc-700">
          {getExpectedEffect(recommendedAction, alignmentState)}
        </div>
      </div>
    </div>
  </div>
{recommendedAction && (
  <div className="mb-4 rounded-xl border border-amber-200 bg-amber-50 p-4">
    <div className="text-xs font-medium uppercase tracking-wide text-amber-700">
      Recommended Action
    </div>

    <div className="mt-1 text-lg font-semibold text-zinc-900">
      {formatLabel(recommendedAction)}
    </div>

    {actionReason && (
      <div className="mt-2 text-sm text-zinc-700">{actionReason}</div>
    )}

    <div className="mt-3 flex flex-wrap gap-2 text-xs">
      {actionPriority && (
        <span className="rounded-md bg-white px-2 py-1 text-zinc-700">
          Priority: {formatLabel(actionPriority)}
        </span>
      )}

      {expectedFailureMode && (
        <span className="rounded-md bg-white px-2 py-1 text-zinc-700">
          Risk: {formatLabel(expectedFailureMode)}
        </span>
      )}
    </div>
  </div>
)}

{recommendedAction && (
  <div className="mb-4 rounded-xl border border-blue-200 bg-blue-50 p-4">
    <div className="text-xs font-medium uppercase tracking-wide text-blue-700">
      Execution Guidance
    </div>

    <div className="mt-3 grid gap-3 md:grid-cols-2">
      <div className="rounded-lg bg-white p-3">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Deployment strategy
        </div>
        <div className="mt-1 text-sm font-semibold text-zinc-900">
          {getExecutionStrategy({
            interventionCandidate: snapshot.intervention_needed,
            recommendedAction,
            structuralState,
            actionPriority,
            expectedFailureMode,
            activityQuality: snapshot.activity_quality,
          })}
        </div>
      </div>

      <div className="rounded-lg bg-white p-3">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Target zone
        </div>
        <div className="mt-1 text-sm text-zinc-700">
          {getExecutionZone({
            interventionCandidate: snapshot.intervention_needed,
            structuralState,
          })}
        </div>
      </div>

      <div className="rounded-lg bg-white p-3">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Sizing guidance
        </div>
        <div className="mt-1 text-sm text-zinc-700">
          {getExecutionSizing({
            interventionCandidate: snapshot.intervention_needed,
            actionPriority,
          })}
        </div>
      </div>

      <div className="rounded-lg bg-white p-3">
        <div className="text-xs font-medium uppercase tracking-wide text-zinc-500">
          Failure mode warning
        </div>
        <div className="mt-1 text-sm text-amber-700">
          {getExecutionRisk({
            interventionCandidate: snapshot.intervention_needed,
            expectedFailureMode,
            activityQuality: snapshot.activity_quality,
          })}
        </div>
      </div>
    </div>
  </div>
)}


  <KeyValueTable
    rows={[
{
  label: "Intervention candidate",
  value:
    snapshot.intervention_needed === null ||
    snapshot.intervention_needed === undefined
      ? "—"
      : snapshot.intervention_needed
      ? "Yes"
      : "No",
},
{
  label: "Recommended action",
  value: formatLabel(recommendedAction),
},
{
  label: "Action priority",
  value: formatLabel(actionPriority),
},
{
  label: "Action type",
  value: getInterventionType(recommendedAction),
},
{
  label: "Action reason",
  value: actionReason ?? "—",
},
{
  label: "Expected effect",
  value: getExpectedEffect(recommendedAction, alignmentState),
},
{
  label: "Deployment strategy",
  value: getExecutionStrategy({
    interventionCandidate: snapshot.intervention_needed,
    recommendedAction,
    structuralState,
    actionPriority,
    expectedFailureMode,
    activityQuality: snapshot.activity_quality,
  }),
},
{
  label: "Target zone",
  value: getExecutionZone({
    interventionCandidate: snapshot.intervention_needed,
    structuralState,
  }),
},
{
  label: "Sizing guidance",
  value: getExecutionSizing({
    interventionCandidate: snapshot.intervention_needed,
    actionPriority,
  }),
},
{
  label: "Failure mode warning",
  value: getExecutionRisk({
    interventionCandidate: snapshot.intervention_needed,
    expectedFailureMode,
    activityQuality: snapshot.activity_quality,
  }),
},
      {
        label: "Incentive dependency",
        value: formatLabel(snapshot.incentive_dependency),
      },
      {
        label: "Activity quality",
        value: formatLabel(snapshot.activity_quality),
      },
      {
        label: "Expected failure mode",
        value: formatLabel(
  snapshot.intervention_intelligence?.expected_failure_mode
),
      },
      {
        label: "Distortion risk",
        value:
          snapshot.intervention_intelligence?.distortion_risk ?? "—",
      },
      {
        label: "Effectiveness estimate",
        value:
          snapshot.intervention_intelligence
            ?.intervention_effectiveness_estimate ?? "—",
      },
    ]}
  />
</SectionCard>

      <SectionCard title="Resolution Intelligence">
        <p className="mb-4 text-xs italic text-zinc-500">
          Resolution and oracle-facing signals that affect settlement clarity
          and dispute exposure
        </p>

        <KeyValueTable
          rows={[
            {
              label: "Resolution state",
              value: formatLabel(resolutionState),
            },
            {
              label: "Resolution clarity score",
              value: formatMetricValue(resolutionClarityScore),
            },
            {
              label: "Oracle risk score",
              value: formatMetricValue(oracleRiskScore),
            },
            {
              label: "Observation",
              value:
                resolutionState || resolutionClarityScore || oracleRiskScore
                  ? "Use this layer to judge whether the market is likely to resolve cleanly or create downstream oracle ambiguity."
                  : "No explicit resolution intelligence available for this market yet.",
            },
          ]}
        />
      </SectionCard>

      <SectionCard title="Participant Signals">
        <p className="mb-4 text-xs italic text-zinc-500">
          Participant mix derived from cohort composition across the current
          observation window
        </p>

        <KeyValueTable
          rows={[
            {
              label: "Participation Quality",
              value: formatMetricValue(participationQuality),
            },
            {
              label: "Total Traders",
              value: formattedTotalTraders,
            },
            {
              label: "Total Trades",
              value: formattedTotalTrades,
            },
            {
              label: "Total Volume",
              value: formattedTotalVolume,
            },
            {
              label: "Neutral Share",
              value:
                neutralShare !== null
                  ? formatPercent01(neutralShare)
                  : "Not available",
            },
            {
              label: "Whale Share",
              value:
                whaleShare !== null ? formatPercent01(whaleShare) : "Not available",
            },
            {
              label: "Speculative Share",
              value:
                speculativeShare !== null
                  ? formatPercent01(speculativeShare)
                  : "Not available",
            },
            {
              label: "Participant Flags",
              value: participantFlags.length > 0 ? participantFlags.join(", ") : "—",
            },
            {
              label: "Cohort Summary",
              value: combinedCohorts || "—",
            },
          ]}
        />
      </SectionCard>
    </div>
  );
}
function getExecutionStrategy(input: {
  interventionCandidate?: boolean | null;
  recommendedAction?: string | null;
  structuralState?: string | null;
  actionPriority?: string | null;
  expectedFailureMode?: string | null;
  activityQuality?: string | null;
}): string {
  if (!input.interventionCandidate) return "No immediate execution guidance";

  const action = input.recommendedAction || "";

  if (action.includes("liquidity")) {
    return "Add targeted liquidity close to the active trading zone rather than broad passive depth";
  }

  if (action.includes("maker")) {
    return "Use maker-side support with tighter quoting discipline instead of broad trader incentives";
  }

  if (action.includes("redesign") || action.includes("tighten")) {
    return "Correct market structure or wording before applying stronger support";
  }

  if (action.includes("subsidize")) {
    return "Avoid direct subsidy until stronger demand evidence appears";
  }

  if (action.includes("monitor")) {
    return "Observe market development before deploying support";
  }

  return "Use operator review before intervention";
}

function getExecutionZone(input: {
  interventionCandidate?: boolean | null;
  structuralState?: string | null;
}): string {
  if (!input.interventionCandidate) return "No target zone required";

  if (input.structuralState === "weak") {
    return "Focus support near the mid-price where spread instability is most damaging";
  }

  if (input.structuralState === "moderate") {
    return "Support the active trading band rather than deep out-of-range liquidity";
  }

  if (input.structuralState === "strong") {
    return "Use minimal intervention and avoid over-supporting already healthy structure";
  }

  return "Review live market conditions before deployment";
}

function getExecutionSizing(input: {
  interventionCandidate?: boolean | null;
  actionPriority?: string | null;
}): string {
  if (!input.interventionCandidate) return "No execution sizing required";

  if (input.actionPriority === "high") {
    return "Use moderate-to-strong support and monitor market response closely";
  }

  if (input.actionPriority === "medium") {
    return "Start with incremental support and expand only if structure improves";
  }

  if (input.actionPriority === "low") {
    return "Use minimal support or observation only";
  }

  return "Use conservative initial sizing";
}

function getExecutionRisk(input: {
  interventionCandidate?: boolean | null;
  expectedFailureMode?: string | null;
  activityQuality?: string | null;
}): string {
  if (!input.interventionCandidate) return "No immediate execution risk";

  if (input.activityQuality === "distorted") {
    return "Support may amplify weak or artificial participation rather than improve real market quality";
  }

  if (input.expectedFailureMode === "one_sided_liquidity") {
    return "Poorly targeted liquidity may create artificial depth without broadening participation";
  }

  if (input.expectedFailureMode === "mercenary_capital") {
    return "Incentives may attract temporary flow without durable conviction";
  }

  if (input.expectedFailureMode === "fake_volume") {
    return "Growth support may increase visible activity without improving true market health";
  }

  return "Execution risk appears manageable, but intervention should still be monitored";
}