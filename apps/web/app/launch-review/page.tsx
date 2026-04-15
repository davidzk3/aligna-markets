"use client";

import { useMemo, useState } from "react";
import {
  type DesignReviewInput,
  type DesignIntelligenceResponse,
  type RewriteIntelligenceResponse,
  type ResolutionSimulationResponse,
  runDesignIntelligence,
  runRewriteIntelligence,
  runResolutionSimulation,
} from "@/lib/api";

function formatLabel(value?: string | null): string {
  if (!value) return "—";
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(2);
}

function formatDecision(value?: string | null): string {
  switch (value) {
    case "launch":
      return "Launch";
    case "launch_with_review":
      return "Launch With Edits";
    case "do_not_launch":
      return "Do Not Launch";
    case "redesign_required":
      return "Redesign Required";
    default:
      return "Run a review";
  }
}

function formatResolutionPath(value?: string | null): string {
  switch (value) {
    case "clean_resolution":
      return "Clean Resolution";
    case "disputed_then_resolved":
      return "Disputed Then Resolved";
    case "ambiguous_resolution":
      return "Ambiguous Resolution";
    case "operator_intervention":
      return "Operator Intervention";
    default:
      return "Not simulated";
  }
}

function formatConfidenceBand(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Not available";
  }
  if (value >= 0.75) return "High confidence";
  if (value >= 0.5) return "Moderate confidence";
  if (value >= 0.25) return "Review-sensitive";
  return "Low confidence";
}

const DEFAULT_INPUT: DesignReviewInput = {
  title: "Will the Fed cut rates by September 2026?",
  description:
    'This market will resolve to "Yes" if the US Federal Reserve announces a reduction in the target federal funds rate at or before its September 2026 meeting. Otherwise, this market will resolve to "No". The primary resolution source will be the official Federal Reserve statement and target rate publication.',
  category: "macro",
  market_type: "event_binary",
  protocol: "polymarket",
  oracle_family: "uma_oo",
};

export default function LaunchReviewPage() {
  const [input, setInput] = useState<DesignReviewInput>(DEFAULT_INPUT);
  const [designResult, setDesignResult] =
    useState<DesignIntelligenceResponse | null>(null);
  const [rewriteResult, setRewriteResult] =
    useState<RewriteIntelligenceResponse | null>(null);
  const [simulationResult, setSimulationResult] =
    useState<ResolutionSimulationResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const likelySupportDependency = useMemo(() => {
    const decision = designResult?.operator_decision_band;
    const rewritePriority = rewriteResult?.rewrite_priority;

    if (decision === "do_not_launch") return "Do not support yet";
    if (rewritePriority === "high") return "Likely redesign before support";
    if (
      designResult?.resolution_risk_band === "high" ||
      designResult?.dispute_propensity_band === "high"
    ) {
      return "High support dependency risk";
    }
    if (
      designResult?.resolution_risk_band === "manageable" ||
      designResult?.dispute_propensity_band === "moderate"
    ) {
      return "Moderate support dependency risk";
    }
    return "Not obviously support-dependent";
  }, [designResult, rewriteResult]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const [design, rewrite, simulation] = await Promise.all([
        runDesignIntelligence(input),
        runRewriteIntelligence(input),
        runResolutionSimulation(input),
      ]);

      setDesignResult(design);
      setRewriteResult(rewrite);
      setSimulationResult(simulation);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run launch review");
    } finally {
      setLoading(false);
    }
  }

  function updateField<K extends keyof DesignReviewInput>(
    key: K,
    value: DesignReviewInput[K]
  ) {
    setInput((prev) => ({ ...prev, [key]: value }));
  }

  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Launch Review</p>
        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          Test a market before launch
        </h1>
        <p className="mt-3 max-w-3xl text-sm leading-7 text-zinc-600">
          Paste a draft market and review launch readiness, rewrite needs,
          dispute risk, and likely intervention dependency before the market
          goes live.
        </p>
      </section>

      <div className="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
        <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <form onSubmit={handleSubmit} className="space-y-5">
            <div>
              <label className="mb-2 block text-sm font-medium text-zinc-700">
                Market title
              </label>
              <input
                value={input.title}
                onChange={(e) => updateField("title", e.target.value)}
                className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
              />
            </div>

            <div>
              <label className="mb-2 block text-sm font-medium text-zinc-700">
                Market description
              </label>
              <textarea
                value={input.description}
                onChange={(e) => updateField("description", e.target.value)}
                rows={10}
                className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
              />
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <label className="mb-2 block text-sm font-medium text-zinc-700">
                  Category
                </label>
                <input
                  value={input.category}
                  onChange={(e) => updateField("category", e.target.value)}
                  className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
                />
              </div>

              <div>
                <label className="mb-2 block text-sm font-medium text-zinc-700">
                  Market type
                </label>
                <input
                  value={input.market_type}
                  onChange={(e) => updateField("market_type", e.target.value)}
                  className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
                />
              </div>

              <div>
                <label className="mb-2 block text-sm font-medium text-zinc-700">
                  Protocol
                </label>
                <input
                  value={input.protocol}
                  onChange={(e) => updateField("protocol", e.target.value)}
                  className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
                />
              </div>

              <div>
                <label className="mb-2 block text-sm font-medium text-zinc-700">
                  Oracle family
                </label>
                <input
                  value={input.oracle_family}
                  onChange={(e) => updateField("oracle_family", e.target.value)}
                  className="w-full rounded-xl border border-zinc-300 px-4 py-3 text-sm outline-none ring-0 focus:border-zinc-500"
                />
              </div>
            </div>

            <div className="flex items-center gap-3">
              <button
                type="submit"
                disabled={loading}
                className="inline-flex items-center rounded-xl bg-zinc-900 px-5 py-3 text-sm font-semibold text-white hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {loading ? "Running review..." : "Run Launch Review"}
              </button>

              <button
                type="button"
                onClick={() => {
                  setInput(DEFAULT_INPUT);
                  setDesignResult(null);
                  setRewriteResult(null);
                  setSimulationResult(null);
                  setError(null);
                }}
                className="inline-flex items-center rounded-xl border border-zinc-300 bg-white px-5 py-3 text-sm font-semibold text-zinc-900 hover:bg-zinc-50"
              >
                Reset
              </button>
            </div>

            {error ? (
              <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                {error}
              </div>
            ) : null}
          </form>
        </section>

        <section className="space-y-6">
<div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">Decision</p>
  <h2 className="mt-1 text-xl font-semibold text-zinc-900">
    {formatDecision(designResult?.operator_decision_band)}
  </h2>
  <p className="mt-3 text-sm leading-7 text-zinc-600">
    {designResult?.decision_rationale ||
      "Launch Review will return a first-pass operator decision, rationale, and rewrite needs."}
  </p>
  <p className="mt-2 text-sm text-zinc-500">
    This reflects expected oracle behavior and dispute likelihood, not just wording quality.
  </p>

  <div className="mt-4 flex flex-wrap gap-2">
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      Resolution risk:{" "}
      {formatLabel(designResult?.resolution_risk_band)}
    </span>
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      Dispute propensity:{" "}
      {formatLabel(designResult?.dispute_propensity_band)}
    </span>
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      Rewrite priority: {formatLabel(rewriteResult?.rewrite_priority)}
    </span>
  </div>
</div>

<div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">
    Likely support dependency
  </p>
  <h2 className="mt-1 text-xl font-semibold text-zinc-900">
    {likelySupportDependency}
  </h2>
  <p className="mt-3 text-sm leading-7 text-zinc-600">
    This is a first-pass view of whether the market is likely to need
    support, redesign, or no subsidy yet before healthy trading can
    emerge.
  </p>
</div>

<div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">Resolution risks (operator-critical)</p>
  <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-zinc-600">
{rewriteResult?.rewrite_notes_compact?.length ? (
  rewriteResult.rewrite_notes_compact.map((note, idx) => (
    <li key={idx}>{note}</li>
  ))
) : designResult ? (
  <>
    <li>
      Resolution risk classified as {formatLabel(designResult.resolution_risk_band)}.
    </li>
    <li>
      Dispute likelihood classified as {formatLabel(designResult.dispute_propensity_band)}.
    </li>
    <li>
      Review recommended before launch to reduce ambiguity in resolution conditions.
    </li>
  </>
) : (
  <li>Run Launch Review to surface resolution risks.</li>
)}
  </ul>
</div>

<div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">Operator-ready resolution spec</p>
  <p className="mt-3 text-sm leading-7 text-zinc-700">
    {rewriteResult?.revised_description_compact ||
      "Run review to generate a compact rewrite suggestion."}
  </p>
</div>

<div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">Simulated resolution path</p>
  <h2 className="mt-1 text-xl font-semibold text-zinc-900">
    {formatResolutionPath(simulationResult?.simulation?.resolution_path)}
  </h2>
  <p className="mt-3 text-sm leading-7 text-zinc-600">
    This is a first-pass simulation of how the market may behave in the oracle workflow before launch.
  </p>

  <div className="mt-4 flex flex-wrap gap-2">
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      Settlement hours:{" "}
      {simulationResult?.simulation?.expected_settlement_hours ?? "—"}
    </span>
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      Confidence: {formatNumber(simulationResult?.simulation?.confidence)}
    </span>
    <span className="rounded-md bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-700">
      {formatConfidenceBand(simulationResult?.simulation?.confidence)}
    </span>
  </div>

  <div className="mt-4 grid gap-3 md:grid-cols-2">
    <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-3">
      <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">
        Simulation run
      </div>
      <div className="mt-1 text-sm font-semibold text-zinc-900">
        {simulationResult?.simulation_run_id ?? "—"}
      </div>
    </div>

    <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-3">
      <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">
        Engine version
      </div>
      <div className="mt-1 text-sm font-semibold text-zinc-900">
        {simulationResult?.engine_version || "—"}
      </div>
    </div>
  </div>
</div>
        </section>
      </div>

      <div className="grid gap-8 lg:grid-cols-2">

<section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm lg:col-span-2">
  <p className="text-sm font-medium text-zinc-500">Learning loop</p>
  <div className="mt-3 grid gap-4 md:grid-cols-3">
    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Predicted path
      </p>
      <p className="mt-2 text-sm font-semibold text-zinc-900">
        {formatResolutionPath(simulationResult?.simulation?.resolution_path)}
      </p>
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Simulation id
      </p>
      <p className="mt-2 text-sm font-semibold text-zinc-900">
        {simulationResult?.simulation_run_id ?? "—"}
      </p>
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        What happens next
      </p>
      <p className="mt-2 text-sm leading-6 text-zinc-600">
        After the real market resolves, this simulation can be evaluated against
        actual oracle behavior to measure path accuracy, dispute correctness,
        and settlement timing error.
      </p>
    </div>
  </div>
</section>

        <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <p className="text-sm font-medium text-zinc-500">Top rewrite actions</p>
          <div className="mt-4 space-y-4">
{rewriteResult?.top_rewrite_actions?.length ? (
  rewriteResult.top_rewrite_actions.map((action, idx) => (
    <div
      key={`${action.action_type}-${idx}`}
      className="rounded-xl border border-zinc-200 p-4"
    >
      <div className="flex flex-wrap gap-2">
        <span className="rounded-md bg-zinc-100 px-2 py-1 text-xs font-medium text-zinc-700">
          {formatLabel(action.action_type)}
        </span>
        <span className="rounded-md bg-zinc-100 px-2 py-1 text-xs font-medium text-zinc-700">
          Clause {action.target_clause_index ?? "—"}
        </span>
      </div>

      <p className="mt-3 text-sm font-medium text-zinc-900">
        {action.reason || "No rewrite reason provided"}
      </p>

      {action.after_text ? (
        <p className="mt-2 text-sm text-zinc-600">
          Suggested rewrite: {action.after_text}
        </p>
      ) : null}
    </div>
  ))
) : rewriteResult?.rewrite_notes_compact?.length ? (
  rewriteResult.rewrite_notes_compact.map((note, idx) => (
    <div
      key={`rewrite-note-${idx}`}
      className="rounded-xl border border-zinc-200 p-4 text-sm text-zinc-700"
    >
      {note}
    </div>
  ))
) : (
<p className="text-sm text-zinc-600">
  No structured rewrite actions were generated. The market is broadly valid, but the operator should still review timing language, source hierarchy, and settlement criteria before launch.
</p>
)}
          </div>
        </section>

<section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
  <p className="text-sm font-medium text-zinc-500">
    Split and consistency
  </p>

  <div className="mt-4 space-y-4">
    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Split recommendation
      </p>
      <p className="mt-2 text-sm font-medium text-zinc-900">
        {rewriteResult?.split_recommendation?.should_split
          ? `Yes — ${formatLabel(
              rewriteResult.split_recommendation?.suggested_split_type
            )}`
          : "No split recommended"}
      </p>

      {rewriteResult?.split_recommendation?.rationale?.length ? (
        <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-zinc-600">
          {rewriteResult.split_recommendation.rationale.map((item, idx) => (
            <li key={idx}>{item}</li>
          ))}
        </ul>
      ) : null}
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Failure modes
      </p>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-zinc-600">
        {designResult?.resolution_risk_band === "high" && (
          <li>Ambiguous outcome definitions may produce conflicting interpretations at settlement.</li>
        )}
        {designResult?.dispute_propensity_band === "high" && (
          <li>High oracle dispute likelihood due to unclear verification or source logic.</li>
        )}
{designResult?.resolution_risk_band === "manageable" && (
  <li>
    Edge cases around timing or source interpretation may require manual resolution judgment.
  </li>
)}
        {!designResult && <li>Run Launch Review to surface likely failure modes.</li>}
      </ul>
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Consistency check
      </p>
      <p className="mt-2 text-sm font-medium text-zinc-900">
        {formatLabel(rewriteResult?.consistency_status)}
      </p>
      <p className="mt-1 text-sm text-zinc-600">
        Score: {formatNumber(rewriteResult?.consistency_score)}
      </p>
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Simulated oracle flow
      </p>
      <p className="mt-2 text-sm leading-7 text-zinc-600">
        {simulationResult?.simulation?.resolution_path
          ? `${formatResolutionPath(
              simulationResult.simulation.resolution_path
            )} with estimated settlement time of ${
              simulationResult.simulation.expected_settlement_hours ?? "—"
            } hours and ${formatConfidenceBand(
              simulationResult.simulation.confidence
            ).toLowerCase()}.`
          : "No simulation result available yet."}
      </p>
    </div>

    <div className="rounded-xl border border-zinc-200 p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-zinc-500">
        Launch note
      </p>
      <p className="mt-2 text-sm leading-7 text-zinc-600">
        {rewriteResult?.launch_decision_note ||
          "No launch note available yet."}
      </p>
    </div>
  </div>
</section>
      </div>
    </div>
  );
}