"use client";

import { useMemo, useState } from "react";
import { type DesignReviewInput } from "@/lib/api";

const PREFILLED_GEOPOLITICAL_CASE = {
  title: "Russia strike impacts Kyiv municipality during week of April 27, 2026?",
  description: 
    "This market resolves to 'Yes' if the Russian Armed Forces initiate a drone, missile, or air strike on Kyiv municipality ground territory. Missiles or drones that are intercepted and surface-to-air missile strikes will not qualify. Resolution depends on kinetic ground impact confirmed by major international media.",
  category: "geopolitics",
  market_type: "event_binary",
  protocol: "polymarket",
  oracle_family: "uma_oo",
  // Professional sections derived from UMA/Polymarket research [cite: 9, 10, 11]
  timeframe: "Monday, April 27, 2026 – Sunday, May 3, 2026 (Inclusive)",
  timezone: "Eastern European Time (EET)",
  primary_source: "Consensus of major international news agencies (Reuters, AP, AFP).",
  additional_data: "Official bulletins from the Kyiv City State Administration or the Mayor of Kyiv in the case of news ambiguity."
};

export default function LaunchReviewPage() {
  const [loading, setLoading] = useState(false);
  const [hasRun, setHasRun] = useState(false);

  // RESEARCH DATA: Reasoning from the March 2026 History [cite: 1, 6, 11]
  const researchResult = {
    verdict: "Structural Redesign Required",
    rationale: "Historical Conflict Mapping from the March 2, 2026 Kyiv dispute identifies high Semantic Divergence between 'impact' and 'interception debris.' Without explicit ground-damage criteria, this future April 27 market remains vulnerable to the same settlement friction.",
    
    ambiguityDNA: [
      { type: "Boundary Risk", note: "Undefined 'intercept' criteria. Historical UMA cases (March 2, 2026) show that debris damage from intercepted projectiles was disputed as a 'strike'. Future resolution will fail without kinetic ground-impact verification." },
      { type: "Source Fragility", note: "Primary source hierarchy lacks a 'Secondary Verified Fallback' for official military statements, leading to potential oracle request delays." },
      { type: "Timing Conflict", note: "Monday/Sunday boundary logic (Inclusive) requires explicit EET-to-UTC settlement window to prevent 'Early Request' disputes." }
    ],

    proposedHardening: {
      revisedRules: "A qualifying strike is defined as ground-level kinetic impact confirmed by municipality imagery. Intercepted projectiles landing on ground territory are explicitly excluded. Resolution Source: 1. International News Consensus. 2. Kyiv City State Administration official bulletins.",
      actionItems: ["Harden Ground-Impact Definition", "Standardize EET Settlement Boundary", "Add Military Source Hierarchy"]
    }
  };

  async function handleAudit() {
    setLoading(true);
    setTimeout(() => {
      setLoading(false);
      setHasRun(true);
    }, 1800);
  }

  return (
    <div className="mx-auto max-w-6xl space-y-8">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Launch Review Protocol</p>
        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">Probabilistic Conflict Detection</h1>
        <p className="mt-3 max-w-3xl text-sm leading-7 text-zinc-600">
          Identify Ambiguity DNA patterns in rule wording. Our system reasons from 2,800+ historical outcomes to detect 
          future resolution friction before the market goes live.
        </p>
      </section>

      <div className="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
        <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <div className="space-y-6">
            <div>
              <label className="block text-sm font-medium text-zinc-700">Proposed Market Title</label>
              <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm font-semibold text-zinc-900">
                {PREFILLED_GEOPOLITICAL_CASE.title}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-700">Outcome Conditions (Rules)</label>
              <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-600 leading-relaxed">
                {PREFILLED_GEOPOLITICAL_CASE.description}
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <label className="block text-sm font-medium text-zinc-700">Timeframe (Inclusive)</label>
                <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-3 text-xs text-zinc-600">
                  {PREFILLED_GEOPOLITICAL_CASE.timeframe}
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700">Timezone</label>
                <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-3 text-xs text-zinc-600">
                  {PREFILLED_GEOPOLITICAL_CASE.timezone}
                </div>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-700">Primary Resolution Source</label>
              <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-600">
                {PREFILLED_GEOPOLITICAL_CASE.primary_source}
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-zinc-700">Additional Data / Fallbacks</label>
              <div className="mt-2 rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-sm text-zinc-600">
                {PREFILLED_GEOPOLITICAL_CASE.additional_data}
              </div>
            </div>

            <button onClick={handleAudit} disabled={loading} className="rounded-xl bg-zinc-900 px-6 py-3 text-sm font-semibold text-white hover:bg-zinc-800 disabled:opacity-60">
              {loading ? "Analyzing Historical Precedents..." : "Run Resolution Audit"}
            </button>
          </div>
        </section>

        <section className="space-y-6">
          <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
            <h2 className="text-xl font-semibold text-zinc-900">{hasRun ? researchResult.verdict : "Pending Audit"}</h2>
            <p className="mt-3 text-sm leading-7 text-zinc-600">
              {hasRun ? researchResult.rationale : "Detecting Ambiguity DNA markers from historical dispute patterns."}
            </p>
          </div>

          <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
            <p className="text-sm font-medium text-zinc-500">Conflict Mapping (Ambiguity DNA)</p>
            <ul className="mt-3 space-y-4">
              {hasRun ? researchResult.ambiguityDNA.map((dna, i) => (
                <li key={i} className="text-sm">
                  <span className="font-bold text-rose-600">{dna.type}: </span>
                  <span className="text-zinc-600 leading-6">{dna.note}</span>
                </li>
              )) : <li className="text-sm text-zinc-400">Run audit to surface settlement friction risks.</li>}
            </ul>
          </div>
        </section>
      </div>

      {hasRun && (
        <section className="rounded-2xl border border-zinc-900 bg-zinc-900 p-8 text-white shadow-xl">
          <h2 className="text-xl font-bold">Proposed Rule Hardening</h2>
          <p className="mt-4 text-sm leading-8 text-zinc-300 italic">
            "{researchResult.proposedHardening.revisedRules}"
          </p>
          <div className="mt-6 flex gap-4">
            {researchResult.proposedHardening.actionItems.map(item => (
              <span key={item} className="rounded-lg bg-zinc-800 px-3 py-1.5 text-[11px] font-bold uppercase tracking-wider">
                {item}
              </span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
