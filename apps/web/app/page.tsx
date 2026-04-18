import Link from "next/link";

const useCases = [
  {
    title: "Launch intelligence",
    body: "Review market wording, ambiguity, source hierarchy, timing logic, rewrite needs, and expected dispute exposure before a market goes live.",
  },
  {
    title: "Live market triage",
    body: "Track structure, social demand, participation quality, alignment, and intervention pressure while markets are active.",
  },
  {
    title: "Support intelligence",
    body: "Identify when support is likely needed, which failure mode is present, and whether intervention is likely to improve market quality.",
  },
  {
    title: "Resolution learning",
    body: "Compare predicted versus actual settlement behavior so launch review, simulation, and intervention logic improve over time.",
  },
] as const;

const lifecycle = [
  {
    title: "Before launch",
    body: "Review wording, resolution safety, expected dispute risk, and likely support requirements before a market goes live.",
  },
  {
    title: "While live",
    body: "Measure whether demand, structure, and participation are combining into a healthy market or drifting toward fragility and support dependence.",
  },
  {
    title: "After outcome",
    body: "Persist what happened, compare prediction versus reality, and feed those learnings back into future launch and support decisions.",
  },
] as const;

function Pill({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex rounded-full border border-black/10 bg-white/70 px-3 py-1 text-xs font-medium tracking-[0.08em] text-neutral-700 backdrop-blur">
      {children}
    </span>
  );
}

function SectionEyebrow({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-xs font-semibold uppercase tracking-[0.24em] text-neutral-500">
      {children}
    </p>
  );
}

function CoveragePill({
  children,
  variant = "default",
}: {
  children: React.ReactNode;
  variant?: "default" | "coming_soon";
}) {
  const styles =
    variant === "coming_soon"
      ? "border border-indigo-200 bg-indigo-50 text-indigo-700"
      : "border border-emerald-200 bg-emerald-50 text-emerald-700";

  return (
    <span
      className={`inline-flex items-center rounded-full px-3.5 py-1.5 text-xs font-semibold tracking-[0.04em] ${styles}`}
    >
      {children}
    </span>
  );
}

export default function HomePage() {
  return (
    <main className="min-h-screen bg-[#f7f5f1] text-neutral-950">
      <section className="border-b border-black/8 bg-[radial-gradient(circle_at_top,#efe7ff_0%,#f7f5f1_42%,#f7f5f1_100%)]">
        <div className="mx-auto max-w-7xl px-6 py-20 sm:px-8 lg:px-10 lg:py-24">
          <div className="grid gap-14 lg:grid-cols-[1.05fr_0.95fr] lg:items-center">
            <div>
              <div className="mb-6 flex flex-wrap gap-2">
                <Pill>Prediction Markets</Pill>
                <Pill>Lifecycle Intelligence</Pill>
                <Pill>Launch + Live + Learning</Pill>
              </div>

              <SectionEyebrow>Aligna Markets</SectionEyebrow>

              <h1 className="mt-5 max-w-5xl text-5xl font-semibold leading-[1.02] tracking-[-0.04em] text-neutral-950 sm:text-6xl lg:text-7xl">
                Design, monitor, simulate, and learn from prediction markets.
              </h1>

              <p className="mt-6 max-w-2xl text-lg leading-8 text-neutral-700 sm:text-xl">
                Aligna Markets is a full lifecycle prediction market intelligence
                system. It helps protocols review markets before launch, monitor
                live market quality, understand support and incentive dynamics,
                and post-resolution learning.
              </p>

              <div className="mt-7 flex flex-wrap items-center gap-2.5">
                <CoveragePill>Current coverage: Polymarket</CoveragePill>
                <CoveragePill variant="coming_soon">
                  Kalshi integration coming soon
                </CoveragePill>
              </div>

              <p className="mt-4 max-w-2xl text-sm leading-7 text-neutral-600">
                Live market ingestion, structure scoring, and operator triage are
                currently powered by Polymarket market data.
              </p>

              <div className="mt-8 flex flex-wrap gap-3">
                <Link
                  href="/launch-review"
                  className="inline-flex items-center rounded-full bg-neutral-950 px-6 py-3 text-sm font-semibold text-white transition hover:opacity-90"
                >
                  Launch Review
                </Link>
                <Link
                  href="/explorer"
                  className="inline-flex items-center rounded-full border border-black/10 bg-white px-6 py-3 text-sm font-semibold text-neutral-900 transition hover:bg-neutral-50"
                >
                  Open Explorer
                </Link>
              </div>

              <p className="mt-6 max-w-2xl text-sm leading-7 text-neutral-600">
                Start with Launch Review for draft markets. Use Explorer for
                live market triage and operator monitoring.
              </p>
            </div>

            <div className="rounded-[30px] border border-black/8 bg-white p-5 shadow-[0_20px_80px_rgba(15,23,42,0.08)] sm:p-6">
              <div className="rounded-[24px] border border-black/8 bg-neutral-950 p-5 font-mono text-[13px] leading-6 text-white/85">
                <div className="text-white/40">aligna.market_snapshot</div>

                <div className="mt-3 text-white">market_id: m_74294b4d75</div>
                <div>market: Will the Fed cut rates by June?</div>
                <div>structural_state: moderate</div>
                <div>demand_state: established</div>
                <div>alignment_state: conviction_mismatch</div>
                <div>support_state: review_required</div>
                <div>recommended_action: add_targeted_liquidity</div>
                <div>learning_status: tracking</div>

                <div className="mt-4 border-t border-white/10 pt-4 text-white/70">
                  Demand is present, but market quality is not yet converting
                  into credible participation.
                </div>
              </div>

              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl border border-black/8 bg-[#faf8f4] p-4">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-neutral-500">
                    Live snapshot
                  </div>
                  <div className="mt-2 min-h-[72px] text-sm leading-6 text-neutral-700">
                    Real-time signals show how structure, social demand, participation, and intervention pressure are evolving, so teams can define and execute the right support strategy.
                  </div>
                </div>

                <div className="rounded-2xl border border-black/8 bg-[#faf8f4] p-4">
                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-neutral-500">
                    Learning loop
                  </div>
                  <div className="mt-2 min-h-[72px] text-sm leading-6 text-neutral-700">
                    The system records how markets behave across their full lifecycle then feeds those learnings back into future market design, support, and intervention strategy.
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-6 py-20 sm:px-8 lg:px-10">
        <div className="grid gap-10 lg:grid-cols-[0.85fr_1.15fr] lg:items-start">
          <div>
            <SectionEyebrow>What the system does</SectionEyebrow>
            <h2 className="mt-4 max-w-2xl text-4xl font-semibold tracking-[-0.04em] text-neutral-950 sm:text-5xl">
              One operating layer across launch, live markets, and learning.
            </h2>
            <p className="mt-5 max-w-xl text-base leading-8 text-neutral-700">
              The product connects market design, live monitoring, support
              decisions, and eventual outcomes into one continuous system.
            </p>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            {useCases.map((item) => (
              <div
                key={item.title}
                className="rounded-[26px] border border-black/8 bg-white p-6 shadow-sm"
              >
                <div className="text-xl font-semibold tracking-[-0.03em] text-neutral-950">
                  {item.title}
                </div>
                <p className="mt-3 min-h-[112px] text-sm leading-7 text-neutral-700">
                  {item.body}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-6 pb-20 sm:px-8 lg:px-10">
        <div className="rounded-[34px] border border-black/8 bg-white p-8 shadow-sm sm:p-10">
          <SectionEyebrow>Lifecycle coverage</SectionEyebrow>

          <div className="mt-4 flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <h2 className="max-w-3xl text-4xl font-semibold tracking-[-0.04em] text-neutral-950 sm:text-5xl">
              The product is designed to learn, not just score.
            </h2>
            <p className="max-w-xl text-base leading-8 text-neutral-700">
              Launch review, live triage, and outcome evaluation are part of the
              same operating system.
            </p>
          </div>

          <div className="mt-10 grid gap-4 lg:grid-cols-3">
            {lifecycle.map((item, index) => (
              <div
                key={item.title}
                className="rounded-[28px] border border-black/8 bg-[#faf8f4] p-6"
              >
                <div className="text-xs font-semibold uppercase tracking-[0.24em] text-neutral-500">
                  {String(index + 1).padStart(2, "0")}
                </div>
                <div className="mt-4 text-2xl font-semibold tracking-[-0.03em] text-neutral-950">
                  {item.title}
                </div>
                <p className="mt-4 min-h-[140px] text-sm leading-7 text-neutral-700">
                  {item.body}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="mx-auto max-w-7xl px-6 pb-24 sm:px-8 lg:px-10">
        <div className="rounded-[34px] border border-black/8 bg-gradient-to-br from-neutral-950 via-[#14182b] to-[#0a0f1d] p-8 text-white shadow-[0_30px_90px_rgba(15,23,42,0.14)] sm:p-10">
          <div className="grid gap-10 lg:grid-cols-[1fr_0.92fr] lg:items-center">
            <div>
              <SectionEyebrow>Start with the product</SectionEyebrow>

              <h2 className="mt-4 max-w-3xl text-4xl font-semibold tracking-[-0.04em] text-white sm:text-5xl">
                Review draft markets, triage live ones, and learn from real
                outcomes.
              </h2>

              <p className="mt-5 max-w-2xl text-base leading-8 text-white/70">
                Use Launch Review for pre-launch market design and settlement
                safety. Use Explorer and market detail pages to understand
                whether live market activity is healthy, support-dependent, or
                becoming distorted.
              </p>

              <div className="mt-8 flex flex-wrap gap-3">
                <Link
                  href="/launch-review"
                  className="inline-flex items-center rounded-full bg-white px-6 py-3 text-sm font-semibold text-neutral-950 transition hover:bg-neutral-100"
                >
                  Review a market
                </Link>
                <Link
                  href="/explorer"
                  className="inline-flex items-center rounded-full border border-white/15 px-6 py-3 text-sm font-semibold text-white transition hover:bg-white/5"
                >
                  Explore live markets
                </Link>
              </div>
            </div>

            <div className="rounded-[28px] border border-white/10 bg-white/[0.04] p-5 font-mono text-[13px] leading-6 text-white/80">
              <div className="text-white/45">aligna.learning_loop</div>
              <div className="mt-2">launch_review: complete</div>
              <div>simulation_run_id: 1</div>
              <div>predicted_resolution_path: clean_resolution</div>
              <div>actual_resolution_path: clean_resolution</div>
              <div>prediction_correct: true</div>
              <div>timing_error_hours: 1.0</div>
              <div>evaluation_status: persisted</div>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}