import Link from "next/link";

function Pill({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <span className="inline-flex items-center rounded-full border border-zinc-200 bg-white px-3 py-1 text-xs font-medium text-zinc-700">
      {children}
    </span>
  );
}

export default function LandingPage() {
  return (
    <div className="mx-auto max-w-6xl space-y-16">
      <section className="rounded-3xl border border-zinc-200 bg-white px-8 py-14 shadow-sm">
        <div className="max-w-4xl space-y-6">
          <div className="flex flex-wrap gap-2">
            <Pill>Prediction Markets</Pill>
            <Pill>Lifecycle Intelligence</Pill>
            <Pill>Resolution</Pill>
            <Pill>Learning Loop</Pill>
          </div>


          <div className="space-y-4">
            <p className="text-sm font-medium uppercase tracking-[0.18em] text-zinc-500">
              FULL LIFECYCLE INTELLIGENCE FOR PREDICTION MARKETS
            </p>

            <h1 className="max-w-4xl text-4xl font-semibold leading-tight text-zinc-950 md:text-5xl">
              Design, evaluate, simulate, and learn from prediction markets across the full lifecycle.
            </h1>

            <p className="max-w-3xl text-lg leading-8 text-zinc-600">
              Aligna Markets is a full lifecycle prediction market intelligence system. It evaluates market design, live market structure, external demand, participant quality, intervention needs, oracle resolution risk, and post-resolution learning.
            </p>
          </div>

          <div className="flex flex-wrap gap-4">
            <Link
              href="/explorer"
              className="inline-flex items-center rounded-xl bg-zinc-900 px-5 py-3 text-sm font-semibold text-white hover:bg-zinc-800"
            >
              Open Explorer
            </Link>

            <Link
              href="/methodology"
              className="inline-flex items-center rounded-xl border border-zinc-200 bg-white px-5 py-3 text-sm font-semibold text-zinc-900 hover:bg-zinc-50"
            >
              View Methodology
            </Link>
          </div>
        </div>
      </section>

      <section className="grid gap-6 md:grid-cols-3">
        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <p className="text-sm font-medium text-zinc-500">Problem</p>
          <h2 className="mt-2 text-xl font-semibold text-zinc-900">
            A market is more than a price chart.
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            A market can look active while still being fragile, misaligned, poorly specified, or risky to resolve.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <p className="text-sm font-medium text-zinc-500">What Aligna Measures</p>
          <h2 className="mt-2 text-xl font-semibold text-zinc-900">
            Design, live trading, resolution, and learning signals.
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Aligna evaluates market design quality, live microstructure, external demand, cohort participation, intervention candidates, simulated resolution paths, and the learning loop between prediction and actual outcome.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <p className="text-sm font-medium text-zinc-500">Operator Output</p>
          <h2 className="mt-2 text-xl font-semibold text-zinc-900">
            An operating system, not just a dashboard.
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            The product supports launch review, live market triage, intervention reasoning, oracle-aware resolution analysis, and post-resolution evaluation so protocols can operate markets as engineered systems.
          </p>
        </div>
      </section>

      <section className="rounded-3xl border border-zinc-200 bg-white px-8 py-10 shadow-sm">
        <div className="max-w-4xl space-y-6">
          <div>
            <p className="text-sm font-medium uppercase tracking-[0.18em] text-zinc-500">
              LIFECYCLE COVERAGE
            </p>
            <h2 className="mt-2 text-3xl font-semibold text-zinc-900">
              Each market is evaluated across its lifecycle.
            </h2>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="rounded-2xl border border-emerald-200 bg-emerald-50 p-5">
              <h3 className="text-lg font-semibold text-emerald-900">
                Before launch
              </h3>
              <p className="mt-2 text-sm leading-7 text-emerald-800">
                Evaluate design clarity, rewrite needs, launch readiness, and dispute risk before a market goes live.
              </p>
            </div>

            <div className="rounded-2xl border border-amber-200 bg-amber-50 p-5">
              <h3 className="text-lg font-semibold text-amber-900">
                While live
              </h3>
              <p className="mt-2 text-sm leading-7 text-amber-800">
                Track structure, demand, alignment, participant quality, and intervention candidates while the market is actively trading.
              </p>
            </div>

            <div className="rounded-2xl border border-blue-200 bg-blue-50 p-5">
              <h3 className="text-lg font-semibold text-blue-900">
                At resolution
              </h3>
              <p className="mt-2 text-sm leading-7 text-blue-800">
                Assess oracle-facing fragility, simulate likely resolution paths, and identify markets that may be disputed, delayed, or require manual judgment.
              </p>
            </div>

            <div className="rounded-2xl border border-zinc-200 bg-zinc-50 p-5">
              <h3 className="text-lg font-semibold text-zinc-900">After outcome</h3>
              <p className="mt-2 text-sm leading-7 text-zinc-700">
                Compare predicted vs actual resolution behavior, measure timing error, and build a learning loop that improves future design, simulation, and intervention decisions.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-3xl border border-zinc-200 bg-white px-8 py-10 shadow-sm">
        <div className="max-w-4xl space-y-6">
          <div>
            <p className="text-sm font-medium uppercase tracking-[0.18em] text-zinc-500">
              INTERVENTION AND RESOLUTION LAYER
            </p>
            <h2 className="mt-2 text-3xl font-semibold text-zinc-900">
              Not every weak market deserves support, and not every market deserves launch.
            </h2>
          </div>

          <p className="max-w-3xl text-sm leading-7 text-zinc-600">
            Aligna helps protocols decide when to launch, when to rewrite, when to support, when to avoid subsidy, how a market may behave in the oracle workflow, and how to learn from actual outcomes after settlement.
          </p>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="rounded-2xl border border-zinc-200 p-5">
              <h3 className="text-base font-semibold text-zinc-900">
                Example operator actions
              </h3>
              <ul className="mt-3 space-y-2 text-sm text-zinc-600">
                <li>Launch with edits</li>
                <li>Rewrite before launch</li>
                <li>Add targeted liquidity</li>
                <li>Do not subsidize yet</li>
                <li>Monitor and learn</li>
              </ul>
            </div>

            <div className="rounded-2xl border border-zinc-200 p-5">
              <h3 className="text-base font-semibold text-zinc-900">
                Example Lifecycle risks
              </h3>
              <ul className="mt-3 space-y-2 text-sm text-zinc-600">
                <li>Oracle ambiguity</li>
                <li>One-sided liquidity risk</li>
                <li>Artificial or distorted participation</li>
                <li>Prediction vs reality mismatch</li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-3xl border border-zinc-200 bg-zinc-900 px-8 py-10 text-white shadow-sm">
        <div className="max-w-4xl space-y-5">
          <p className="text-sm font-medium uppercase tracking-[0.18em] text-zinc-400">
            START WITH THE PRODUCT
          </p>

          <h2 className="text-3xl font-semibold">
            Review markets across launch, live trading, resolution, and learning.
          </h2>

          <p className="max-w-3xl text-sm leading-7 text-zinc-300">
            Use Launch Review for draft markets, Explorer for live market triage, and detailed market pages for intervention, participant, and oracle-aware reasoning.
          </p>

          <div className="flex flex-col sm:flex-row items-center gap-4">
  <Link
    href="/launch-review"
    className="inline-flex items-center rounded-xl bg-white px-5 py-3 text-sm font-semibold text-zinc-900 hover:bg-zinc-100"
  >
    Launch Review
  </Link>

  <Link
    href="/explorer"
    className="inline-flex items-center rounded-xl border border-white/20 px-5 py-3 text-sm font-semibold text-white hover:bg-white/10"
  >
    Explore Markets
  </Link>
</div>
        </div>
      </section>
    </div>
  );
}

