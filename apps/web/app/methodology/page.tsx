export default function MethodologyPage() {
  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Methodology</p>
        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          The Aligna Framework: Resolution-First Intelligence
        </h1>
        <p className="mt-3 max-w-3xl text-sm leading-7 text-zinc-600">
          Aligna Markets treats prediction markets as interactive settlement systems. 
          Our methodology focuses on <strong>Settlement Safety</strong>, ensuring that every market 
          has a deterministic path to resolution by identifying ambiguity before it manifests as a protocol dispute.
        </p>
      </section>

      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Structural Durability
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Evaluates the underlying participation quality. We look for maker-like consistency and organic 
            distribution to ensure a market can withstand volatility without collapsing into whale-driven distortion.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Conviction Analysis
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Analyzes external demand for tradable signal. We prioritize <strong>Durable Conviction</strong> over noisy chatter, identifying when attention is a reliable indicator of intent versus speculative narrative exhaust.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Conflict Mapping
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Compares market rules against 2,800+ historical UMA resolutions. We identify Ambiguity DNA patterns in rule wording that have historically led to settlement friction to harden the resolution path before launch.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            The Learning Loop
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Our models are updated daily with actual protocol settlement behavior. By closing the gap between 
            predicted paths and actual DVM outcomes, the system provides a research-led feedback loop.
          </p>
        </div>
      </section>
    </div>
  );
}
