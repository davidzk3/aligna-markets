export default function MethodologyPage() {
  return (
    <div className="mx-auto max-w-5xl space-y-8">
      <section className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-medium text-zinc-500">Methodology</p>
        <h1 className="mt-1 text-2xl font-semibold text-zinc-900">
          How Aligna evaluates markets
        </h1>
        <p className="mt-3 max-w-3xl text-sm leading-7 text-zinc-600">
          Aligna Markets evaluates prediction markets as systems. The goal is
          not only to observe pricing, but to understand whether the market is
          functioning properly, whether external attention is backed by real
          participation, and whether intervention is justified.
        </p>
      </section>

      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Structural intelligence
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Measures market quality using liquidity health, spread behavior,
            concentration, durability, and participation distribution.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Demand intelligence
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Measures external attention, demand state, signal persistence, and
            whether credible external interest exists around the market.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Alignment intelligence
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Compares market structure and demand to determine whether attention
            is supported by participation or whether the market is structurally
            lagging, weak, or ahead of current demand.
          </p>
        </div>

        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h2 className="text-lg font-semibold text-zinc-900">
            Intervention intelligence
          </h2>
          <p className="mt-3 text-sm leading-7 text-zinc-600">
            Flags intervention candidates and proposes first-pass action
            hypotheses such as targeted liquidity, maker support, redesign, or
            no subsidy yet. These recommendations are review inputs, not final
            operator decisions.
          </p>
        </div>
      </section>
    </div>
  );
}