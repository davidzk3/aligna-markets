import Link from "next/link";

type CandidateCardProps = {
  marketId: string;
  title?: string | null;
  category?: string | null;
  structuralState?: string | null;
  socialSignal?: string | null;
  alignmentState?: string | null;
  interventionNeeded?: boolean | null;
  recommendedAction?: string | null;
  actionPriority?: string | null;
  actionReason?: string | null;
  incentiveDependency?: string | null;
  activityQuality?: string | null;
  expectedFailureMode?: string | null;
  hasContextualSummary?: boolean;
  scoreLabel: string;
  scoreValue: string;
  summary?: string | null;
  flags?: string[] | null;
  url?: string | null;
};

function getStructuralBadgeClass(value?: string | null) {
  switch (value) {
    case "strong":
      return "bg-emerald-100 text-emerald-700";
    case "moderate":
      return "bg-amber-100 text-amber-700";
    case "weak":
      return "bg-rose-100 text-rose-700";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function getDemandBadgeClass(value?: string | null) {
  switch (value) {
    case "strong":
      return "bg-emerald-100 text-emerald-700";
    case "established":
      return "bg-blue-100 text-blue-700";
    case "building":
      return "bg-violet-100 text-violet-700";
    case "limited":
      return "bg-amber-100 text-amber-700";
    case "absent":
      return "bg-zinc-100 text-zinc-600";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function getAlignmentBadgeClass(value?: string | null) {
  switch (value) {
    case "confirmed":
      return "bg-emerald-100 text-emerald-700";
    case "structure_led":
      return "bg-blue-100 text-blue-700";
    case "conviction_mismatch":
      return "bg-orange-100 text-orange-700";
    case "weak":
      return "bg-zinc-100 text-zinc-600";
    default:
      return "bg-zinc-100 text-zinc-700";
  }
}

function formatFailureMode(value?: string | null) {
  if (!value) return null;
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatAction(value?: string | null) {
  if (!value) return null;
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatLabel(value?: string | null) {
  if (!value) return null;
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function Badge({
  children,
  className,
  primary = false,
}: {
  children: React.ReactNode;
  className: string;
  primary?: boolean;
}) {
  return (
    <span
      className={`inline-flex items-center rounded-full whitespace-nowrap font-medium ${
        primary ? "px-3 py-1.5 text-xs" : "px-2.5 py-1 text-[11px]"
      } ${className}`}
    >
      {children}
    </span>
  );
}

export default function CandidateCard({
  marketId,
  title,
  category,
  structuralState,
  socialSignal,
  alignmentState,
  interventionNeeded,
  recommendedAction,
  actionPriority,
  actionReason,
  incentiveDependency,
  activityQuality,
  expectedFailureMode,
  scoreLabel,
  scoreValue,
  summary,
  flags,
  url,
}: CandidateCardProps) {
  const detailHref = {
    pathname: `/markets/${marketId}`,
    query: {
      title: title || "",
      url: url || "",
    },
  };

  const showActionBlock =
    interventionNeeded === true &&
    (recommendedAction ||
      actionPriority ||
      actionReason ||
      incentiveDependency ||
      activityQuality ||
      expectedFailureMode);

  return (
    <div className="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm transition-shadow hover:shadow-md">
      <div className="space-y-3">
        <div className="flex items-start justify-between gap-4">
          <h3 className="line-clamp-2 text-lg font-semibold leading-6 text-zinc-900">
            {title || marketId}
          </h3>

          {alignmentState ? (
            <Badge
              className={getAlignmentBadgeClass(alignmentState)}
              primary
            >
              {formatLabel(alignmentState)}
            </Badge>
          ) : null}
        </div>

        {category ? (
          <p className="text-xs text-zinc-500">{category}</p>
        ) : null}

        <div className="flex flex-wrap gap-2">
          {structuralState ? (
            <Badge className={getStructuralBadgeClass(structuralState)}>
              Structural: {formatLabel(structuralState)}
            </Badge>
          ) : null}

          {socialSignal ? (
            <Badge className={getDemandBadgeClass(socialSignal)}>
              Demand: {formatLabel(socialSignal)}
            </Badge>
          ) : null}
        </div>
      </div>

      <div className="mt-4 space-y-3">
        <p className="text-sm text-zinc-600">
          <span className="font-medium text-zinc-800">{scoreLabel}:</span>{" "}
          {scoreValue}
        </p>

        {summary ? (
          <p className="line-clamp-3 text-sm leading-6 text-zinc-600">
            {summary}
          </p>
        ) : null}

        {showActionBlock ? (
          <div className="rounded-xl border border-zinc-200 bg-zinc-50 p-3">
            {recommendedAction ? (
              <p className="text-sm font-semibold text-zinc-900">
                {formatAction(recommendedAction)}
              </p>
            ) : null}

            {actionReason ? (
              <p className="mt-1 text-xs leading-5 text-zinc-600">
                {actionReason}
              </p>
            ) : null}

            <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-zinc-500">
              {actionPriority ? (
                <span>Priority: {formatLabel(actionPriority)}</span>
              ) : null}

              {expectedFailureMode ? (
                <span>Risk: {formatFailureMode(expectedFailureMode)}</span>
              ) : null}

              {incentiveDependency ? (
                <span>Incentive: {formatLabel(incentiveDependency)}</span>
              ) : null}

              {activityQuality ? (
                <span>Activity: {formatLabel(activityQuality)}</span>
              ) : null}
            </div>
          </div>
        ) : null}

        {flags && flags.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {flags.slice(0, 3).map((flag) => (
              <span
                key={flag}
                className="rounded-full border border-zinc-200 px-2.5 py-1 text-[11px] text-zinc-500"
              >
                {flag}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <div className="mt-5 flex items-center gap-4 text-sm font-medium">
        <Link href={detailHref} className="text-blue-600 hover:underline">
          View detail
        </Link>

        {url ? (
          <a
            href={url}
            target="_blank"
            rel="noreferrer"
            className="text-zinc-700 hover:underline"
          >
            Open market
          </a>
        ) : null}
      </div>
    </div>
  );
}
