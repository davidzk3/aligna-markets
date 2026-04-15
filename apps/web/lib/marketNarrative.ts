// PURE PRESENTATION LAYER ONLY
// No business logic, no interpretation, no state derivation

function uniqueStrings(values: Array<string | null | undefined>): string[] {
  return Array.from(new Set(values.filter((v): v is string => Boolean(v))));
}

export function mapFlagLabel(flag: string): string {
  switch (flag) {
    case "STRONG_NEUTRAL_BASE":
      return "strong neutral base";

    case "DEMAND_STRONG":
      return "demand strong";
    case "DEMAND_ESTABLISHED":
      return "demand established";
    case "DEMAND_BUILDING":
      return "demand building";
    case "DEMAND_LIMITED":
      return "demand limited";
    case "DEMAND_ABSENT":
      return "demand absent";

    case "DEMAND_AHEAD_OF_STRUCTURE":
      return "demand ahead of structure";
    case "DEMAND_LAGGING_STRUCTURE":
      return "demand lagging structure";
    case "STRUCTURE_DEMAND_CONFIRMED":
      return "structure demand confirmed";
    case "LARGE_ALIGNMENT_GAP":
      return "large alignment gap";

    case "RECENT_WHALE_PARTICIPATION_PRESENT":
      return "recent whale participation";
    case "RECENT_SPECULATIVE_FLOW_PRESENT":
      return "recent speculative flow";
    case "NEUTRAL_SHARE_WEAKER_IN_RECENT_WINDOW":
      return "neutral share weaker recently";
    case "LATEST_DAY_VS_RECENT_WINDOW_DIVERGENCE":
      return "latest day vs recent window divergence";

    case "LOW_CONFIDENCE_PROXY":
      return "low confidence proxy";
    case "SOURCE_CONCENTRATION_RISK":
      return "source concentration risk";

    case "mixed_horizon":
      return "mixed horizon";

    default:
      return flag.toLowerCase().replace(/_/g, " ");
  }
}

export function mapFlagsForDisplay(flags?: string[] | null): string[] {
  if (!flags || flags.length === 0) return [];
  return uniqueStrings(flags.map(mapFlagLabel));
}

/**
 * Optional lightweight participant flags (DISPLAY ONLY)
 * No decision-making, just descriptive labeling
 */
export function mapParticipantFlags(params: {
  neutralShare?: number | null;
  whaleShare?: number | null;
  speculativeShare?: number | null;
  participationQuality?: number | null;
}): string[] {
  const {
    neutralShare = null,
    whaleShare = null,
    speculativeShare = null,
    participationQuality = null,
  } = params;

  const out: string[] = [];

  if (
    neutralShare !== null &&
    whaleShare !== null &&
    speculativeShare !== null &&
    neutralShare >= 0.7 &&
    whaleShare <= 0.2 &&
    speculativeShare <= 0.15
  ) {
    out.push("strong neutral base");
  }

  if (whaleShare !== null && whaleShare >= 0.3) {
    out.push("elevated whale share");
  }

  if (speculativeShare !== null && speculativeShare >= 0.25) {
    out.push("elevated speculative share");
  }

  if (participationQuality !== null && participationQuality < 0.5) {
    out.push("weak participation quality");
  }

  return uniqueStrings(out);
}