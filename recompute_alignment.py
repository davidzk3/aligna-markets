from apps.api.services.market_alignment_intelligence import compute_market_alignment_daily

if __name__ == "__main__":
    result = compute_market_alignment_daily(
        day=None,
        market_id=None,
        limit_markets=2000,
        horizon_mode="mixed_latest",
    )
    print(result)