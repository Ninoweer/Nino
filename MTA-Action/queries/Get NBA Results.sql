SELECT
  current_channel,
  next_channel,
  most_likely_next_channel,
  is_most_likely_next_channel,
  next_channel_rank,
  pct_next_channel_percent,
  transition_count,
  journeys,
  orders,
  revenue
FROM `action-dwh.attribution_model_ecom.mta_next_channel_probability_20260528_20260610`
ORDER BY
  current_channel,
  next_channel_rank,
  next_channel;
