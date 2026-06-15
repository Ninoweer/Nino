SELECT
  current_channel,
  next_channel,
  pct_next_channel,
  pct_next_channel_percent,
  transition_count,
  total_next_channel_transitions_from_current,
  journeys,
  orders,
  revenue,
  most_likely_next_channel,
  is_most_likely_next_channel,
  next_channel_rank
FROM `action-dwh.attribution_model_ecom.mta_next_channel_probability_20260528_20260610`
ORDER BY
  current_channel,
  next_channel_rank,
  next_channel;
