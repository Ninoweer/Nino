SELECT
  current_channel,
  most_likely_next_action,
  transition_count,
  ROUND(100 * pct_of_next_actions_from_current_channel, 1) AS pct_next_action,
  orders,
  revenue
FROM `action-dwh.attribution_model_ecom.mta_next_best_action_top_20260528_20260610`
ORDER BY current_channel;
