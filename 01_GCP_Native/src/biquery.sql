SELECT 
  data_element.tags
FROM 
  `gcp-native-v2.Test.Cryptos`,
  UNNEST(data) AS data_element
LIMIT 100;

SELECT column_name, data_type
FROM `gcp-native-v2.Test.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'Cryptos';

SELECT
  data_element.tvl_ratio,
  data_element.quote.USD.last_updated,
  data_element.quote.USD.fully_diluted_market_cap,
  data_element.quote.USD.price,
  data_element.quote.USD.market_cap,
  data_element.quote.USD.percent_change_60d,
  data_element.quote.USD.percent_change_30d,
  data_element.quote.USD.percent_change_90d,
  data_element.quote.USD.percent_change_1h,
  data_element.quote.USD.volume_change_24h,
  data_element.quote.USD.percent_change_7d,
  data_element.quote.USD.percent_change_24h,
  data_element.quote.USD.market_cap_dominance,
  data_element.quote.USD.tvl,
  data_element.quote.USD.volume_24h,
  data_element.self_reported_market_cap,
  data_element.infinite_supply,
  data_element.total_supply,
  data_element.last_updated,
  data_element.circulating_supply,
  data_element.date_added,
  data_element.cmc_rank,
  data_element.platform.slug,
  data_element.platform.token_address,
  data_element.platform.symbol,
  data_element.platform.name,
  data_element.platform.id,
  data_element.max_supply,
  data_element.num_market_pairs,
  data_element.slug,
  data_element.symbol,
  data_element.name,
  data_element.self_reported_circulating_supply,
  data_element.id,
  status.notice,
  status.error_code,
  status.credit_count,
  status.elapsed,
  status.error_message,
  status.total_count,
  status.timestamp
FROM `gcp-native-v2.Test.Cryptos`,
UNNEST(data) AS data_element