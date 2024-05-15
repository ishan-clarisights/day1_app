Rails.application.routes.draw do
  get 'insights/new'
  get '/fetch_data', to: 'insights#fetch_data'
  get 'facebook_ad_insights_backfill/fetch_and_store_ad_insights'
  get 'facebook_ad_dimension_backfill/fetch_and_store_ad_dimension_data'
  get 'facebook_campaign_dimension_backfill/fetch_and_store_campaign_dimension_data'
  get 'facebook_campaign_insights_backfill/fetch_and_store_campaign_insights'
  get 'facebook_ad_set_insights_backfill/fetch_and_store_adset_insights'
  get 'facebook_ad_set_dimension_backfill/fetch_and_store_adset_dimension_data'
  get 'facebook_ads/fetch_and_store_account_insights'
  get 'revenue_data_merge/read_csv', to: 'revenue_data_merge#read_csv'
end
