class InsightsController < ApplicationController
  ACCOUNT_LEVEL_GROUPBY_DIMENSIONS = ["acount_id", "account_name"]
  CAMPAIGN_LEVEL_GROUPBY_DIMENSIONS = ["campaign_id", "campaign_name", "objective", "buying_type"]
  ADSET_LEVEL_GROUPBY_DIMENSIONS = ["adset_id", "adset_name", "optimization_goal", "billing_event", "bid_strategy"]
  AD_LEVEL_GROUPBY_DIMENSIONS = ["ad_id", "ad_name", "ad_type"]
  
  def new

    @options = ["Option 1", "Option 2", "Option 3"]
  end
end
