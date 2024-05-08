require "test_helper"

class FacebookCampaignInsightsBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_campaign_insights" do
    get facebook_campaign_insights_backfill_fetch_and_store_campaign_insights_url
    assert_response :success
  end
end
