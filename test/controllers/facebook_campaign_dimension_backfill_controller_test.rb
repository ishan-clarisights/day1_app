require "test_helper"

class FacebookCampaignDimensionBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_campaign_dimension_data" do
    get facebook_campaign_dimension_backfill_fetch_and_store_campaign_dimension_data_url
    assert_response :success
  end
end
