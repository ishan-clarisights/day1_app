require "test_helper"

class FacebookAdInsightsBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_ad_insights" do
    get facebook_ad_insights_backfill_fetch_and_store_ad_insights_url
    assert_response :success
  end
end
