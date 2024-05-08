require "test_helper"

class FacebookAdSetInsightsBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_adset_insights" do
    get facebook_ad_set_insights_backfill_fetch_and_store_adset_insights_url
    assert_response :success
  end
end
