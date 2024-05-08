require "test_helper"

class FacebookAdDimensionBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_ad_dimension_data" do
    get facebook_ad_dimension_backfill_fetch_and_store_ad_dimension_data_url
    assert_response :success
  end
end
