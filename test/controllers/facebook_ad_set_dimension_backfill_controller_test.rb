require "test_helper"

class FacebookAdSetDimensionBackfillControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_and_store_adset_dimension_data" do
    get facebook_ad_set_dimension_backfill_fetch_and_store_adset_dimension_data_url
    assert_response :success
  end
end
