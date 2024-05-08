require "test_helper"

class FacebookAdsControllerTest < ActionDispatch::IntegrationTest
  test "should get fetch_ad_accounts" do
    get facebook_ads_fetch_ad_accounts_url
    assert_response :success
  end
end
