class FacebookAdDimensionBackfillController < ApplicationController
  ACCESS_TOKEN = 'EAAKDX99BZAz4BO6bXJm9vZCL4mwZBhFZACtutPb3tsHsCiF2gRv1N22TzUIAViNPzhA1buIN83DNSp6Q83s5CBRrnRKSqEWiTDBjDkrhU4kKhl3BIDZA2CV3wgB5xu2zxlUP5GwlUrB6oEJOZCrBGADWCZAZCw3X16zWVdZACsv8D3lKht8Pg3rb7xganYXyo5Ko8'
  API_VERSION = 'v19.0'

  def fetch_and_store_ad_dimension_data
    ad_account_ids = fetch_accounts
    # ad_account_ids = ["act_2416286235360237"]

    ads_dimensions = []

    threads = ad_account_ids.map do |account_id|
      Concurrent::Future.execute { fetch_ads_dimensions(account_id) }
    end

    threads.each { |t| ads_dimensions.concat(t.value) }

    create_ad_dimensions_table

    thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

    ads_dimensions.each do |dimensions|
      Concurrent::Promises.future_on(thread_pool) do
        process_and_store_ad_dimensions(dimensions)
      end
    end

    # process_and_store_ad_dimensions(ads_dimensions[0])

    thread_pool.shutdown
    thread_pool.wait_for_termination

    render json: JSON.pretty_generate(ads_dimensions.length())
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_ads_dimensions(account_id)
      continue = true

      data = []

      original_url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/ads?fields=name,adset_id,adcreatives\\{object_type,instagram_permalink_url,effective_object_story_id,object_story_spec\\}&limit=200&access_token=#{ACCESS_TOKEN}"
      url = original_url

      while continue do
        response = `curl "#{url}"`

        json_response = JSON.parse(response)

        if !json_response.nil? && !json_response["data"].nil?
          data.concat(json_response["data"])

          if (!json_response["paging"].nil? && !json_response["paging"]["next"].nil?)
            continue = true
            after_value = URI.decode_www_form(URI(json_response["paging"]["next"]).query).to_h["after"]
            url = original_url + "&after=" + after_value
          else
            continue = false
          end
        end
      end
      data
    end

    def process_and_store_ad_dimensions(raw_data)
      ad_type = "UNKNOWN"

      if (!raw_data["adcreatives"].nil? && !raw_data["adcreatives"]["data"].nil? && !raw_data["adcreatives"]["data"].empty? && !raw_data["adcreatives"]["data"][0]["object_type"].nil?)
        ad_type = raw_data["adcreatives"]["data"][0]["object_type"]
      end

      landing_page = nil

      if (!raw_data["adcreatives"].nil? && !raw_data["adcreatives"]["data"].nil? && !raw_data["adcreatives"]["data"].empty? && !raw_data["adcreatives"]["data"][0]["object_story_spec"].nil? && !raw_data["adcreatives"]["data"][0]["object_story_spec"]["link_data"].nil? && !raw_data["adcreatives"]["data"][0]["object_story_spec"]["link_data"]["link"].nil?)
        landing_page = raw_data["adcreatives"]["data"][0]["object_story_spec"]["link_data"]["link"]
      end

      if (!raw_data["adcreatives"].nil? && !raw_data["adcreatives"]["data"].nil? && !raw_data["adcreatives"]["data"].empty? && !raw_data["adcreatives"]["data"][0]["object_story_spec"].nil? && !raw_data["adcreatives"]["data"][0]["object_story_spec"]["template_data"].nil? && !raw_data["adcreatives"]["data"][0]["object_story_spec"]["template_data"]["link"].nil?)
        landing_page = raw_data["adcreatives"]["data"][0]["object_story_spec"]["template_data"]["link"]
      end

      facebook_post = nil

      if (!raw_data["adcreatives"].nil? && !raw_data["adcreatives"]["data"].nil? && !raw_data["adcreatives"]["data"].empty? && !raw_data["adcreatives"]["data"][0]["effective_object_story_id"].nil?)
        facebook_post = "https://www.facebook.com/" + raw_data["adcreatives"]["data"][0]["effective_object_story_id"]
      end

      instagram_post = nil

      if (!raw_data["adcreatives"].nil? && !raw_data["adcreatives"]["data"].nil? && !raw_data["adcreatives"]["data"].empty? && !raw_data["adcreatives"]["data"][0]["instagram_permalink_url"].nil?)
        instagram_post = raw_data["adcreatives"]["data"][0]["instagram_permalink_url"]
      end

      data = {
        ad_id: raw_data["id"],
        ad_name: raw_data["name"],
        adset_id: raw_data["adset_id"],
        ad_type: ad_type,
        landing_page: landing_page,
        facebook_post: facebook_post,
        instagram_post: instagram_post
      }

      puts data

      store_dimensions_for_ad(data)
    end

    def create_ad_dimensions_table
      db = SQLite3::Database.open 'insights.db'

      db.execute "CREATE TABLE IF NOT EXISTS ad_dimensions (
          ad_id VARCHAR(255),
          ad_name VARCHAR(255),
          adset_id VARCHAR(255),
          ad_type VARCHAR(255),
          landing_page VARCHAR(255),
          facebook_post VARCHAR(255),
          instagram_post VARCHAR(255),
          PRIMARY KEY (ad_id)
        )"

      db.close
    end

    def store_dimensions_for_ad(data)
      db = SQLite3::Database.open 'insights.db'

      db.execute("REPLACE INTO ad_dimensions (ad_id, ad_name, adset_id, ad_type, landing_page, facebook_post, instagram_post) VALUES (?, ?, ?, ?, ?, ?, ?)",
              [data[:ad_id], data[:ad_name], data[:adset_id], data[:ad_type], data[:landing_page], data[:facebook_post], data[:instagram_post]])

      db.close
    end
end
