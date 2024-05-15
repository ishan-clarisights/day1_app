require Rails.root.join('config/initializers/constants')

class FacebookAdDimensionBackfillController < ApplicationController
  def initialize
    @logger = Logger.new('logfile.log')
  end

  def fetch_and_store_ad_dimension_data
    begin
      db = open_db_connection

      campaign_ids = fetch_all_campaigns_id(db)

      ads_dimensions = []

      thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

      campaign_ids.each do |campaign_id|
        Concurrent::Promises.future_on(thread_pool) do
          data = fetch_ads_dimensions(campaign_id)
          ads_dimensions.concat(data)
        end
      end

      thread_pool.shutdown
      thread_pool.wait_for_termination

      create_ad_dimensions_table(db)

      thread_pool1 = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

      ads_dimensions.each do |dimensions|
        Concurrent::Promises.future_on(thread_pool1) do
          process_and_store_ad_dimensions(dimensions, db)
        end
      end

      thread_pool1.shutdown
      thread_pool1.wait_for_termination

      close_db_connection(db)

      render json: JSON.pretty_generate("Fetched dimensions for #{ads_dimensions.length()} adsets.")
    rescue StandardError => e
      render json: { error: "Internal Server Error. #{e.class}: #{e.message}" }, status: :internal_server_error
    end
  end

  private
    def fetch_ads_dimensions(level_id)
      continue = true

      data = []

      original_url = "https://graph.facebook.com/#{API_VERSION}/#{level_id}/ads?fields=name,adset_id,adcreatives\\{object_type,instagram_permalink_url,effective_object_story_id,object_story_spec\\}&limit=200&access_token=#{ACCESS_TOKEN}"
      url = original_url

      while continue do
        response = `curl "#{url}"`

        if response.nil? || JSON.parse(response).nil? || !JSON.parse(response)['error'].nil?
          @logger.error("Error while fetching ad dimensions for level #{level_id} and date: #{date}. #{response} #{url}")
        end

        json_response = JSON.parse(response)

        if !json_response["data"].nil?
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

    def process_and_store_ad_dimensions(raw_data, db)
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

      store_dimensions_for_ad(data, db)
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def create_ad_dimensions_table(db)
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
    end

    def store_dimensions_for_ad(data, db)
      db.execute("REPLACE INTO ad_dimensions (ad_id, ad_name, adset_id, ad_type, landing_page, facebook_post, instagram_post) VALUES (?, ?, ?, ?, ?, ?, ?)",
              [data[:ad_id], data[:ad_name], data[:adset_id], data[:ad_type], data[:landing_page], data[:facebook_post], data[:instagram_post]])
    end

    def fetch_all_campaigns_id(db)
      db.execute("SELECT DISTINCT campaign_id from campaign_insights").flatten;
    end
end
