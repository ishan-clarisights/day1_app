require Rails.root.join('config/initializers/constants')

class FacebookAdInsightsBackfillController < ApplicationController
  def initialize
    @logger = Logger.new('logfile.log')
  end

  def fetch_and_store_ad_insights
    begin
      db = open_db_connection

      campaign_ids = fetch_all_campaigns_id(db)

      create_ad_insights_table(db)

      thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

      (Date.parse('2024-03-01')..Date.parse('2024-05-13')).each do |date|
        campaign_ids.each do |campaign_id|
          Concurrent::Promises.future_on(thread_pool) do
            fetch_and_store_campaign_ads_insights_for_date(campaign_id, date, db)
          end
        end
      end

      thread_pool.shutdown
      thread_pool.wait_for_termination

      close_db_connection(db)
      
      render json: JSON.pretty_generate("Done fetching insights for ads.")
    rescue StandardError => e
      render json: { error: "Internal Server Error. #{e.class}: #{e.message}" }, status: :internal_server_error
    end
  end

  private
    def fetch_and_store_campaign_ads_insights_for_date(campaign_id, date, db)
      url = "https://graph.facebook.com/#{API_VERSION}/#{campaign_id}/insights?fields=ad_id,adset_id,ctr,inline_link_click_ctr,clicks,inline_link_clicks,cost_per_inline_link_click,impressions,spend,actions&time_range=\\{\'since\':\'#{date}\',\'until\':\'#{date}\'\\}&level=ad&limit=10000&access_token=#{ACCESS_TOKEN}"
        
      response = `curl "#{url}"`

      if response.nil? || JSON.parse(response).nil? || !JSON.parse(response)['error'].nil?
        @logger.warn("Error while fetching ad insights for campaign #{campaign_id} and date: #{date}.")
      end

      insights_data = JSON.parse(response)['data']

      if !insights_data.nil? && !insights_data.empty?
        insights_data.each do |insight|
          if !insight.nil? && !insight.empty?
            mobile_app_installs = 0
            likes = 0
            landing_page_view = 0
            video_view = 0
            comment = 0

            actions =  insight["actions"]
            actions.each do |action|
              if action["action_type"] == "mobile_app_install"
                mobile_app_installs += action["value"].to_i
              end
              if action["action_type"] == "like"
                likes += action["value"].to_i
              end
              if action["action_type"] == "landing_page_view"
                landing_page_view += action["value"].to_i
              end
              if action["action_type"] == "video_view"
                video_view += action["value"].to_i
              end
              if action["action_type"] == "comment"
                comment += action["value"].to_i
              end
            end

            data = {
              ad_id: insight["ad_id"],
              date: date.to_s,
              adset_id: insight["adset_id"],
              ctr: insight["ctr"].to_f,
              inline_link_click_ctr: insight["inline_link_click_ctr"].to_f,
              clicks: insight["clicks"].to_i,
              inline_link_clicks: insight["inline_link_clicks"].to_i,
              cost_per_inline_link_click: insight["cost_per_inline_link_click"].to_f,
              impressions: insight["impressions"].to_i,
              spend: insight["spend"].to_f,
              mobile_app_installs: mobile_app_installs,
              landing_page_view: landing_page_view,
              video_view: video_view,
              likes: likes,
              comment: comment
            }

            store_ad_insights(data, db)
          end
        end
      elsif
        @logger.warn("Ad insights not found for campaign: #{campaign_id} and date: #{date}")
      end
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def create_ad_insights_table(db)
      db.execute "CREATE TABLE IF NOT EXISTS ad_insights (
          ad_id VARCHAR(255),
          date VARCHAR(255),
          adset_id VARCHAR(255),
          ctr FLOAT,
          inline_link_click_ctr FLOAT,
          clicks INTEGER,
          inline_link_clicks INTEGER,
          cost_per_inline_link_click FLOAT,
          impressions INTEGER,
          spend FLOAT,
          mobile_app_installs INTEGER,
          landing_page_view INTEGER,
          video_view INTEGER,
          likes INTEGER,
          comment INTEGER,
          PRIMARY KEY (ad_id, date)
        )"
    end

    def store_ad_insights(row, db)
      db.execute("REPLACE INTO ad_insights (ad_id, date, adset_id, ctr, inline_link_click_ctr, clicks, inline_link_clicks, cost_per_inline_link_click, impressions, spend, mobile_app_installs, landing_page_view, video_view, likes, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [row[:ad_id], row[:date], row[:adset_id], row[:ctr], row[:inline_link_click_ctr], row[:clicks], row[:inline_link_clicks], row[:cost_per_inline_link_click], row[:impressions], row[:spend], row[:mobile_app_installs], row[:landing_page_view], row[:video_view], row[:likes], row[:comment]])
    end

    def fetch_all_campaigns_id(db)
      db.execute("SELECT DISTINCT campaign_id from campaign_insights").flatten;
    end
end
