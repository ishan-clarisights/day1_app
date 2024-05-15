require Rails.root.join('config/initializers/constants')

class FacebookCampaignInsightsBackfillController < ApplicationController
  def initialize
    @logger = Logger.new('logfile.log')
  end

  def fetch_and_store_campaign_insights
    begin
      ad_account_ids = fetch_accounts

      db = open_db_connection

      create_campaign_insights_table(db)

      thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

      (Date.parse('2024-03-01')..Date.parse('2024-05-14')).each do |date|
        ad_account_ids.each do |account_id|
          Concurrent::Promises.future_on(thread_pool) do
            fetch_and_store_account_campaign_insights_for_date(account_id, date, db)
          end
        end
      end

      thread_pool.shutdown
      thread_pool.wait_for_termination

      close_db_connection(db)
      
      render json: JSON.pretty_generate("Done fetching insights for campaigns.")
    rescue StandardError => e
      render json: { error: "Internal Server Error. #{e.class}: #{e.message}" }, status: :internal_server_error
    end
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      if(response.nil? || JSON.parse(response).nil? || JSON.parse(response)['data'].nil? )
        raise ArgumentError, 'Unable to fetch accounts'
      end

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_and_store_account_campaign_insights_for_date(account_id, date, db)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/insights?fields=campaign_id,account_id,ctr,inline_link_click_ctr,clicks,inline_link_clicks,cost_per_inline_link_click,impressions,spend,actions&time_range=\\{\'since\':\'#{date}\',\'until\':\'#{date}\'\\}&level=campaign&limit=10000&access_token=#{ACCESS_TOKEN}"
        
      response = `curl "#{url}"`

      if response.nil? || JSON.parse(response).nil? || !JSON.parse(response)['error'].nil?
        @logger.error("Error while fetching campaign insights for #{account_id} and date: #{date}. #{response} #{url}")
        raise ArgumentError, "Error while fetching campaign insights for #{account_id} and date: #{date}."
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
            if !actions.nil?
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
            end

            data = {
              campaign_id: insight["campaign_id"],
              date: date.to_s,
              account_id: insight["account_id"],
              ctr: insight["ctr"].nil? ? 0 : insight["ctr"].to_f,
              inline_link_click_ctr: insight["inline_link_click_ctr"].nil? ? 0 : insight["inline_link_click_ctr"].to_f,
              clicks: insight["clicks"].nil? ? 0 : insight["clicks"].to_i,
              inline_link_clicks: insight["inline_link_clicks"].nil? ? 0 : insight["inline_link_clicks"].to_i,
              cost_per_inline_link_click: insight["cost_per_inline_link_click"].nil? ? 0 : insight["cost_per_inline_link_click"].to_f,
              impressions: insight["impressions"].nil? ? 0 : insight["impressions"].to_i,
              spend: insight["spend"].nil? ? 0 : insight["spend"].to_f,
              mobile_app_installs: mobile_app_installs,
              landing_page_view: landing_page_view,
              video_view: video_view,
              likes: likes,
              comment: comment
            }

            store_campaign_insights(data, db)
          end
        end
      end
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def create_campaign_insights_table(db)
      db.execute "CREATE TABLE IF NOT EXISTS campaign_insights (
          campaign_id VARCHAR(255),
          date VARCHAR(255),
          account_id VARCHAR(255),
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
          PRIMARY KEY (campaign_id, date)
        )"
    end

    def store_campaign_insights(row, db)
      if !check_if_campaign_dimension_present(row[:campaign_id], db)
        fetch_and_store_dimension(row[:campaign_id], db)
      end

      db.execute("REPLACE INTO campaign_insights (campaign_id, date, account_id, ctr, inline_link_click_ctr, clicks, inline_link_clicks, cost_per_inline_link_click, impressions, spend, mobile_app_installs, landing_page_view, video_view, likes, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [row[:campaign_id], row[:date], "act_" + row[:account_id], row[:ctr], row[:inline_link_click_ctr], row[:clicks], row[:inline_link_clicks], row[:cost_per_inline_link_click], row[:impressions], row[:spend], row[:mobile_app_installs], row[:landing_page_view], row[:video_view], row[:likes], row[:comment]])
    end

    def check_if_campaign_dimension_present(campaign_id, db)
      result = db.get_first_value("SELECT COUNT(*) FROM campaign_dimensions WHERE campaign_id = ?", campaign_id)
      
      result == 1
    end

    def fetch_and_store_dimension(campaign_id, db)
      url = "https://graph.facebook.com/#{API_VERSION}/#{campaign_id}/?fields=name,account_id,start_time,objective,daily_budget,lifetime_budget,buying_type&access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      data = JSON.parse(response)

      if !data.nil?
        if !data["start_time"].nil? && !data["start_time"].split("T").nil? && !data["start_time"].split("T")[0].nil?
          actual_start_date = data["start_time"].split("T")[0]
          data["start_date"] = actual_start_date
        elsif
          data["start_date"] = ""
        end

        db.execute("REPLACE INTO campaign_dimensions (campaign_id, campaign_name, start_date, account_id, objective, daily_budget, lifetime_budget, buying_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [data["id"], data["name"], data["start_date"], "act_" + data["account_id"], data["objective"], data["daily_budget"].to_f, data["lifetime_budget"].to_f, data["buying_type"]])
      end
    end
end
