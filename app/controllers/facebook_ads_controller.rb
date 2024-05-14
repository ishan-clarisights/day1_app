require Rails.root.join('config/initializers/constants')

class FacebookAdsController < ApplicationController
  def initialize
    @logger = Logger.new(STDOUT)
    @logger.formatter = proc do |severity, datetime, progname, msg|
      "#{severity}: #{msg}\n"
    end
  end

  def fetch_and_store_account_insights
    begin
      ad_account_ids = fetch_accounts

      db = open_db_connection

      create_account_insights_table(db)

      thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 1)

      (Date.parse('2024-03-01')..Date.parse('2024-05-13')).each do |date|
        ad_account_ids.each do |account_id|
          Concurrent::Promises.future_on(thread_pool) do
            fetch_and_store_account_insights_data(account_id, date, db)
          end
        end
      end

      thread_pool.shutdown
      thread_pool.wait_for_termination

      close_db_connection(db)

      render json: JSON.pretty_generate("Done")
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

    def fetch_and_store_account_insights_data(account_id, date, db)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/insights?fields=account_name,ctr,inline_link_click_ctr,clicks,inline_link_clicks,cost_per_inline_link_click,impressions,spend,actions&time_range=\\{\'since\':\'#{date}\',\'until\':\'#{date}\'\\}&access_token=#{ACCESS_TOKEN}"
      
      response = `curl "#{url}"`

      insights_data = JSON.parse(response)['data']

      if !insights_data.nil? && !insights_data.empty?
        mobile_app_installs = 0
        likes = 0
        landing_page_view = 0
        video_view = 0
        comment = 0

        actions =  insights_data[0]["actions"]
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

        all_insights = {
          account_id: account_id,
          date: date.to_s,
          account_name: insights_data[0]["account_name"],
          ctr: insights_data[0]["ctr"].to_f,
          inline_link_click_ctr: insights_data[0]["inline_link_click_ctr"].to_f,
          clicks: insights_data[0]["clicks"].to_i,
          inline_link_clicks: insights_data[0]["inline_link_clicks"].to_i,
          cost_per_inline_link_click: insights_data[0]["cost_per_inline_link_click"].to_f,
          impressions: insights_data[0]["impressions"].to_i,
          spend: insights_data[0]["spend"].to_f,
          mobile_app_installs: mobile_app_installs,
          landing_page_view: landing_page_view,
          video_view: video_view,
          likes: likes,
          comment: comment
        }

        store_account_insights_data(all_insights, db)
      elsif
        @logger.warn("Insights not found for account: #{account_id} and date: #{date}")
      end
    end

    def open_db_connection
      db = SQLite3::Database.open 'insights.db'
      db
    end

    def close_db_connection(db)
      db.close
    end

    def create_account_insights_table(db)
      db.execute "CREATE TABLE IF NOT EXISTS account_insights (
          account_id VARCHAR(255),
          date VARCHAR(255),
          account_name VARCHAR(255),
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
          PRIMARY KEY (account_id, date)
        )"
    end

    def store_account_insights_data(row, db)
      db.execute("REPLACE INTO account_insights (account_id, date, account_name, ctr, inline_link_click_ctr, clicks, inline_link_clicks, cost_per_inline_link_click, impressions, spend, mobile_app_installs, landing_page_view, video_view, likes, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [row[:account_id], row[:date], row[:account_name], row[:ctr], row[:inline_link_click_ctr], row[:clicks], row[:inline_link_clicks], row[:cost_per_inline_link_click], row[:impressions], row[:spend], row[:mobile_app_installs], row[:landing_page_view], row[:video_view], row[:likes], row[:comment]])

    end
end
