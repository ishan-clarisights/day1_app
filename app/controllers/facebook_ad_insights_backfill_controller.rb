class FacebookAdInsightsBackfillController < ApplicationController
  ACCESS_TOKEN = 'EAAKDX99BZAz4BO6bXJm9vZCL4mwZBhFZACtutPb3tsHsCiF2gRv1N22TzUIAViNPzhA1buIN83DNSp6Q83s5CBRrnRKSqEWiTDBjDkrhU4kKhl3BIDZA2CV3wgB5xu2zxlUP5GwlUrB6oEJOZCrBGADWCZAZCw3X16zWVdZACsv8D3lKht8Pg3rb7xganYXyo5Ko8'
  API_VERSION = 'v19.0'

  def fetch_and_store_ad_insights
    ad_account_ids = fetch_accounts

    create_ad_insights_table

    thread_pool = Concurrent::ThreadPoolExecutor.new(min_threads: 1, max_threads: 10)

    (Date.parse('2024-03-01')..Date.parse('2024-05-07')).each do |date|
      ad_account_ids.each do |account_id|
        Concurrent::Promises.future_on(thread_pool) do
          fetch_and_store_account_ads_insights_for_date(account_id, date)
        end
      end
    end

    thread_pool.shutdown
    thread_pool.wait_for_termination
    
    render json: JSON.pretty_generate("Done")
  end

  private
    def fetch_accounts
      url = "https://graph.facebook.com/#{API_VERSION}/me/adaccounts?access_token=#{ACCESS_TOKEN}"

      response = `curl "#{url}"`

      ad_account_ids = JSON.parse(response)['data'].map { |account| account['id'] }

      return ad_account_ids
    end

    def fetch_and_store_account_ads_insights_for_date(account_id, date)
      url = "https://graph.facebook.com/#{API_VERSION}/#{account_id}/insights?fields=ad_id,adset_id,ctr,inline_link_click_ctr,clicks,inline_link_clicks,cost_per_inline_link_click,impressions,spend,actions&time_range=\\{\'since\':\'#{date}\',\'until\':\'#{date}\'\\}&level=ad&limit=10000&access_token=#{ACCESS_TOKEN}"
        
      response = `curl "#{url}"`

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

            store_ad_insights(data)
          end
        end
      end
    end

    def create_ad_insights_table
      db = SQLite3::Database.open 'insights.db'

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

        db.close
    end

    def store_ad_insights(row)
      db = SQLite3::Database.open 'insights.db'

      db.execute("REPLACE INTO ad_insights (ad_id, date, adset_id, ctr, inline_link_click_ctr, clicks, inline_link_clicks, cost_per_inline_link_click, impressions, spend, mobile_app_installs, landing_page_view, video_view, likes, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [row[:ad_id], row[:date], row[:adset_id], row[:ctr], row[:inline_link_click_ctr], row[:clicks], row[:inline_link_clicks], row[:cost_per_inline_link_click], row[:impressions], row[:spend], row[:mobile_app_installs], row[:landing_page_view], row[:video_view], row[:likes], row[:comment]])
      
      db.close
    end
end
