class InsightsController < ApplicationController
  ACCOUNT_LEVEL_GROUPBY_DIMENSIONS = {"account_id" => "Account Id", "account_name" => "Account Name"}
  CAMPAIGN_LEVEL_GROUPBY_DIMENSIONS = {"campaign_id" => "Campaign Id", "campaign_name" => "Campaign Name", "objective" => "Campaign Objective", "buying_type" => "Campaign Buying Type"}
  ADSET_LEVEL_GROUPBY_DIMENSIONS = {"adset_id" => "Adset Id", "adset_name" => "Adset name", "optimization_goal" => "Adset Optimization Goal", "billing_event" => "Adset Billing Event", "bid_strategy" => "Adset Bid Strategy"}
  AD_LEVEL_GROUPBY_DIMENSIONS = {"ad_id" => "Ad Id", "ad_name" => "Ad Name", "ad_type" => "Ad Type"}
  METRICS = {"ctr" => "Click-through rate", "inline_link_click_ctr" => "Inline link click-through rate", "clicks": "Clicks", "inline_link_clicks": "Inline link clicks", "cost_per_inline_link_click" => "Cost per inline link clicks", "impressions" => "Impressions", "spend" => "Spend", "mobile_app_installs" => "Mobile App Installs", "landing_page_view" => "Landing Page Views", "video_view" => "Video Views", "likes" => "Likes", "comment" => "Comments"}

  def new
    @group_by_dimensions = ACCOUNT_LEVEL_GROUPBY_DIMENSIONS.merge(CAMPAIGN_LEVEL_GROUPBY_DIMENSIONS).merge(ADSET_LEVEL_GROUPBY_DIMENSIONS).merge(AD_LEVEL_GROUPBY_DIMENSIONS)
    @metrics = METRICS
  end

  def fetch_data
    start_date = params[:start_date]
    end_date = params[:end_date]
    selected_group_by_dimensions = params[:group_by_dimensions] || []
    selected_metrics = params[:metrics] || []

    selected_account_level_dimensions = []
    selected_campaign_level_dimensions = []
    selected_adset_level_dimensions = []
    selected_ad_level_dimensions = []

    selected_group_by_dimensions.each do |selected_dimension|
      if ACCOUNT_LEVEL_GROUPBY_DIMENSIONS.keys.include?(selected_dimension)
        selected_account_level_dimensions.push(selected_dimension)
      end
      if CAMPAIGN_LEVEL_GROUPBY_DIMENSIONS.keys.include?(selected_dimension)
        selected_campaign_level_dimensions.push(selected_dimension)
      end
      if ADSET_LEVEL_GROUPBY_DIMENSIONS.keys.include?(selected_dimension)
        selected_adset_level_dimensions.push(selected_dimension)
      end
      if AD_LEVEL_GROUPBY_DIMENSIONS.keys.include?(selected_dimension)
        selected_ad_level_dimensions.push(selected_dimension)
      end
    end

    render json: JSON.pretty_generate(form_sql_query(start_date, end_date, selected_account_level_dimensions, selected_campaign_level_dimensions, selected_adset_level_dimensions, selected_ad_level_dimensions, selected_metrics))
  end

  private
    def form_sql_query(start_date, end_date, selected_account_level_dimensions, selected_campaign_level_dimensions, selected_adset_level_dimensions, selected_ad_level_dimensions, selected_metrics)
      query = "SELECT "

      selected_account_level_dimensions.each do |dimension|
        query = query + "ai." + dimension + ", "
      end

      selected_campaign_level_dimensions.each do |dimension|
        query = query + "cd." + dimension + ", "
      end

      selected_adset_level_dimensions.each do |dimension|
        query = query + "asd." + dimension + ", "
      end

      selected_ad_level_dimensions.each do |dimension|
        query = query + "ad." + dimension + ", "
      end

      metric_table_abbr = "ai"

      if (!selected_ad_level_dimensions.empty?)
        metric_table_abbr = "adi"
      elsif (!selected_adset_level_dimensions.empty?)
        metric_table_abbr = "asi"
      elsif (!selected_campaign_level_dimensions.empty?)
        metric_table_abbr = "ci"
      else
        metric_table_abbr = "ai"
      end

      selected_metrics.each do |metric|
        if (metric == "ctr")
          query = query + "(CAST(SUM(" + metric_table_abbr + ".clicks) AS FLOAT) * 100) / " + "SUM(" + metric_table_abbr + ".impressions) AS final_ctr, "
        elsif (metric == "inline_link_click_ctr")
          query = query + "(CAST(SUM(" + metric_table_abbr + ".inline_link_clicks) AS FLOAT) * 100) / " + "SUM(" + metric_table_abbr + ".impressions) AS final_inline_link_click_ctr, "
        elsif (metric == "clicks")
          query = query + "SUM(" + metric_table_abbr + ".clicks) AS total_clicks, "
        elsif (metric == "inline_link_clicks")
          query = query + "SUM(" + metric_table_abbr + ".inline_link_clicks) AS total_inline_link_clicks, "
        elsif (metric == "cost_per_inline_link_click")
          query = query + "(CAST(SUM(" + metric_table_abbr + ".spend) AS FLOAT) * 100) / " + "SUM(" + metric_table_abbr + ".inline_link_clicks) AS final_cost_per_inline_link_click, "
        elsif (metric == "spend")
          query = query + "SUM(" + metric_table_abbr + ".spend) AS total_spend, "
        elsif (metric == "impressions")
          query = query + "SUM(" + metric_table_abbr + ".impressions) AS total_impressions, "
        elsif (metric == "mobile_app_installs")
          query = query + "SUM(" + metric_table_abbr + ".mobile_app_installs) AS total_mobile_app_installs, "
        elsif (metric == "landing_page_view")
          query = query + "SUM(" + metric_table_abbr + ".landing_page_view) AS total_landing_page_view, "
        elsif (metric == "likes")
          query = query + "SUM(" + metric_table_abbr + ".likes) AS total_likes, "
        else
          query = query + "SUM(" + metric_table_abbr + ".comment) AS total_comment, "
        end
      end

      query = query[0..-3]
      query = query + " "

      if(!selected_account_level_dimensions.empty?)
        query = query + "FROM account_insights ai "
      end

      if(!selected_account_level_dimensions.empty? && (!selected_campaign_level_dimensions.empty? || !selected_adset_level_dimensions.empty? || !selected_ad_level_dimensions.empty?))
        query = query + "JOIN campaign_dimensions cd ON ai.account_id = cd.account_id "
      end

      if(selected_account_level_dimensions.empty? && !selected_campaign_level_dimensions.empty?)
        query = query + "FROM campaign_dimensions cd "
      end

      if(!selected_campaign_level_dimensions.empty? && selected_adset_level_dimensions.empty? && selected_ad_level_dimensions.empty?)
        query = query + "JOIN campaign_insights ci ON cd.campaign_id = ci.campaign_id "
      end

      if((!selected_account_level_dimensions.empty? || !selected_campaign_level_dimensions.empty?)  && (!selected_adset_level_dimensions.empty? || !selected_ad_level_dimensions.empty?))
        query = query + "JOIN adset_dimensions asd ON cd.campaign_id = asd.campaign_id "
      end

      if(selected_account_level_dimensions.empty? && selected_campaign_level_dimensions.empty? && !selected_adset_level_dimensions.empty?)
        query = query + "FROM adset_dimensions asd "
      end

      if(!selected_adset_level_dimensions.empty? && selected_ad_level_dimensions.empty?)
        query = query + "JOIN adset_insights asi ON asd.adset_id = asi.adset_id "
      end

      if((!selected_account_level_dimensions.empty? || !selected_campaign_level_dimensions.empty? || !selected_adset_level_dimensions.empty?)  && (!selected_ad_level_dimensions.empty?))
        query = query + "JOIN ad_dimensions ad ON asd.adset_id = ad.adset_id "
      end

      if(selected_account_level_dimensions.empty? && selected_campaign_level_dimensions.empty? && selected_adset_level_dimensions.empty? && !selected_ad_level_dimensions.empty?)
        query = query + "FROM adset_dimensions asd "
      end

      if(!selected_ad_level_dimensions.empty?)
        query = query + "JOIN ad_insights adi ON ad.ad_id = adi.ad_id "
      end
      
      query = query + "WHERE "

      if (!selected_ad_level_dimensions.empty?)
        query = query + "adi.date BETWEEN '" + start_date + "' AND '" + end_date + "' "
      elsif (!selected_adset_level_dimensions.empty?)
        query = query + "asi.date BETWEEN '" + start_date + "' AND '" + end_date + "' "
      elsif (!selected_campaign_level_dimensions.empty?)
        query = query + "ci.date BETWEEN '" + start_date + "' AND '" + end_date + "' "
      else
        query = query + "ai.date BETWEEN '" + start_date + "' AND '" + end_date + "' "
      end

      query = query + "GROUP BY "

      selected_account_level_dimensions.each do |dimension|
        query = query + "ai." + dimension + ", "
      end

      selected_campaign_level_dimensions.each do |dimension|
        query = query + "cd." + dimension + ", "
      end

      selected_adset_level_dimensions.each do |dimension|
        query = query + "asd." + dimension + ", "
      end

      selected_ad_level_dimensions.each do |dimension|
        query = query + "ad." + dimension + ", "
      end

      query = query[0..-3]

      query
    end
end
