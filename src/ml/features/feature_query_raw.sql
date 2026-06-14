/* race_idごとの出走頭数を馬番の最大値から算出 */
with temp_race_horse_count as (
  select
    race_id
    ,max(horse_number) as num_horses
  -- race_resultsを参照すると、当日の出走情報の取得が誤っているときに意図しない挙動となるため、horse_resultsを参照して出走頭数を算出する
  from `{project_id}`.raw.horse_results
  group by race_id
)

/* レース内の上がり3Fタイム順位を事前計算 */
,temp_race_last3f_ranks as (
  select
    race_id
    ,horse_id
    ,rank() over (partition by race_id order by last_3f_time asc) as last_3f_rank_in_race
  from `{project_id}`.raw.race_results
  where last_3f_time is not null
  qualify row_number() over (partition by race_id, horse_id order by last_3f_time asc) = 1
)

/* 直接的に馬柱を見て集計できる特徴量 */
,temp_base_race_entries as (
  select
  -- レースに関する情報
    h_r.race_id
    ,r_i.race_date
    ,r_i.venue_code
    ,r_i.venue_name
    ,r_i.race_number
    ,r_i.race_name
    ,r_i.course_type
    ,r_i.distance
    ,r_i.direction
    ,r_i.age_condition
    ,r_i.race_class
    ,coalesce(t_r_h_c.num_horses, r_i.num_horses) as num_horses
    -- 出走馬に関する情報
    ,h_r.horse_id
    ,h_r.horse_name
    ,h_m.birth_date -- 馬の生年月日
    ,date_diff(r_i.race_date, h_m.birth_date, year) as horse_age -- 馬齢
    ,min(date_diff(r_i.race_date, h_m.birth_date, year)) over (partition by h_r.race_id) as min_horse_age -- そのレースの最小馬齢
    ,h_r.bracket_number
    ,h_r.horse_number
    ,h_r.trainer_name
    ,h_r.trainer_code
    ,h_r.jockey_name
    ,h_r.jockey_code
    ,h_r.weight_carried -- 斤量
    ,h_r.idm -- 能力指数
    ,h_r.jockey_index -- 騎手指数
    ,h_r.info_index -- 情報指数
    ,h_r.total_index
    ,h_r.running_style -- 脚質(1逃げ、2先行、3差し、4追込)
    ,h_r.distance_aptitude -- 距離適性
    ,case
      when r_i.distance < 1400 then 'sprint'
      when r_i.distance < 1800 then 'mile'
      when r_i.distance < 2200 then 'intermediate'
      else 'long'
    end as distance_band
    ,h_r.improvement -- 上昇度
    ,h_r.base_odds -- 想定オッズ
    ,h_r.base_popularity -- 想定人気
    ,h_r.popularity_index -- 人気指数
    ,h_r.training_index -- 調教指数
    ,h_r.stable_index -- 厩舎指数
    ,h_r.jockey_expected_win_rate -- 騎手期待連対率
    ,h_r.surge_index -- 激走指数
    ,h_r.hoof_code -- 蹄コード
    ,h_r.heavy_aptitude_code -- 重適性指数
    ,h_r.blinker -- ブリンカー
    ,h_r.ten_index
    ,h_r.pace_index
    ,h_r.agari_index
    ,h_r.position_index
    ,h_r.pace_forecast
    -- 展開利を見込んだ特徴量
    ,case
      when h_r.pace_forecast = 'S' and h_r.running_style = 1 then 2
      when h_r.pace_forecast = 'S' and h_r.running_style = 2 then 1
      else 0
    end as early_advantage
    ,case
      when h_r.pace_forecast = 'H' and h_r.running_style = 3 then 1
      when h_r.pace_forecast = 'H' and h_r.running_style = 4 then 2
      else 0
    end as behind_advantage
    ,case
      when coalesce(t_r_h_c.num_horses, r_i.num_horses) < 10 and h_r.running_style = 1 then 1
      else 0
    end as small_number_early_advantage
    ,h_r.mid_gap
    ,h_r.trainer_affiliation -- 厩舎の所属
    ,h_r.prev_race_key_1
    ,h_r.prev_race_key_2
    ,h_r.prev_race_key_3
    ,h_r.prev_race_key_4
    ,h_r.prev_race_key_5
    ,h_r.overall_mark
    ,h_r.idm_mark
    ,h_r.info_mark
    ,h_r.jockey_mark
    ,h_r.stable_mark
  from
    `{project_id}`.raw.horse_results as h_r
    left join `{project_id}`.raw.race_info as r_i
      on h_r.race_id = r_i.race_id
    left join `{project_id}`.raw.horse_master as h_m
      on h_r.horse_id = h_m.horse_id
    left join temp_race_horse_count as t_r_h_c
      on h_r.race_id = t_r_h_c.race_id
  where
    r_i.race_date BETWEEN '{start_date}' AND '{end_date}'
    and r_i.race_class != 'A1'                                                                            -- 新馬戦を除外
    and coalesce(r_i.course_type, '') != 'obstacle'                                                       -- 障害戦を除外
    and coalesce(t_r_h_c.num_horses, r_i.num_horses) > 7                                                  -- 少頭数（7頭以下）を除外
    and not (r_i.venue_code = '04' and r_i.distance = 1000 and r_i.direction = 'straight')               -- 新潟直線1000mを除外
)

/* 過去レースの情報から集計できる特徴量 */
,temp_past_race_features as (
  select
    t_b_r_e.*
    -- 馬齢ごとのセグメントを作成
    ,case
      when t_b_r_e.horse_age = t_b_r_e.min_horse_age then 1
      else 0
    end as horse_age_segment
    -- 1走前のレース結果
    ,r_r_1.race_name as race_name_1
    ,round(safe_divide(date_diff(t_b_r_e.race_date, r_r_1.race_date, day), 7))-1 as race_date_diff_1
    ,case
      when t_b_r_e.course_type != r_r_1.course_type then 1
      else 0
    end as condition_change_flag
    ,r_r_1.finish_position as finish_position_1
    ,IF(r_r_1.finish_position > 0, safe_divide(r_r_1.finish_position, coalesce(t_r_h_c_1.num_horses, r_r_1.num_horses)), NULL) as finish_position_rate_1 -- 全体に対するゴール位置の割合 (finish_position=0は無効扱い)
    ,r_r_1.win_odds as win_odds_1
    ,r_r_1.win_popularity as win_popularity_1
    ,safe_divide(r_r_1.win_popularity, coalesce(t_r_h_c_1.num_horses, r_r_1.num_horses)) as popularity_rate_1 -- 全体に対する人気の割合
    ,safe_divide(r_r_1.win_popularity-r_r_1.finish_position, coalesce(t_r_h_c_1.num_horses, r_r_1.num_horses)) as upside_rate_1 -- 人気に対してどの程度着順が上振れるか(プラスだと上振れ)
    ,r_r_1.idm as idm_1
    ,r_r_1.improvement_code as improvement_code_1
    ,r_r_1.late_start as late_start_1
    ,r_r_1.position_fault as position_fault_1
    ,r_r_1.disadvantage          as disadvantage_1
    ,r_r_1.front_disadvantage    as front_disadvantage_1
    ,r_r_1.mid_disadvantage      as mid_disadvantage_1
    ,r_r_1.back_disadvantage     as back_disadvantage_1
    ,r_r_1.course_position       as course_position_prev1
    ,r_r_1.track_bias            as track_bias_prev1
    ,r_r_1.finish_time as finish_time_1
    ,r_r_1.last_3f_time as last_3f_1
    ,r_r_1.corner_position_4 as corner_position_1
    ,r_r_1.corner_position_1 as corner1_prev_1
    ,r_r_1.corner_position_2 as corner2_prev_1
    ,r_r_1.corner_position_3 as corner3_prev_1
    ,r_r_1.corner_position_4 as corner4_prev_1
    ,r_l3f_1.last_3f_rank_in_race as last_3f_rank_in_race_1
    ,SUBSTR(r_r_1.race_id, 1, 2) as venue_code_prev_1
    ,v_i_prev1.straight_bias_innermost as prev1_straight_bias_innermost
    ,v_i_prev1.straight_bias_inner     as prev1_straight_bias_inner
    ,v_i_prev1.straight_bias_outer     as prev1_straight_bias_outer
    ,v_i_prev1.straight_bias_outermost as prev1_straight_bias_outermost
    ,r_r_1.distance as distance_prev_1
    ,r_r_1.track_condition as track_condition_prev_1
    ,t_b_r_e.distance - r_r_1.distance as distance_change
    ,case
      when r_r_1.distance is null then null
      when t_b_r_e.distance > r_r_1.distance then 1
      when t_b_r_e.distance < r_r_1.distance then -1
      else 0
    end as distance_change_flag
    -- 2走前のレース結果
    ,r_r_2.race_name as race_name_2
    ,round(safe_divide(date_diff(t_b_r_e.race_date, r_r_2.race_date, day), 7))-1 as race_date_diff_2
    ,r_r_2.finish_position as finish_position_2
    ,IF(r_r_2.finish_position > 0, safe_divide(r_r_2.finish_position, coalesce(t_r_h_c_2.num_horses, r_r_2.num_horses)), NULL) as finish_position_rate_2 -- 全体に対するゴール位置の割合 (finish_position=0は無効扱い)
    ,r_r_2.win_odds as win_odds_2
    ,r_r_2.win_popularity as win_popularity_2
    ,safe_divide(r_r_2.win_popularity, coalesce(t_r_h_c_2.num_horses, r_r_2.num_horses)) as popularity_rate_2 -- 全体に対する人気の割合
    ,safe_divide(r_r_2.win_popularity-r_r_2.finish_position, coalesce(t_r_h_c_2.num_horses, r_r_2.num_horses)) as upside_rate_2 -- 人気に対してどの程度着順が上振れるか(プラスだと上振れ)
    ,r_r_2.idm as idm_2
    ,r_r_2.improvement_code as improvement_code_2
    ,r_r_2.late_start as late_start_2
    ,r_r_2.position_fault as position_fault_2
    ,r_r_2.disadvantage          as disadvantage_2
    ,r_r_2.front_disadvantage    as front_disadvantage_2
    ,r_r_2.mid_disadvantage      as mid_disadvantage_2
    ,r_r_2.back_disadvantage     as back_disadvantage_2
    ,r_r_2.course_position       as course_position_prev2
    ,r_r_2.track_bias            as track_bias_prev2
    ,r_r_2.finish_time as finish_time_2
    ,r_r_2.last_3f_time as last_3f_2
    ,r_r_2.corner_position_4 as corner_position_2
    ,r_r_2.corner_position_1 as corner1_prev_2
    ,r_r_2.corner_position_2 as corner2_prev_2
    ,r_r_2.corner_position_3 as corner3_prev_2
    ,r_r_2.corner_position_4 as corner4_prev_2
    ,r_l3f_2.last_3f_rank_in_race as last_3f_rank_in_race_2
    ,v_i_prev2.straight_bias_innermost as prev2_straight_bias_innermost
    ,v_i_prev2.straight_bias_inner     as prev2_straight_bias_inner
    ,v_i_prev2.straight_bias_outer     as prev2_straight_bias_outer
    ,v_i_prev2.straight_bias_outermost as prev2_straight_bias_outermost
    -- 3走前のレース結果
    ,r_r_3.race_name as race_name_3
    ,r_r_3.finish_position as finish_position_3
    ,IF(r_r_3.finish_position > 0, safe_divide(r_r_3.finish_position, coalesce(t_r_h_c_3.num_horses, r_r_3.num_horses)), NULL) as finish_position_rate_3 -- 全体に対するゴール位置の割合 (finish_position=0は無効扱い)
    ,r_r_3.win_odds as win_odds_3
    ,r_r_3.win_popularity as win_popularity_3
    ,safe_divide(r_r_3.win_popularity, coalesce(t_r_h_c_3.num_horses, r_r_3.num_horses)) as popularity_rate_3 -- 全体に対する人気の割合
    ,safe_divide(r_r_3.win_popularity-r_r_3.finish_position, coalesce(t_r_h_c_3.num_horses, r_r_3.num_horses)) as upside_rate_3 -- 人気に対してどの程度着順が上振れるか(プラスだと上振れ)
    ,r_r_3.idm as idm_3
    ,r_r_3.improvement_code as improvement_code_3
    ,r_r_3.late_start as late_start_3
    ,r_r_3.position_fault as position_fault_3
    ,r_r_3.disadvantage          as disadvantage_3
    ,r_r_3.front_disadvantage    as front_disadvantage_3
    ,r_r_3.mid_disadvantage      as mid_disadvantage_3
    ,r_r_3.back_disadvantage     as back_disadvantage_3
    ,r_r_3.course_position       as course_position_prev3
    ,r_r_3.track_bias            as track_bias_prev3
    ,r_r_3.finish_time as finish_time_3
    ,r_r_3.last_3f_time as last_3f_3
    ,r_r_3.corner_position_4 as corner_position_3
    ,r_r_3.corner_position_1 as corner1_prev_3
    ,r_r_3.corner_position_2 as corner2_prev_3
    ,r_r_3.corner_position_3 as corner3_prev_3
    ,r_r_3.corner_position_4 as corner4_prev_3
    ,r_l3f_3.last_3f_rank_in_race as last_3f_rank_in_race_3
    ,v_i_prev3.straight_bias_innermost as prev3_straight_bias_innermost
    ,v_i_prev3.straight_bias_inner     as prev3_straight_bias_inner
    ,v_i_prev3.straight_bias_outer     as prev3_straight_bias_outer
    ,v_i_prev3.straight_bias_outermost as prev3_straight_bias_outermost
    -- 4走前のレース結果
    ,r_r_4.race_name as race_name_4
    ,r_r_4.finish_position as finish_position_4
    ,IF(r_r_4.finish_position > 0, safe_divide(r_r_4.finish_position, coalesce(t_r_h_c_4.num_horses, r_r_4.num_horses)), NULL) as finish_position_rate_4 -- 全体に対するゴール位置の割合 (finish_position=0は無効扱い)
    ,r_r_4.win_odds as win_odds_4
    ,r_r_4.win_popularity as win_popularity_4
    ,safe_divide(r_r_4.win_popularity, coalesce(t_r_h_c_4.num_horses, r_r_4.num_horses)) as popularity_rate_4 -- 全体に対する人気の割合
    ,safe_divide(r_r_4.win_popularity-r_r_4.finish_position, coalesce(t_r_h_c_4.num_horses, r_r_4.num_horses)) as upside_rate_4 -- 人気に対してどの程度着順が上振れるか(プラスだと上振れ)
    ,r_r_4.idm as idm_4
    ,r_r_4.improvement_code as improvement_code_4
    ,r_r_4.late_start as late_start_4
    ,r_r_4.position_fault as position_fault_4
    ,r_r_4.disadvantage          as disadvantage_4
    ,r_r_4.front_disadvantage    as front_disadvantage_4
    ,r_r_4.mid_disadvantage      as mid_disadvantage_4
    ,r_r_4.back_disadvantage     as back_disadvantage_4
    ,r_r_4.course_position       as course_position_prev4
    ,r_r_4.track_bias            as track_bias_prev4
    ,r_r_4.finish_time as finish_time_4
    ,r_r_4.last_3f_time as last_3f_4
    ,r_r_4.corner_position_4 as corner_position_4
    ,r_l3f_4.last_3f_rank_in_race as last_3f_rank_in_race_4
    ,v_i_prev4.straight_bias_innermost as prev4_straight_bias_innermost
    ,v_i_prev4.straight_bias_inner     as prev4_straight_bias_inner
    ,v_i_prev4.straight_bias_outer     as prev4_straight_bias_outer
    ,v_i_prev4.straight_bias_outermost as prev4_straight_bias_outermost
    -- 5走前のレース結果
    ,r_r_5.race_name as race_name_5
    ,r_r_5.finish_position as finish_position_5
    ,IF(r_r_5.finish_position > 0, safe_divide(r_r_5.finish_position, coalesce(t_r_h_c_5.num_horses, r_r_5.num_horses)), NULL) as finish_position_rate_5 -- 全体に対するゴール位置の割合 (finish_position=0は無効扱い)
    ,r_r_5.win_odds as win_odds_5
    ,r_r_5.win_popularity as win_popularity_5
    ,safe_divide(r_r_5.win_popularity, coalesce(t_r_h_c_5.num_horses, r_r_5.num_horses)) as popularity_rate_5 -- 全体に対する人気の割合
    ,safe_divide(r_r_5.win_popularity-r_r_5.finish_position, coalesce(t_r_h_c_5.num_horses, r_r_5.num_horses)) as upside_rate_5 -- 人気に対してどの程度着順が上振れるか(プラスだと上振れ)
    ,r_r_5.idm as idm_5
    ,r_r_5.improvement_code as improvement_code_5
    ,r_r_5.late_start as late_start_5
    ,r_r_5.position_fault as position_fault_5
    ,r_r_5.disadvantage          as disadvantage_5
    ,r_r_5.front_disadvantage    as front_disadvantage_5
    ,r_r_5.mid_disadvantage      as mid_disadvantage_5
    ,r_r_5.back_disadvantage     as back_disadvantage_5
    ,r_r_5.course_position       as course_position_prev5
    ,r_r_5.track_bias            as track_bias_prev5
    ,r_r_5.finish_time as finish_time_5
    ,r_r_5.last_3f_time as last_3f_5
    ,r_r_5.corner_position_4 as corner_position_5
    ,r_l3f_5.last_3f_rank_in_race as last_3f_rank_in_race_5
    ,v_i_prev5.straight_bias_innermost as prev5_straight_bias_innermost
    ,v_i_prev5.straight_bias_inner     as prev5_straight_bias_inner
    ,v_i_prev5.straight_bias_outer     as prev5_straight_bias_outer
    ,v_i_prev5.straight_bias_outermost as prev5_straight_bias_outermost
    -- 直近5走の平均情報
    ,safe_divide(
      (coalesce(r_r_1.idm, 0) + coalesce(r_r_2.idm, 0) + coalesce(r_r_3.idm, 0) + coalesce(r_r_4.idm, 0) + coalesce(r_r_5.idm, 0))
      ,nullif(
        (case when r_r_1.idm is not null then 1 else 0 end +
        case when r_r_2.idm is not null then 1 else 0 end +
        case when r_r_3.idm is not null then 1 else 0 end +
        case when r_r_4.idm is not null then 1 else 0 end +
        case when r_r_5.idm is not null then 1 else 0 end)
        , 0)
    ) as mean_idm
    ,safe_divide(
      (coalesce(r_r_1.idm*1.5, 0) + coalesce(r_r_2.idm*1.25, 0) + coalesce(r_r_3.idm*1, 0) + coalesce(r_r_4.idm*0.75, 0) + coalesce(r_r_5.idm*0.5, 0))
      ,nullif(
        (case when r_r_1.idm is not null then 1.5 else 0 end +
        case when r_r_2.idm is not null then 1.25 else 0 end +
        case when r_r_3.idm is not null then 1 else 0 end +
        case when r_r_4.idm is not null then 0.75 else 0 end +
        case when r_r_5.idm is not null then 0.5 else 0 end)
        , 0)
    )as ema_idm
    ,(SELECT MAX(v) FROM UNNEST([r_r_1.idm, r_r_2.idm, r_r_3.idm, r_r_4.idm, r_r_5.idm]) v WHERE v IS NOT NULL) as max_idm
    ,(SELECT MIN(v) FROM UNNEST([r_r_1.idm, r_r_2.idm, r_r_3.idm, r_r_4.idm, r_r_5.idm]) v WHERE v IS NOT NULL) as min_idm
    /* finish_position (着順) -- finish_position=0は無効(失格/取消)のためNULL扱い */
    ,safe_divide(
      (coalesce(IF(r_r_1.finish_position > 0, r_r_1.finish_position, NULL), 0) + coalesce(IF(r_r_2.finish_position > 0, r_r_2.finish_position, NULL), 0) + coalesce(IF(r_r_3.finish_position > 0, r_r_3.finish_position, NULL), 0) + coalesce(IF(r_r_4.finish_position > 0, r_r_4.finish_position, NULL), 0) + coalesce(IF(r_r_5.finish_position > 0, r_r_5.finish_position, NULL), 0))
      ,nullif(
        (case when r_r_1.finish_position is not null AND r_r_1.finish_position > 0 then 1 else 0 end +
        case when r_r_2.finish_position is not null AND r_r_2.finish_position > 0 then 1 else 0 end +
        case when r_r_3.finish_position is not null AND r_r_3.finish_position > 0 then 1 else 0 end +
        case when r_r_4.finish_position is not null AND r_r_4.finish_position > 0 then 1 else 0 end +
        case when r_r_5.finish_position is not null AND r_r_5.finish_position > 0 then 1 else 0 end)
        , 0)
    ) as mean_finish_position
    ,safe_divide(
      (coalesce(IF(r_r_1.finish_position > 0, r_r_1.finish_position, NULL)*1.5, 0) + coalesce(IF(r_r_2.finish_position > 0, r_r_2.finish_position, NULL)*1.25, 0) + coalesce(IF(r_r_3.finish_position > 0, r_r_3.finish_position, NULL)*1, 0) + coalesce(IF(r_r_4.finish_position > 0, r_r_4.finish_position, NULL)*0.75, 0) + coalesce(IF(r_r_5.finish_position > 0, r_r_5.finish_position, NULL)*0.5, 0))
      ,nullif(
        (case when r_r_1.finish_position is not null AND r_r_1.finish_position > 0 then 1.5 else 0 end +
        case when r_r_2.finish_position is not null AND r_r_2.finish_position > 0 then 1.25 else 0 end +
        case when r_r_3.finish_position is not null AND r_r_3.finish_position > 0 then 1 else 0 end +
        case when r_r_4.finish_position is not null AND r_r_4.finish_position > 0 then 0.75 else 0 end +
        case when r_r_5.finish_position is not null AND r_r_5.finish_position > 0 then 0.5 else 0 end)
        , 0)
    ) as ema_finish_position
    ,(SELECT MAX(v) FROM UNNEST([r_r_1.finish_position, r_r_2.finish_position, r_r_3.finish_position, r_r_4.finish_position, r_r_5.finish_position]) v WHERE v IS NOT NULL AND v > 0) as max_finish_position
    ,(SELECT MIN(v) FROM UNNEST([r_r_1.finish_position, r_r_2.finish_position, r_r_3.finish_position, r_r_4.finish_position, r_r_5.finish_position]) v WHERE v IS NOT NULL AND v > 0) as min_finish_position
    /* win_popularity (単勝人気) */
    ,safe_divide(
      (coalesce(r_r_1.win_popularity, 0) + coalesce(r_r_2.win_popularity, 0) + coalesce(r_r_3.win_popularity, 0) + coalesce(r_r_4.win_popularity, 0) + coalesce(r_r_5.win_popularity, 0))
      ,nullif(
        (case when r_r_1.win_popularity is not null then 1 else 0 end +
        case when r_r_2.win_popularity is not null then 1 else 0 end +
        case when r_r_3.win_popularity is not null then 1 else 0 end +
        case when r_r_4.win_popularity is not null then 1 else 0 end +
        case when r_r_5.win_popularity is not null then 1 else 0 end)
        , 0)
    ) as mean_win_popularity
    ,safe_divide(
      (coalesce(r_r_1.win_popularity*1.5, 0) + coalesce(r_r_2.win_popularity*1.25, 0) + coalesce(r_r_3.win_popularity*1, 0) + coalesce(r_r_4.win_popularity*0.75, 0) + coalesce(r_r_5.win_popularity*0.5, 0))
      ,nullif(
        (case when r_r_1.win_popularity is not null then 1.5 else 0 end +
        case when r_r_2.win_popularity is not null then 1.25 else 0 end +
        case when r_r_3.win_popularity is not null then 1 else 0 end +
        case when r_r_4.win_popularity is not null then 0.75 else 0 end +
        case when r_r_5.win_popularity is not null then 0.5 else 0 end)
        , 0)
    ) as ema_win_popularity
    ,(SELECT MAX(v) FROM UNNEST([r_r_1.win_popularity, r_r_2.win_popularity, r_r_3.win_popularity, r_r_4.win_popularity, r_r_5.win_popularity]) v WHERE v IS NOT NULL) as max_win_popularity
    ,(SELECT MIN(v) FROM UNNEST([r_r_1.win_popularity, r_r_2.win_popularity, r_r_3.win_popularity, r_r_4.win_popularity, r_r_5.win_popularity]) v WHERE v IS NOT NULL) as min_win_popularity
    /* finish_time (走破タイム) */
    ,safe_divide(
      (coalesce(r_r_1.finish_time, 0) + coalesce(r_r_2.finish_time, 0) + coalesce(r_r_3.finish_time, 0) + coalesce(r_r_4.finish_time, 0) + coalesce(r_r_5.finish_time, 0))
      ,nullif(
        (case when r_r_1.finish_time is not null then 1 else 0 end +
        case when r_r_2.finish_time is not null then 1 else 0 end +
        case when r_r_3.finish_time is not null then 1 else 0 end +
        case when r_r_4.finish_time is not null then 1 else 0 end +
        case when r_r_5.finish_time is not null then 1 else 0 end)
        , 0)
    ) as mean_finish_time
    ,safe_divide(
      (coalesce(r_r_1.finish_time*1.5, 0) + coalesce(r_r_2.finish_time*1.25, 0) + coalesce(r_r_3.finish_time*1, 0) + coalesce(r_r_4.finish_time*0.75, 0) + coalesce(r_r_5.finish_time*0.5, 0))
      ,nullif(
        (case when r_r_1.finish_time is not null then 1.5 else 0 end +
        case when r_r_2.finish_time is not null then 1.25 else 0 end +
        case when r_r_3.finish_time is not null then 1 else 0 end +
        case when r_r_4.finish_time is not null then 0.75 else 0 end +
        case when r_r_5.finish_time is not null then 0.5 else 0 end)
        , 0)
    ) as ema_finish_time
    /* last_3f (上がり3Fタイム) */
    ,safe_divide(
      (coalesce(r_r_1.last_3f_time, 0) + coalesce(r_r_2.last_3f_time, 0) + coalesce(r_r_3.last_3f_time, 0) + coalesce(r_r_4.last_3f_time, 0) + coalesce(r_r_5.last_3f_time, 0))
      ,nullif(
        (case when r_r_1.last_3f_time is not null then 1 else 0 end +
        case when r_r_2.last_3f_time is not null then 1 else 0 end +
        case when r_r_3.last_3f_time is not null then 1 else 0 end +
        case when r_r_4.last_3f_time is not null then 1 else 0 end +
        case when r_r_5.last_3f_time is not null then 1 else 0 end)
        , 0)
    ) as mean_last_3f
    ,safe_divide(
      (coalesce(r_r_1.last_3f_time*1.5, 0) + coalesce(r_r_2.last_3f_time*1.25, 0) + coalesce(r_r_3.last_3f_time*1, 0) + coalesce(r_r_4.last_3f_time*0.75, 0) + coalesce(r_r_5.last_3f_time*0.5, 0))
      ,nullif(
        (case when r_r_1.last_3f_time is not null then 1.5 else 0 end +
        case when r_r_2.last_3f_time is not null then 1.25 else 0 end +
        case when r_r_3.last_3f_time is not null then 1 else 0 end +
        case when r_r_4.last_3f_time is not null then 0.75 else 0 end +
        case when r_r_5.last_3f_time is not null then 0.5 else 0 end)
        , 0)
    ) as ema_last_3f
    /* corner_position (4角通過順位) */
    ,safe_divide(
      (coalesce(r_r_1.corner_position_4, 0) + coalesce(r_r_2.corner_position_4, 0) + coalesce(r_r_3.corner_position_4, 0) + coalesce(r_r_4.corner_position_4, 0) + coalesce(r_r_5.corner_position_4, 0))
      ,nullif(
        (case when r_r_1.corner_position_4 is not null then 1 else 0 end +
        case when r_r_2.corner_position_4 is not null then 1 else 0 end +
        case when r_r_3.corner_position_4 is not null then 1 else 0 end +
        case when r_r_4.corner_position_4 is not null then 1 else 0 end +
        case when r_r_5.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as mean_corner_position
    ,safe_divide(
      (coalesce(r_r_1.corner_position_4*1.5, 0) + coalesce(r_r_2.corner_position_4*1.25, 0) + coalesce(r_r_3.corner_position_4*1, 0) + coalesce(r_r_4.corner_position_4*0.75, 0) + coalesce(r_r_5.corner_position_4*0.5, 0))
      ,nullif(
        (case when r_r_1.corner_position_4 is not null then 1.5 else 0 end +
        case when r_r_2.corner_position_4 is not null then 1.25 else 0 end +
        case when r_r_3.corner_position_4 is not null then 1 else 0 end +
        case when r_r_4.corner_position_4 is not null then 0.75 else 0 end +
        case when r_r_5.corner_position_4 is not null then 0.5 else 0 end)
        , 0)
    ) as ema_corner_position
    /* 脚質分類用: 過去1〜5走コーナー平均通過順位（全コーナー非NULL値平均、Issue #343） */
    ,safe_divide(
      (coalesce(r_r_1.corner_position_1, 0) + coalesce(r_r_1.corner_position_2, 0) + coalesce(r_r_1.corner_position_3, 0) + coalesce(r_r_1.corner_position_4, 0))
      ,nullif(
        (case when r_r_1.corner_position_1 is not null then 1 else 0 end +
        case when r_r_1.corner_position_2 is not null then 1 else 0 end +
        case when r_r_1.corner_position_3 is not null then 1 else 0 end +
        case when r_r_1.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as avg_corner_prev1
    ,safe_divide(
      (coalesce(r_r_2.corner_position_1, 0) + coalesce(r_r_2.corner_position_2, 0) + coalesce(r_r_2.corner_position_3, 0) + coalesce(r_r_2.corner_position_4, 0))
      ,nullif(
        (case when r_r_2.corner_position_1 is not null then 1 else 0 end +
        case when r_r_2.corner_position_2 is not null then 1 else 0 end +
        case when r_r_2.corner_position_3 is not null then 1 else 0 end +
        case when r_r_2.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as avg_corner_prev2
    ,safe_divide(
      (coalesce(r_r_3.corner_position_1, 0) + coalesce(r_r_3.corner_position_2, 0) + coalesce(r_r_3.corner_position_3, 0) + coalesce(r_r_3.corner_position_4, 0))
      ,nullif(
        (case when r_r_3.corner_position_1 is not null then 1 else 0 end +
        case when r_r_3.corner_position_2 is not null then 1 else 0 end +
        case when r_r_3.corner_position_3 is not null then 1 else 0 end +
        case when r_r_3.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as avg_corner_prev3
    ,safe_divide(
      (coalesce(r_r_4.corner_position_1, 0) + coalesce(r_r_4.corner_position_2, 0) + coalesce(r_r_4.corner_position_3, 0) + coalesce(r_r_4.corner_position_4, 0))
      ,nullif(
        (case when r_r_4.corner_position_1 is not null then 1 else 0 end +
        case when r_r_4.corner_position_2 is not null then 1 else 0 end +
        case when r_r_4.corner_position_3 is not null then 1 else 0 end +
        case when r_r_4.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as avg_corner_prev4
    ,safe_divide(
      (coalesce(r_r_5.corner_position_1, 0) + coalesce(r_r_5.corner_position_2, 0) + coalesce(r_r_5.corner_position_3, 0) + coalesce(r_r_5.corner_position_4, 0))
      ,nullif(
        (case when r_r_5.corner_position_1 is not null then 1 else 0 end +
        case when r_r_5.corner_position_2 is not null then 1 else 0 end +
        case when r_r_5.corner_position_3 is not null then 1 else 0 end +
        case when r_r_5.corner_position_4 is not null then 1 else 0 end)
        , 0)
    ) as avg_corner_prev5
    /* last_3f_rank (レース内上がり順位) の集計 */
    ,safe_divide(
      (coalesce(r_l3f_1.last_3f_rank_in_race, 0) + coalesce(r_l3f_2.last_3f_rank_in_race, 0) + coalesce(r_l3f_3.last_3f_rank_in_race, 0) + coalesce(r_l3f_4.last_3f_rank_in_race, 0) + coalesce(r_l3f_5.last_3f_rank_in_race, 0))
      ,nullif(
        (case when r_l3f_1.last_3f_rank_in_race is not null then 1 else 0 end +
        case when r_l3f_2.last_3f_rank_in_race is not null then 1 else 0 end +
        case when r_l3f_3.last_3f_rank_in_race is not null then 1 else 0 end +
        case when r_l3f_4.last_3f_rank_in_race is not null then 1 else 0 end +
        case when r_l3f_5.last_3f_rank_in_race is not null then 1 else 0 end)
        , 0)
    ) as mean_last_3f_rank
    -- ローテーション特徴量用: 2〜5走前までの週数差分
    ,round(safe_divide(date_diff(t_b_r_e.race_date, r_r_3.race_date, day), 7))-1 as race_date_diff_3
    ,round(safe_divide(date_diff(t_b_r_e.race_date, r_r_4.race_date, day), 7))-1 as race_date_diff_4
    ,round(safe_divide(date_diff(t_b_r_e.race_date, r_r_5.race_date, day), 7))-1 as race_date_diff_5
    -- 前走との斤量差（horse_results=当日予定斤量, race_results=前走実績斤量）
    ,t_b_r_e.weight_carried - r_r_1.weight_carried as weight_carried_diff
  from
    temp_base_race_entries as t_b_r_e
    left join `{project_id}`.raw.race_results as r_r_1
      on t_b_r_e.prev_race_key_1 = concat(r_r_1.horse_id, format_date('%Y%m%d', r_r_1.race_date))
      and r_r_1.race_date < t_b_r_e.race_date
    left join temp_race_horse_count as t_r_h_c_1
      on r_r_1.race_id = t_r_h_c_1.race_id
    left join `{project_id}`.raw.race_results as r_r_2
      on t_b_r_e.prev_race_key_2 = concat(r_r_2.horse_id, format_date('%Y%m%d', r_r_2.race_date))
      and r_r_2.race_date < t_b_r_e.race_date
    left join temp_race_horse_count as t_r_h_c_2
      on r_r_2.race_id = t_r_h_c_2.race_id
    left join `{project_id}`.raw.race_results as r_r_3
      on t_b_r_e.prev_race_key_3 = concat(r_r_3.horse_id, format_date('%Y%m%d', r_r_3.race_date))
      and r_r_3.race_date < t_b_r_e.race_date
    left join temp_race_horse_count as t_r_h_c_3
      on r_r_3.race_id = t_r_h_c_3.race_id
    left join `{project_id}`.raw.race_results as r_r_4
      on t_b_r_e.prev_race_key_4 = concat(r_r_4.horse_id, format_date('%Y%m%d', r_r_4.race_date))
      and r_r_4.race_date < t_b_r_e.race_date
    left join temp_race_horse_count as t_r_h_c_4
      on r_r_4.race_id = t_r_h_c_4.race_id
    left join `{project_id}`.raw.race_results as r_r_5
      on t_b_r_e.prev_race_key_5 = concat(r_r_5.horse_id, format_date('%Y%m%d', r_r_5.race_date))
      and r_r_5.race_date < t_b_r_e.race_date
    left join temp_race_horse_count as t_r_h_c_5
      on r_r_5.race_id = t_r_h_c_5.race_id
    left join temp_race_last3f_ranks as r_l3f_1
      on r_r_1.race_id = r_l3f_1.race_id
      and r_r_1.horse_id = r_l3f_1.horse_id
    left join temp_race_last3f_ranks as r_l3f_2
      on r_r_2.race_id = r_l3f_2.race_id
      and r_r_2.horse_id = r_l3f_2.horse_id
    left join temp_race_last3f_ranks as r_l3f_3
      on r_r_3.race_id = r_l3f_3.race_id
      and r_r_3.horse_id = r_l3f_3.horse_id
    left join temp_race_last3f_ranks as r_l3f_4
      on r_r_4.race_id = r_l3f_4.race_id
      and r_r_4.horse_id = r_l3f_4.horse_id
    left join temp_race_last3f_ranks as r_l3f_5
      on r_r_5.race_id = r_l3f_5.race_id
      and r_r_5.horse_id = r_l3f_5.horse_id
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i_prev1
      on r_r_1.race_date = v_i_prev1.race_date
      and SUBSTR(r_r_1.race_id, 1, 2) = v_i_prev1.venue_code
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i_prev2
      on r_r_2.race_date = v_i_prev2.race_date
      and SUBSTR(r_r_2.race_id, 1, 2) = v_i_prev2.venue_code
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i_prev3
      on r_r_3.race_date = v_i_prev3.race_date
      and SUBSTR(r_r_3.race_id, 1, 2) = v_i_prev3.venue_code
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i_prev4
      on r_r_4.race_date = v_i_prev4.race_date
      and SUBSTR(r_r_4.race_id, 1, 2) = v_i_prev4.venue_code
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i_prev5
      on r_r_5.race_date = v_i_prev5.race_date
      and SUBSTR(r_r_5.race_id, 1, 2) = v_i_prev5.venue_code
)

,temp_past_race_features2 as (
  select
    t_p_r_f.*
    -- 馬齢ごとのセグメントに対して、指数の大小を計算
    ,row_number() over (partition by t_p_r_f.race_id, t_p_r_f.horse_age_segment order by t_p_r_f.idm) as age_segment_idm
    ,row_number() over (partition by t_p_r_f.race_id, t_p_r_f.horse_age_segment order by t_p_r_f.mean_idm) as age_segment_mean_idm
    ,row_number() over (partition by t_p_r_f.race_id, t_p_r_f.horse_age_segment order by t_p_r_f.ema_idm) as age_segment_ema_idm
    ,row_number() over (partition by t_p_r_f.race_id, t_p_r_f.horse_age_segment order by t_p_r_f.max_idm) as age_segment_max_idm
    /* 指数のTOPとの差分を計算 */
    ,t_p_r_f.idm - max(t_p_r_f.idm) over (partition by t_p_r_f.race_id) as idm_diff
    ,t_p_r_f.mean_idm - max(t_p_r_f.mean_idm) over (partition by t_p_r_f.race_id) as mean_idm_diff
    ,t_p_r_f.ema_idm - max(t_p_r_f.ema_idm) over (partition by t_p_r_f.race_id) as ema_idm_diff
    ,t_p_r_f.max_idm - max(t_p_r_f.max_idm) over (partition by t_p_r_f.race_id) as max_idm_diff
    /* finish_position_rate (着順率) */
    ,safe_divide(
      (coalesce(finish_position_rate_1, 0) + coalesce(finish_position_rate_2, 0) + coalesce(finish_position_rate_3, 0) + coalesce(finish_position_rate_4, 0) + coalesce(finish_position_rate_5, 0))
      ,nullif(
        (case when finish_position_rate_1 is not null then 1 else 0 end +
        case when finish_position_rate_2 is not null then 1 else 0 end +
        case when finish_position_rate_3 is not null then 1 else 0 end +
        case when finish_position_rate_4 is not null then 1 else 0 end +
        case when finish_position_rate_5 is not null then 1 else 0 end)
        , 0)
    ) as mean_finish_position_rate
    ,safe_divide(
      (coalesce(finish_position_rate_1*1.5, 0) + coalesce(finish_position_rate_2*1.25, 0) + coalesce(finish_position_rate_3*1, 0) + coalesce(finish_position_rate_4*0.75, 0) + coalesce(finish_position_rate_5*0.5, 0))
      ,nullif(
        (case when finish_position_rate_1 is not null then 1.5 else 0 end + 
        case when finish_position_rate_2 is not null then 1.25 else 0 end + 
        case when finish_position_rate_3 is not null then 1 else 0 end + 
        case when finish_position_rate_4 is not null then 0.75 else 0 end + 
        case when finish_position_rate_5 is not null then 0.5 else 0 end)
        , 0)
    ) as ema_finish_position_rate
    ,(SELECT MAX(v) FROM UNNEST([finish_position_rate_1, finish_position_rate_2, finish_position_rate_3, finish_position_rate_4, finish_position_rate_5]) v WHERE v IS NOT NULL) as max_finish_position_rate
    ,(SELECT MIN(v) FROM UNNEST([finish_position_rate_1, finish_position_rate_2, finish_position_rate_3, finish_position_rate_4, finish_position_rate_5]) v WHERE v IS NOT NULL) as min_finish_position_rate

    /* popularity_rate (人気率) */
    ,safe_divide(
      (coalesce(popularity_rate_1, 0) + coalesce(popularity_rate_2, 0) + coalesce(popularity_rate_3, 0) + coalesce(popularity_rate_4, 0) + coalesce(popularity_rate_5, 0))
      ,nullif(
        (case when popularity_rate_1 is not null then 1 else 0 end + 
        case when popularity_rate_2 is not null then 1 else 0 end + 
        case when popularity_rate_3 is not null then 1 else 0 end + 
        case when popularity_rate_4 is not null then 1 else 0 end + 
        case when popularity_rate_5 is not null then 1 else 0 end)
        , 0)
      ) as mean_popularity_rate
    ,safe_divide(
      (coalesce(popularity_rate_1*1.5, 0) + coalesce(popularity_rate_2*1.25, 0) + coalesce(popularity_rate_3*1, 0) + coalesce(popularity_rate_4*0.75, 0) + coalesce(popularity_rate_5*0.5, 0))
      ,nullif(
        (case when popularity_rate_1 is not null then 1.5 else 0 end + 
        case when popularity_rate_2 is not null then 1.25 else 0 end + 
        case when popularity_rate_3 is not null then 1 else 0 end + 
        case when popularity_rate_4 is not null then 0.75 else 0 end + 
        case when popularity_rate_5 is not null then 0.5 else 0 end)
        , 0)
    ) as ema_popularity_rate
    ,(SELECT MAX(v) FROM UNNEST([popularity_rate_1, popularity_rate_2, popularity_rate_3, popularity_rate_4, popularity_rate_5]) v WHERE v IS NOT NULL) as max_popularity_rate
    ,(SELECT MIN(v) FROM UNNEST([popularity_rate_1, popularity_rate_2, popularity_rate_3, popularity_rate_4, popularity_rate_5]) v WHERE v IS NOT NULL) as min_popularity_rate

    /* upside_rate (上昇率) */
    ,safe_divide(
      (coalesce(upside_rate_1, 0) + coalesce(upside_rate_2, 0) + coalesce(upside_rate_3, 0) + coalesce(upside_rate_4, 0) + coalesce(upside_rate_5, 0))
      ,nullif(
        (case when upside_rate_1 is not null then 1 else 0 end + 
        case when upside_rate_2 is not null then 1 else 0 end + 
        case when upside_rate_3 is not null then 1 else 0 end + 
        case when upside_rate_4 is not null then 1 else 0 end + 
        case when upside_rate_5 is not null then 1 else 0 end)
        , 0)
    ) as mean_upside_rate
    ,safe_divide(
      (coalesce(upside_rate_1*1.5, 0) + coalesce(upside_rate_2*1.25, 0) + coalesce(upside_rate_3*1, 0) + coalesce(upside_rate_4*0.75, 0) + coalesce(upside_rate_5*0.5, 0))
      ,nullif(
        (case when upside_rate_1 is not null then 1.5 else 0 end + 
        case when upside_rate_2 is not null then 1.25 else 0 end + 
        case when upside_rate_3 is not null then 1 else 0 end + 
        case when upside_rate_4 is not null then 0.75 else 0 end + 
        case when upside_rate_5 is not null then 0.5 else 0 end)
      , 0)
    ) as ema_upside_rate
    ,(SELECT MAX(v) FROM UNNEST([upside_rate_1, upside_rate_2, upside_rate_3, upside_rate_4, upside_rate_5]) v WHERE v IS NOT NULL) as max_upside_rate
    ,(SELECT MIN(v) FROM UNNEST([upside_rate_1, upside_rate_2, upside_rate_3, upside_rate_4, upside_rate_5]) v WHERE v IS NOT NULL) as min_upside_rate
    /* 走破タイム正規化: 1走前の同場・同距離・同馬場状態での偏差値（当該レース日付より前のデータのみ使用） */
    ,safe_divide(
      t_p_r_f.finish_time_1 - avg(t_p_r_f.finish_time_1) over (
        partition by t_p_r_f.venue_code_prev_1, t_p_r_f.distance_prev_1, t_p_r_f.track_condition_prev_1
        order by unix_date(t_p_r_f.race_date)
        range between unbounded preceding and 1 preceding
      )
      ,nullif(stddev(t_p_r_f.finish_time_1) over (
        partition by t_p_r_f.venue_code_prev_1, t_p_r_f.distance_prev_1, t_p_r_f.track_condition_prev_1
        order by unix_date(t_p_r_f.race_date)
        range between unbounded preceding and 1 preceding
      ), 0)
    ) as finish_time_normalized
    /* 上がり3F正規化: 1走前の同場・同距離・同馬場状態での偏差値（当該レース日付より前のデータのみ使用） */
    ,safe_divide(
      t_p_r_f.last_3f_1 - avg(t_p_r_f.last_3f_1) over (
        partition by t_p_r_f.venue_code_prev_1, t_p_r_f.distance_prev_1, t_p_r_f.track_condition_prev_1
        order by unix_date(t_p_r_f.race_date)
        range between unbounded preceding and 1 preceding
      )
      ,nullif(stddev(t_p_r_f.last_3f_1) over (
        partition by t_p_r_f.venue_code_prev_1, t_p_r_f.distance_prev_1, t_p_r_f.track_condition_prev_1
        order by unix_date(t_p_r_f.race_date)
        range between unbounded preceding and 1 preceding
      ), 0)
    ) as last_3f_normalized
    -- ローテーション特徴量
    ,case
      when t_p_r_f.race_date_diff_1 is null then null
      when t_p_r_f.race_date_diff_1 >= 12 then 1
      else 0
    end as is_fresh
    ,case
      when t_p_r_f.race_date_diff_1 is null then null
      when t_p_r_f.race_date_diff_1 <= 1 then 1
      else 0
    end as is_renso
    ,IF(
      t_p_r_f.idm_1 is not null and t_p_r_f.idm_3 is not null,
      t_p_r_f.idm_1 - t_p_r_f.idm_3,
      null
    ) as idm_trend_3
    ,IF(
      t_p_r_f.finish_position_1 is not null and t_p_r_f.finish_position_1 > 0
      and t_p_r_f.finish_position_3 is not null and t_p_r_f.finish_position_3 > 0,
      t_p_r_f.finish_position_3 - t_p_r_f.finish_position_1,
      null
    ) as finish_position_trend_3
    ,case
      when t_p_r_f.race_date_diff_1 is null then 1
      when t_p_r_f.race_date_diff_1 >= 12 then 1
      when t_p_r_f.race_date_diff_2 is null or (t_p_r_f.race_date_diff_2 - t_p_r_f.race_date_diff_1) >= 12 then 2
      when t_p_r_f.race_date_diff_3 is null or (t_p_r_f.race_date_diff_3 - t_p_r_f.race_date_diff_2) >= 12 then 3
      when t_p_r_f.race_date_diff_4 is null or (t_p_r_f.race_date_diff_4 - t_p_r_f.race_date_diff_3) >= 12 then 4
      when t_p_r_f.race_date_diff_5 is null or (t_p_r_f.race_date_diff_5 - t_p_r_f.race_date_diff_4) >= 12 then 5
      else 6
    end as continuous_run_count
    -- レース内相対指標（斤量）
    ,rank() over (partition by t_p_r_f.race_id order by t_p_r_f.weight_carried) as weight_carried_rank
    ,t_p_r_f.weight_carried - avg(t_p_r_f.weight_carried) over (partition by t_p_r_f.race_id) as weight_carried_diff_from_mean
    -- レース内相対指標（先行力・脚質分布）
    ,count(case when t_p_r_f.running_style <= 2 then 1 end) over (partition by t_p_r_f.race_id) as running_style_front_count
    ,safe_divide(
      count(case when t_p_r_f.running_style <= 2 then 1 end) over (partition by t_p_r_f.race_id),
      t_p_r_f.num_horses
    ) as running_style_front_ratio
    ,case
      when t_p_r_f.running_style = 1
        and count(case when t_p_r_f.running_style = 1 then 1 end) over (partition by t_p_r_f.race_id) = 1
      then 1
      else 0
    end as is_sole_leader
    -- レース内相対指標（混戦度: idm分散）
    ,stddev(t_p_r_f.idm) over (partition by t_p_r_f.race_id) as race_idm_std
    ,safe_divide(
      stddev(t_p_r_f.idm) over (partition by t_p_r_f.race_id),
      nullif(avg(t_p_r_f.idm) over (partition by t_p_r_f.race_id), 0)
    ) as race_idm_cv
    -- レース内相対指標（馬番の内外位置）
    ,safe_divide(t_p_r_f.horse_number, t_p_r_f.num_horses) as horse_number_ratio
    -- コーナー通過順変化（折り合い指標, Issue #306）
    -- 正値 = 前につけて失速、負値 = 後方から押し上げ。直線コース等でcorner1がNULLの場合はNULL
    ,t_p_r_f.corner4_prev_1 - t_p_r_f.corner1_prev_1 as corner_gain_1to4_prev_1
    ,t_p_r_f.corner4_prev_2 - t_p_r_f.corner1_prev_2 as corner_gain_1to4_prev_2
    ,t_p_r_f.corner4_prev_3 - t_p_r_f.corner1_prev_3 as corner_gain_1to4_prev_3
    ,safe_divide(
      (coalesce(t_p_r_f.corner4_prev_1 - t_p_r_f.corner1_prev_1, 0)
       + coalesce(t_p_r_f.corner4_prev_2 - t_p_r_f.corner1_prev_2, 0)
       + coalesce(t_p_r_f.corner4_prev_3 - t_p_r_f.corner1_prev_3, 0))
      ,nullif(
        (case when t_p_r_f.corner1_prev_1 is not null and t_p_r_f.corner4_prev_1 is not null then 1 else 0 end +
        case when t_p_r_f.corner1_prev_2 is not null and t_p_r_f.corner4_prev_2 is not null then 1 else 0 end +
        case when t_p_r_f.corner1_prev_3 is not null and t_p_r_f.corner4_prev_3 is not null then 1 else 0 end)
        , 0)
    ) as mean_corner_gain_1to4
    ,safe_divide(
      (coalesce((t_p_r_f.corner4_prev_1 - t_p_r_f.corner1_prev_1) * 1.5, 0)
       + coalesce((t_p_r_f.corner4_prev_2 - t_p_r_f.corner1_prev_2) * 1.0, 0)
       + coalesce((t_p_r_f.corner4_prev_3 - t_p_r_f.corner1_prev_3) * 0.5, 0))
      ,nullif(
        (case when t_p_r_f.corner1_prev_1 is not null and t_p_r_f.corner4_prev_1 is not null then 1.5 else 0 end +
        case when t_p_r_f.corner1_prev_2 is not null and t_p_r_f.corner4_prev_2 is not null then 1.0 else 0 end +
        case when t_p_r_f.corner1_prev_3 is not null and t_p_r_f.corner4_prev_3 is not null then 0.5 else 0 end)
        , 0)
    ) as ema_corner_gain_1to4
    ,safe_divide(
      (coalesce(t_p_r_f.corner1_prev_1, 0) + coalesce(t_p_r_f.corner1_prev_2, 0) + coalesce(t_p_r_f.corner1_prev_3, 0))
      ,nullif(
        (case when t_p_r_f.corner1_prev_1 is not null then 1 else 0 end +
        case when t_p_r_f.corner1_prev_2 is not null then 1 else 0 end +
        case when t_p_r_f.corner1_prev_3 is not null then 1 else 0 end)
        , 0)
    ) as mean_corner1_position
    ,IF(
      t_p_r_f.corner1_prev_1 is not null
        and t_p_r_f.finish_position_1 is not null
        and t_p_r_f.finish_position_1 > 0,
      t_p_r_f.corner1_prev_1 - t_p_r_f.finish_position_1,
      null
    ) as corner1_to_finish_delta_prev_1
    -- コース取り傾向集計（Issue #310）
    ,safe_divide(
      coalesce(t_p_r_f.course_position_prev1, 0) +
      coalesce(t_p_r_f.course_position_prev2, 0) +
      coalesce(t_p_r_f.course_position_prev3, 0),
      nullif(
        (case when t_p_r_f.course_position_prev1 is not null then 1 else 0 end +
        case when t_p_r_f.course_position_prev2 is not null then 1 else 0 end +
        case when t_p_r_f.course_position_prev3 is not null then 1 else 0 end),
        0)
    ) as mean_course_position
    ,safe_divide(
      coalesce(t_p_r_f.course_position_prev1 * 1.5, 0) +
      coalesce(t_p_r_f.course_position_prev2 * 1.0, 0) +
      coalesce(t_p_r_f.course_position_prev3 * 0.5, 0),
      nullif(
        (case when t_p_r_f.course_position_prev1 is not null then 1.5 else 0 end +
        case when t_p_r_f.course_position_prev2 is not null then 1.0 else 0 end +
        case when t_p_r_f.course_position_prev3 is not null then 0.5 else 0 end),
        0)
    ) as ema_course_position
    /* 脚質スコア（front=1, mid_front=2, mid=3, back=4）（Issue #343） */
    ,case
      when t_p_r_f.avg_corner_prev1 is null then null
      when t_p_r_f.avg_corner_prev1 <= 3.5 then 1
      when t_p_r_f.avg_corner_prev1 <= 7.0 then 2
      when t_p_r_f.avg_corner_prev1 <= 10.0 then 3
      else 4
    end as gate_style_prev1
    ,case
      when t_p_r_f.avg_corner_prev2 is null then null
      when t_p_r_f.avg_corner_prev2 <= 3.5 then 1
      when t_p_r_f.avg_corner_prev2 <= 7.0 then 2
      when t_p_r_f.avg_corner_prev2 <= 10.0 then 3
      else 4
    end as gate_style_prev2
    ,case
      when t_p_r_f.avg_corner_prev3 is null then null
      when t_p_r_f.avg_corner_prev3 <= 3.5 then 1
      when t_p_r_f.avg_corner_prev3 <= 7.0 then 2
      when t_p_r_f.avg_corner_prev3 <= 10.0 then 3
      else 4
    end as gate_style_prev3
    ,case
      when t_p_r_f.avg_corner_prev4 is null then null
      when t_p_r_f.avg_corner_prev4 <= 3.5 then 1
      when t_p_r_f.avg_corner_prev4 <= 7.0 then 2
      when t_p_r_f.avg_corner_prev4 <= 10.0 then 3
      else 4
    end as gate_style_prev4
    ,case
      when t_p_r_f.avg_corner_prev5 is null then null
      when t_p_r_f.avg_corner_prev5 <= 3.5 then 1
      when t_p_r_f.avg_corner_prev5 <= 7.0 then 2
      when t_p_r_f.avg_corner_prev5 <= 10.0 then 3
      else 4
    end as gate_style_prev5
    /* 近5走脚質スコア平均（Issue #343） */
    ,safe_divide(
      (
        coalesce(case when t_p_r_f.avg_corner_prev1 is null then null when t_p_r_f.avg_corner_prev1 <= 3.5 then 1 when t_p_r_f.avg_corner_prev1 <= 7.0 then 2 when t_p_r_f.avg_corner_prev1 <= 10.0 then 3 else 4 end, 0) +
        coalesce(case when t_p_r_f.avg_corner_prev2 is null then null when t_p_r_f.avg_corner_prev2 <= 3.5 then 1 when t_p_r_f.avg_corner_prev2 <= 7.0 then 2 when t_p_r_f.avg_corner_prev2 <= 10.0 then 3 else 4 end, 0) +
        coalesce(case when t_p_r_f.avg_corner_prev3 is null then null when t_p_r_f.avg_corner_prev3 <= 3.5 then 1 when t_p_r_f.avg_corner_prev3 <= 7.0 then 2 when t_p_r_f.avg_corner_prev3 <= 10.0 then 3 else 4 end, 0) +
        coalesce(case when t_p_r_f.avg_corner_prev4 is null then null when t_p_r_f.avg_corner_prev4 <= 3.5 then 1 when t_p_r_f.avg_corner_prev4 <= 7.0 then 2 when t_p_r_f.avg_corner_prev4 <= 10.0 then 3 else 4 end, 0) +
        coalesce(case when t_p_r_f.avg_corner_prev5 is null then null when t_p_r_f.avg_corner_prev5 <= 3.5 then 1 when t_p_r_f.avg_corner_prev5 <= 7.0 then 2 when t_p_r_f.avg_corner_prev5 <= 10.0 then 3 else 4 end, 0)
      ),
      nullif(
        (case when t_p_r_f.avg_corner_prev1 is not null then 1 else 0 end +
        case when t_p_r_f.avg_corner_prev2 is not null then 1 else 0 end +
        case when t_p_r_f.avg_corner_prev3 is not null then 1 else 0 end +
        case when t_p_r_f.avg_corner_prev4 is not null then 1 else 0 end +
        case when t_p_r_f.avg_corner_prev5 is not null then 1 else 0 end),
        0)
    ) as avg_gate_style_score
    /* 近走上がり3F レース内相対順位: 改善トレンドと平均（Issue #346）
       last3f_rank_improvement_3: 前1走順位 - 前3走順位（負=改善中、正=悪化）
       last3f_rank_avg_3: 前1〜3走の平均上がり3F順位 */
    ,case
      when t_p_r_f.last_3f_rank_in_race_1 is null or t_p_r_f.last_3f_rank_in_race_3 is null then null
      else t_p_r_f.last_3f_rank_in_race_1 - t_p_r_f.last_3f_rank_in_race_3
    end as last3f_rank_improvement_3
    ,safe_divide(
      coalesce(t_p_r_f.last_3f_rank_in_race_1, 0) + coalesce(t_p_r_f.last_3f_rank_in_race_2, 0) + coalesce(t_p_r_f.last_3f_rank_in_race_3, 0),
      nullif(
        case when t_p_r_f.last_3f_rank_in_race_1 is not null then 1 else 0 end +
        case when t_p_r_f.last_3f_rank_in_race_2 is not null then 1 else 0 end +
        case when t_p_r_f.last_3f_rank_in_race_3 is not null then 1 else 0 end,
        0)
    ) as last3f_rank_avg_3
  from
    temp_past_race_features as t_p_r_f
)

/* その馬の実績・開催情報から集計できる特徴量 */
,temp_horse_master_feature as (
  select
    h_r.race_id
    ,r_i.race_date
    ,r_i.venue_code
    ,r_i.venue_name
    ,v_i.turf_condition_code
    ,v_i.turf_condition_inner
    ,v_i.turf_condition_outer
    ,v_i.turf_bias
    ,v_i.straight_bias_innermost
    ,v_i.straight_bias_inner
    ,v_i.straight_bias_outer
    ,v_i.straight_bias_outermost
    ,v_i.dirt_condition_code
    ,v_i.dirt_condition_inner
    ,v_i.dirt_condition_outer
    ,v_i.dirt_bias
    ,r_i.race_number
    ,r_i.race_name
    ,r_i.course_type
    ,r_i.distance
    ,r_i.direction
    ,r_i.age_condition
    ,r_i.race_class
    ,coalesce(t_r_h_c.num_horses, r_i.num_horses) as num_horses
    ,h_r.horse_number
    -- 全成績
    ,safe_divide(h_e.jra_win+h_e.jra_place+h_e.jra_show, h_e.jra_win+h_e.jra_place+h_e.jra_show+h_e.jra_out) as top3_finish_rate
    ,safe_divide(h_e.jra_win+h_e.jra_place, h_e.jra_win+h_e.jra_place+h_e.jra_show+h_e.jra_out) as top2_finish_rate
    ,safe_divide(h_e.jra_win, h_e.jra_win+h_e.jra_place+h_e.jra_show+h_e.jra_out) as top1_finish_rate
    -- 芝ダート障害別成績
    ,safe_divide(h_e.surface_win+h_e.surface_place+h_e.surface_show, h_e.surface_win+h_e.surface_place+h_e.surface_show+h_e.surface_out) as surface_top3_finish_rate
    ,safe_divide(h_e.surface_win+h_e.surface_place, h_e.surface_win+h_e.surface_place+h_e.surface_show+h_e.surface_out) as surface_top2_finish_rate
    ,safe_divide(h_e.surface_win, h_e.surface_win+h_e.surface_place+h_e.surface_show+h_e.surface_out) as surface_top1_finish_rate
    ,case
      when h_e.surface_win+h_e.surface_place+h_e.surface_show+h_e.surface_out = 0 then 1
      else 0
    end as new_surface_flag -- その条件での初出走フラグ
    -- 芝ダート障害別・距離別成績
    ,safe_divide(h_e.surface_dist_win+h_e.surface_dist_place+h_e.surface_dist_show, h_e.surface_dist_win+h_e.surface_dist_place+h_e.surface_dist_show+h_e.surface_dist_out) as surface_dist_top3_finish_rate
    ,safe_divide(h_e.surface_dist_win+h_e.surface_dist_place, h_e.surface_dist_win+h_e.surface_dist_place+h_e.surface_dist_show+h_e.surface_dist_out) as surface_dist_top2_finish_rate
    ,safe_divide(h_e.surface_dist_win, h_e.surface_dist_win+h_e.surface_dist_place+h_e.surface_dist_show+h_e.surface_dist_out) as surface_dist_top1_finish_rate
    ,case
      when h_e.surface_dist_win+h_e.surface_dist_place+h_e.surface_dist_show+h_e.surface_dist_out = 0 then 1
      else 0
    end as new_surface_dist_flag -- その条件&距離での初出走フラグ
    -- コース別成績
    ,safe_divide(h_e.track_dist_win+h_e.track_dist_place+h_e.track_dist_show, h_e.track_dist_win+h_e.track_dist_place+h_e.track_dist_show+h_e.track_dist_out) as track_dist_top3_finish_rate
    ,safe_divide(h_e.track_dist_win+h_e.track_dist_place, h_e.track_dist_win+h_e.track_dist_place+h_e.track_dist_show+h_e.track_dist_out) as track_dist_top2_finish_rate
    ,safe_divide(h_e.track_dist_win, h_e.track_dist_win+h_e.track_dist_place+h_e.track_dist_show+h_e.track_dist_out) as track_dist_top1_finish_rate
    ,case
      when h_e.track_dist_win+h_e.track_dist_place+h_e.track_dist_show+h_e.track_dist_out = 0 then 1
      else 0
    end as new_track_dist_flag -- そのコースでの初出走フラグ
    -- ローテーション別成績
    ,safe_divide(h_e.rotation_win+h_e.rotation_place+h_e.rotation_show, h_e.rotation_win+h_e.rotation_place+h_e.rotation_show+h_e.rotation_out) as rotation_top3_finish_rate
    ,safe_divide(h_e.rotation_win+h_e.rotation_place, h_e.rotation_win+h_e.rotation_place+h_e.rotation_show+h_e.rotation_out) as rotation_top2_finish_rate
    ,safe_divide(h_e.rotation_win, h_e.rotation_win+h_e.rotation_place+h_e.rotation_show+h_e.rotation_out) as rotation_top1_finish_rate
    -- 右回り/左回り別成績
    ,safe_divide(h_e.direction_win+h_e.direction_place+h_e.direction_show, h_e.direction_win+h_e.direction_place+h_e.direction_show+h_e.direction_out) as direction_top3_finish_rate
    ,safe_divide(h_e.direction_win+h_e.direction_place, h_e.direction_win+h_e.direction_place+h_e.direction_show+h_e.direction_out) as direction_top2_finish_rate
    ,safe_divide(h_e.direction_win, h_e.direction_win+h_e.direction_place+h_e.direction_show+h_e.direction_out) as direction_top1_finish_rate
    ,case
      when h_e.direction_win+h_e.direction_place+h_e.direction_show+h_e.direction_out = 0 then 1
      else 0
    end as new_direction_flag -- その回りでの初出走フラグ
    -- 芝コンディション別成績
    ,case
      -- ダートの場合
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code in (1, 2)
        then safe_divide(h_e.good_win+h_e.good_place+h_e.good_show, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 4
        then safe_divide(h_e.heavy_win+h_e.heavy_place+h_e.heavy_show, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      -- 芝の場合
      when r_i.course_type = 'turf' and v_i.turf_condition_code in (1, 2)
        then safe_divide(h_e.good_win+h_e.good_place+h_e.good_show, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 4
        then safe_divide(h_e.heavy_win+h_e.heavy_place+h_e.heavy_show, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      else null
    end as condition_top3_finish_rate
    ,case
      -- ダートの場合
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code in (1, 2)
        then safe_divide(h_e.good_win+h_e.good_place, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win+h_e.slightly_heavy_place, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 4
        then safe_divide(h_e.heavy_win+h_e.heavy_place, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      -- 芝の場合
      when r_i.course_type = 'turf' and v_i.turf_condition_code in (1, 2)
        then safe_divide(h_e.good_win+h_e.good_place, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win+h_e.slightly_heavy_place, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 4
        then safe_divide(h_e.heavy_win+h_e.heavy_place, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      else null
    end as condition_top2_finish_rate
    ,case
      -- ダートの場合
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code in (1, 2)
        then safe_divide(h_e.good_win, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'dirt' and v_i.dirt_condition_code = 4
        then safe_divide(h_e.heavy_win, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      -- 芝の場合
      when r_i.course_type = 'turf' and v_i.turf_condition_code in (1, 2)
        then safe_divide(h_e.good_win, h_e.good_win+h_e.good_place+h_e.good_show+h_e.good_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 3
        then safe_divide(h_e.slightly_heavy_win, h_e.slightly_heavy_win+h_e.slightly_heavy_place+h_e.slightly_heavy_show+h_e.slightly_heavy_out)
      when r_i.course_type = 'turf' and v_i.turf_condition_code = 4
        then safe_divide(h_e.heavy_win, h_e.heavy_win+h_e.heavy_place+h_e.heavy_show+h_e.heavy_out)
      else null
    end as condition_top1_finish_rate
    -- ペース別成績
    ,case
      when h_r.pace_forecast = 'S'
        then safe_divide(h_e.slow_pace_win+h_e.slow_pace_place+h_e.slow_pace_show, h_e.slow_pace_win+h_e.slow_pace_place+h_e.slow_pace_show+h_e.slow_pace_out)
      when h_r.pace_forecast = 'M'
        then safe_divide(h_e.medium_pace_win+h_e.medium_pace_place+h_e.medium_pace_show, h_e.medium_pace_win+h_e.medium_pace_place+h_e.medium_pace_show+h_e.medium_pace_out)
      when h_r.pace_forecast = 'H'
        then safe_divide(h_e.high_pace_win+h_e.high_pace_place+h_e.high_pace_show, h_e.high_pace_win+h_e.high_pace_place+h_e.high_pace_show+h_e.high_pace_out)
      else null
    end as pace_top3_finish_rate
    ,case
      when h_r.pace_forecast = 'S'
        then safe_divide(h_e.slow_pace_win+h_e.slow_pace_place, h_e.slow_pace_win+h_e.slow_pace_place+h_e.slow_pace_show+h_e.slow_pace_out)
      when h_r.pace_forecast = 'M'
        then safe_divide(h_e.medium_pace_win+h_e.medium_pace_place, h_e.medium_pace_win+h_e.medium_pace_place+h_e.medium_pace_show+h_e.medium_pace_out)
      when h_r.pace_forecast = 'H'
        then safe_divide(h_e.high_pace_win+h_e.high_pace_place, h_e.high_pace_win+h_e.high_pace_place+h_e.high_pace_show+h_e.high_pace_out)
      else null
    end as pace_top2_finish_rate
    ,case
      when h_r.pace_forecast = 'S'
        then safe_divide(h_e.slow_pace_win, h_e.slow_pace_win+h_e.slow_pace_place+h_e.slow_pace_show+h_e.slow_pace_out)
      when h_r.pace_forecast = 'M'
        then safe_divide(h_e.medium_pace_win, h_e.medium_pace_win+h_e.medium_pace_place+h_e.medium_pace_show+h_e.medium_pace_out)
      when h_r.pace_forecast = 'H'
        then safe_divide(h_e.high_pace_win, h_e.high_pace_win+h_e.high_pace_place+h_e.high_pace_show+h_e.high_pace_out)
      else null
    end as pace_top1_finish_rate
    -- 季節別成績
    ,safe_divide(h_e.season_win+h_e.season_place+h_e.season_show, h_e.season_win+h_e.season_place+h_e.season_show+h_e.season_out) as season_top3_finish_rate
    ,safe_divide(h_e.season_win+h_e.season_place, h_e.season_win+h_e.season_place+h_e.season_show+h_e.season_out) as season_top2_finish_rate
    ,safe_divide(h_e.season_win, h_e.season_win+h_e.season_place+h_e.season_show+h_e.season_out) as season_top1_finish_rate
    -- 枠順別成績
    ,safe_divide(h_e.bracket_win+h_e.bracket_place+h_e.bracket_show, h_e.bracket_win+h_e.bracket_place+h_e.bracket_show+h_e.bracket_out) as bracket_top3_finish_rate
    ,safe_divide(h_e.bracket_win+h_e.bracket_place, h_e.bracket_win+h_e.bracket_place+h_e.bracket_show+h_e.bracket_out) as bracket_top2_finish_rate
    ,safe_divide(h_e.bracket_win, h_e.bracket_win+h_e.bracket_place+h_e.bracket_show+h_e.bracket_out) as bracket_top1_finish_rate
    -- 騎手別・距離別成績
    ,safe_divide(h_e.jockey_dist_win+h_e.jockey_dist_place+h_e.jockey_dist_show, h_e.jockey_dist_win+h_e.jockey_dist_place+h_e.jockey_dist_show+h_e.jockey_dist_out) as jockey_dist_top3_finish_rate
    ,safe_divide(h_e.jockey_dist_win+h_e.jockey_dist_place, h_e.jockey_dist_win+h_e.jockey_dist_place+h_e.jockey_dist_show+h_e.jockey_dist_out) as jockey_dist_top2_finish_rate
    ,safe_divide(h_e.jockey_dist_win, h_e.jockey_dist_win+h_e.jockey_dist_place+h_e.jockey_dist_show+h_e.jockey_dist_out) as jockey_dist_top1_finish_rate
    -- 騎手別・距離コース別成績
    ,safe_divide(h_e.jockey_track_dist_win+h_e.jockey_track_dist_place+h_e.jockey_track_dist_show, h_e.jockey_track_dist_win+h_e.jockey_track_dist_place+h_e.jockey_track_dist_show+h_e.jockey_track_dist_out) as jockey_track_dist_top3_finish_rate
    ,safe_divide(h_e.jockey_track_dist_win+h_e.jockey_track_dist_place, h_e.jockey_track_dist_win+h_e.jockey_track_dist_place+h_e.jockey_track_dist_show+h_e.jockey_track_dist_out) as jockey_track_dist_top2_finish_rate
    ,safe_divide(h_e.jockey_track_dist_win, h_e.jockey_track_dist_win+h_e.jockey_track_dist_place+h_e.jockey_track_dist_show+h_e.jockey_track_dist_out) as jockey_track_dist_top1_finish_rate
    -- 騎手別・調教師別成績
    ,safe_divide(h_e.jockey_trainer_win+h_e.jockey_trainer_place+h_e.jockey_trainer_show, h_e.jockey_trainer_win+h_e.jockey_trainer_place+h_e.jockey_trainer_show+h_e.jockey_trainer_out) as jockey_trainer_top3_finish_rate
    ,safe_divide(h_e.jockey_trainer_win+h_e.jockey_trainer_place, h_e.jockey_trainer_win+h_e.jockey_trainer_place+h_e.jockey_trainer_show+h_e.jockey_trainer_out) as jockey_trainer_top2_finish_rate
    ,safe_divide(h_e.jockey_trainer_win, h_e.jockey_trainer_win+h_e.jockey_trainer_place+h_e.jockey_trainer_show+h_e.jockey_trainer_out) as jockey_trainer_top1_finish_rate
    -- 騎手別・オーナー別成績
    ,safe_divide(h_e.jockey_owner_win+h_e.jockey_owner_place+h_e.jockey_owner_show, h_e.jockey_owner_win+h_e.jockey_owner_place+h_e.jockey_owner_show+h_e.jockey_owner_out) as jockey_owner_top3_finish_rate
    ,safe_divide(h_e.jockey_owner_win+h_e.jockey_owner_place, h_e.jockey_owner_win+h_e.jockey_owner_place+h_e.jockey_owner_show+h_e.jockey_owner_out) as jockey_owner_top2_finish_rate
    ,safe_divide(h_e.jockey_owner_win, h_e.jockey_owner_win+h_e.jockey_owner_place+h_e.jockey_owner_show+h_e.jockey_owner_out) as jockey_owner_top1_finish_rate
    -- 騎手別・ブリンカー別成績
    ,safe_divide(h_e.jockey_blinker_win+h_e.jockey_blinker_place+h_e.jockey_blinker_show, h_e.jockey_blinker_win+h_e.jockey_blinker_place+h_e.jockey_blinker_show+h_e.jockey_blinker_out) as jockey_blinker_top3_finish_rate
    ,safe_divide(h_e.jockey_blinker_win+h_e.jockey_blinker_place, h_e.jockey_blinker_win+h_e.jockey_blinker_place+h_e.jockey_blinker_show+h_e.jockey_blinker_out) as jockey_blinker_top2_finish_rate
    ,safe_divide(h_e.jockey_blinker_win, h_e.jockey_blinker_win+h_e.jockey_blinker_place+h_e.jockey_blinker_show+h_e.jockey_blinker_out) as jockey_blinker_top1_finish_rate
    -- 調教師別・オーナー別成績
    ,safe_divide(h_e.trainer_owner_win+h_e.trainer_owner_place+h_e.trainer_owner_show, h_e.trainer_owner_win+h_e.trainer_owner_place+h_e.trainer_owner_show+h_e.trainer_owner_out) as trainer_owner_top3_finish_rate
    ,safe_divide(h_e.trainer_owner_win+h_e.trainer_owner_place, h_e.trainer_owner_win+h_e.trainer_owner_place+h_e.trainer_owner_show+h_e.trainer_owner_out) as trainer_owner_top2_finish_rate
    ,safe_divide(h_e.trainer_owner_win, h_e.trainer_owner_win+h_e.trainer_owner_place+h_e.trainer_owner_show+h_e.trainer_owner_out) as trainer_owner_top1_finish_rate
    -- 父産駒情報
    ,case
      when r_i.course_type = 'dirt' then sire_dirt_place_rate
      when r_i.course_type = 'turf' then sire_turf_place_rate
      else null
    end as sire_surface_place_rate
    ,case
      when r_i.course_type = 'dirt' then sire_dirt_place_rate-sire_turf_place_rate
      when r_i.course_type = 'turf' then sire_turf_place_rate-sire_dirt_place_rate
      else null
    end as sire_surface_place_diff
    ,case
      when r_i.course_type = 'dirt' then safe_divide(sire_dirt_place_rate, sire_turf_place_rate)
      when r_i.course_type = 'turf' then safe_divide(sire_turf_place_rate, sire_dirt_place_rate)
      else null
    end as sire_surface_place_ratio
    ,r_i.distance-sire_avg_place_distance as sire_place_distance_diff
    -- 母父産駒情報
    ,case
      when r_i.course_type = 'dirt' then broodmare_sire_dirt_place_rate
      when r_i.course_type = 'turf' then broodmare_sire_turf_place_rate
      else null
    end as broodmare_sire_place_rate
    ,case
      when r_i.course_type = 'dirt' then broodmare_sire_dirt_place_rate-broodmare_sire_turf_place_rate
      when r_i.course_type = 'turf' then broodmare_sire_turf_place_rate-broodmare_sire_dirt_place_rate
      else null
    end as broodmare_sire_surface_place_diff
    ,case
      when r_i.course_type = 'dirt' then safe_divide(broodmare_sire_dirt_place_rate, broodmare_sire_turf_place_rate)
      when r_i.course_type = 'turf' then safe_divide(broodmare_sire_turf_place_rate, broodmare_sire_dirt_place_rate)
      else null
    end as broodmare_sire_surface_place_ratio
    ,r_i.distance-broodmare_sire_avg_place_distance as broodmare_sire_place_distance_diff
  from
    `{project_id}`.raw.race_info as r_i
    left join `{project_id}`.raw.horse_extended as h_e
      on r_i.race_id = h_e.race_id
    -- data_categoryが複数存在する場合は最大値（最新段階）を優先して参照する
    -- KAAは木曜以降に段階的に更新される（1:特別登録→2:想定確定→3:枠確定→4:前日）
    left join (
      select *
      from `{project_id}`.raw.venue_info
      qualify row_number() over (
        partition by venue_code, race_date
        order by coalesce(data_category, 0) desc
      ) = 1
    ) as v_i
      on r_i.race_date = v_i.race_date
      and r_i.venue_code = v_i.venue_code
    left join `{project_id}`.raw.horse_results as h_r
      on h_e.race_id = h_r.race_id
      and h_e.horse_number = h_r.horse_number
    left join temp_race_horse_count as t_r_h_c
      on r_i.race_id = t_r_h_c.race_id
  where
    r_i.race_date BETWEEN '{start_date}' AND '{end_date}'
    and r_i.race_class != 'A1'                                                                            -- 新馬戦を除外
    and coalesce(r_i.course_type, '') != 'obstacle'                                                       -- 障害戦を除外
    and coalesce(t_r_h_c.num_horses, r_i.num_horses) > 7                                                  -- 少頭数（7頭以下）を除外
    and not (r_i.venue_code = '04' and r_i.distance = 1000 and r_i.direction = 'straight')               -- 新潟直線1000mを除外
)

,temp_horse_master_feature2 as (
  select
    t_h_m_f.*
    -- 全体成績と個別成績の差分を比較
    ,surface_top3_finish_rate - top3_finish_rate as surface_top3_finish_rate_diff
    ,surface_top2_finish_rate - top2_finish_rate as surface_top2_finish_rate_diff
    ,surface_top1_finish_rate - top1_finish_rate as surface_top1_finish_rate_diff
    ,surface_dist_top3_finish_rate - top3_finish_rate as surface_dist_top3_finish_rate_diff
    ,surface_dist_top2_finish_rate - top2_finish_rate as surface_dist_top2_finish_rate_diff
    ,surface_dist_top1_finish_rate - top1_finish_rate as surface_dist_top1_finish_rate_diff
    ,track_dist_top3_finish_rate - top3_finish_rate as track_dist_top3_finish_rate_diff
    ,track_dist_top2_finish_rate - top2_finish_rate as track_dist_top2_finish_rate_diff
    ,track_dist_top1_finish_rate - top1_finish_rate as track_dist_top1_finish_rate_diff
    ,rotation_top3_finish_rate - top3_finish_rate as rotation_top3_finish_rate_diff
    ,rotation_top2_finish_rate - top2_finish_rate as rotation_top2_finish_rate_diff
    ,rotation_top1_finish_rate - top1_finish_rate as rotation_top1_finish_rate_diff
    ,direction_top3_finish_rate - top3_finish_rate as direction_top3_finish_rate_diff
    ,direction_top2_finish_rate - top2_finish_rate as direction_top2_finish_rate_diff
    ,direction_top1_finish_rate - top1_finish_rate as direction_top1_finish_rate_diff
    ,condition_top3_finish_rate - top3_finish_rate as condition_top3_finish_rate_diff
    ,condition_top2_finish_rate - top2_finish_rate as condition_top2_finish_rate_diff
    ,condition_top1_finish_rate - top1_finish_rate as condition_top1_finish_rate_diff
    ,pace_top3_finish_rate - top3_finish_rate as pace_top3_finish_rate_diff
    ,pace_top2_finish_rate - top2_finish_rate as pace_top2_finish_rate_diff
    ,pace_top1_finish_rate - top1_finish_rate as pace_top1_finish_rate_diff
    ,season_top3_finish_rate - top3_finish_rate as season_top3_finish_rate_diff
    ,season_top2_finish_rate - top2_finish_rate as season_top2_finish_rate_diff
    ,season_top1_finish_rate - top1_finish_rate as season_top1_finish_rate_diff
    ,bracket_top3_finish_rate - top3_finish_rate as bracket_top3_finish_rate_diff
    ,bracket_top2_finish_rate - top2_finish_rate as bracket_top2_finish_rate_diff
    ,bracket_top1_finish_rate - top1_finish_rate as bracket_top1_finish_rate_diff
  from
    temp_horse_master_feature as t_h_m_f
)

/* TEスムージング用グローバル平均（全期間3着以内率） */
,temp_global_mean_te as (
  select
    avg(case when finish_position between 1 and 3 then 1.0 else 0.0 end) as global_top3_rate
  from `{project_id}`.raw.race_results
  where finish_position > 0
    and date_diff(current_date(), race_date, day) <= 1826
)

/* TE計算の元となる全期間の騎手・調教師・種牡馬・馬自身実績履歴（当日同日レースは除外）
   horse_results を起点にすることで、race_results にまだ存在しない当日予測レースも含める。
   window関数の RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING が当日行を除外するため
   学習時の分布に影響しない。
   2段階構成: temp_te_history_raw (JOIN) → temp_te_history_base (LAG計算で前走比較カラム追加) */
,temp_te_history_raw as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,h_r.horse_id
    ,h_r.weight_carried
    ,r_i.race_date
    ,r_i.course_type
    ,r_i.venue_code
    ,r_i.distance
    ,case
      when r_i.distance < 1400 then 'sprint'
      when r_i.distance < 1800 then 'mile'
      when r_i.distance < 2200 then 'intermediate'
      else 'long'
    end as distance_band
    ,r_i.direction
    ,h_r.jockey_code
    ,h_r.trainer_code
    ,h_m.sire_name
    ,h_m.dam_name
    ,date_diff(r_i.race_date, h_m.birth_date, year) as horse_age
    ,case when r_r.finish_position between 1 and 3 then 1 else 0 end as is_top3
    ,r_i.race_class
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i
      on h_r.race_id = r_i.race_id
    inner join (
      -- horse_master に同一 horse_id が複数行存在する場合、1行に絞る（重複JOINによる行爆発を防止）
      select horse_id, sire_name, dam_name, birth_date
      from `{project_id}`.raw.horse_master
      qualify row_number() over (partition by horse_id order by horse_id) = 1
    ) as h_m on h_r.horse_id = h_m.horse_id
    left join `{project_id}`.raw.race_results as r_r
      on h_r.race_id = r_r.race_id
      and h_r.horse_number = r_r.horse_number
  where
    (r_r.finish_position > 0 or r_r.race_id is null)
    and coalesce(r_i.course_type, '') != 'obstacle'
)
,temp_te_history_base as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    ,course_type
    ,venue_code
    ,distance
    ,distance_band
    ,direction
    ,jockey_code
    ,trainer_code
    ,sire_name
    ,dam_name
    ,horse_age
    ,is_top3
    ,case
      when horse_age = 2 then '2yo'
      when horse_age = 3 then '3yo'
      when horse_age = 4 then '4yo'
      else '5plus'
    end as age_band
    ,case
      when extract(month from race_date) in (3, 4, 5)  then 'spring'
      when extract(month from race_date) in (6, 7, 8)  then 'summer'
      when extract(month from race_date) in (9, 10, 11) then 'autumn'
      else 'winter'
    end as season
    ,case
      when lag(distance) over (partition by horse_id order by race_date) is null then null
      when distance > lag(distance) over (partition by horse_id order by race_date) then 'extension'
      when distance < lag(distance) over (partition by horse_id order by race_date) then 'shortening'
      else 'same'
    end as distance_change_type
    ,case
      when lag(weight_carried) over (partition by horse_id order by race_date) is null then null
      when weight_carried > lag(weight_carried) over (partition by horse_id order by race_date) then 'increase'
      when weight_carried < lag(weight_carried) over (partition by horse_id order by race_date) then 'decrease'
      else 'same'
    end as weight_carried_change_type
    ,race_class
  from temp_te_history_raw
)

/* 騎手 Target Encoding（累積3着以内率、スムージング係数m=10、同日除外）
   出走数 < 20 の騎手は全TE値をNULLとして扱う（低頻度エンティティのノイズ除去） */
,temp_jockey_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by jockey_code
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as jockey_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_course_type_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_distance_band_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_direction_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_course_type_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_course_type_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as jockey_course_type_distance_venue_te
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
/* 低頻度マスク適用: 1軸は >= 20、2軸複合 >= 5、3軸複合 >= 3
   組み合わせが細かいほど同条件への出走回数は少なくなるため軸数で閾値を段階的に緩和 */
,temp_jockey_te as (
  select
    race_id
    ,horse_number
    ,IF(jockey_count >= 20, jockey_te, NULL) as jockey_te
    ,IF(jockey_count >= 20, jockey_course_type_te, NULL) as jockey_course_type_te
    ,IF(jockey_count >= 20, jockey_venue_te, NULL) as jockey_venue_te
    ,IF(jockey_count >= 20, jockey_distance_band_te, NULL) as jockey_distance_band_te
    ,IF(jockey_count >= 20, jockey_distance_te, NULL) as jockey_distance_te
    ,IF(jockey_count >= 20, jockey_direction_te, NULL) as jockey_direction_te
    ,IF(jockey_count >= 5, jockey_course_type_venue_te, NULL) as jockey_course_type_venue_te
    ,IF(jockey_count >= 5, jockey_course_type_distance_te, NULL) as jockey_course_type_distance_te
    ,IF(jockey_count >= 3, jockey_course_type_distance_venue_te, NULL) as jockey_course_type_distance_venue_te
  from temp_jockey_te_pre
)

/* 騎手×馬コンビ Target Encoding（Issue #345）
   jockey_code × horse_id の累積3着以内率。スムージング係数m=5（コンビ実績は少ないため小さめ）。
   低頻度マスク: 3戦未満はNULL。当日レース除外: RANGE BETWEEN ... AND 1 PRECEDING */
,temp_jockey_horse_combo_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by jockey_code, horse_id
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as combo_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by jockey_code, horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 5 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by jockey_code, horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 5
    ) as jockey_horse_combo_te_raw
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
,temp_jockey_horse_combo_te as (
  select
    race_id
    ,horse_number
    ,combo_count as jockey_horse_combo_count
    ,if(combo_count >= 3, jockey_horse_combo_te_raw, null) as jockey_horse_combo_te
  from temp_jockey_horse_combo_te_pre
)

/* 乗り替わりフラグ（Issue #345）
   is_regular_jockey: 直近3走のうち2走以上で同一騎手 → 1（主戦継続）、それ以外 → 0
   jockey_change_type: 0=継続, 1=格上乗り替わり（今走jockey_te > 前走jockey_te）, -1=格下乗り替わり
   prev_jockey_te は前走時点の騎手TE（temp_jockey_te_pre から取得） */
,temp_jockey_change as (
  select
    b.race_id
    ,b.horse_number
    ,b.horse_id
    ,b.jockey_code
    ,lag(b.jockey_code, 1) over (partition by b.horse_id order by b.race_date) as prev1_jockey_code
    ,lag(b.jockey_code, 2) over (partition by b.horse_id order by b.race_date) as prev2_jockey_code
    ,lag(b.jockey_code, 3) over (partition by b.horse_id order by b.race_date) as prev3_jockey_code
    ,p.jockey_te as cur_jockey_te
    ,lag(p.jockey_te, 1) over (partition by b.horse_id order by b.race_date) as prev_jockey_te
  from temp_te_history_base as b
    inner join temp_jockey_te_pre as p
      on b.race_id = p.race_id and b.horse_number = p.horse_number
)

/* 調教師 Target Encoding（累積3着以内率、スムージング係数m=10、同日除外）
   出走数 < 20 の調教師は全TE値をNULLとして扱う（低頻度エンティティのノイズ除去） */
,temp_trainer_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by trainer_code
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as trainer_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_course_type_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_distance_band_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_direction_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_course_type_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_course_type_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by trainer_code, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by trainer_code, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as trainer_course_type_distance_venue_te
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
/* 低頻度マスク適用: 1軸は >= 20、2軸複合 >= 5、3軸複合 >= 3 */
,temp_trainer_te as (
  select
    race_id
    ,horse_number
    ,IF(trainer_count >= 20, trainer_te, NULL) as trainer_te
    ,IF(trainer_count >= 20, trainer_course_type_te, NULL) as trainer_course_type_te
    ,IF(trainer_count >= 20, trainer_venue_te, NULL) as trainer_venue_te
    ,IF(trainer_count >= 20, trainer_distance_band_te, NULL) as trainer_distance_band_te
    ,IF(trainer_count >= 20, trainer_distance_te, NULL) as trainer_distance_te
    ,IF(trainer_count >= 20, trainer_direction_te, NULL) as trainer_direction_te
    ,IF(trainer_count >= 5, trainer_course_type_venue_te, NULL) as trainer_course_type_venue_te
    ,IF(trainer_count >= 5, trainer_course_type_distance_te, NULL) as trainer_course_type_distance_te
    ,IF(trainer_count >= 3, trainer_course_type_distance_venue_te, NULL) as trainer_course_type_distance_venue_te
  from temp_trainer_te_pre
)

/* 種牡馬 Target Encoding（累積3着以内率、スムージング係数m=10、同日除外）
   出走数 < 20 の種牡馬は全TE値をNULLとして扱う（低頻度エンティティのノイズ除去） */
,temp_sire_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by sire_name
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as sire_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_course_type_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_distance_band_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_direction_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_course_type_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_course_type_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by sire_name, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by sire_name, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_course_type_distance_venue_te
    -- 出走比率（条件別出走数 / 全出走数、Issue #332）
    ,safe_divide(
      count(*) over (
        partition by sire_name, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ),
      count(*) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      )
    ) as sire_course_type_run_ratio
    ,safe_divide(
      count(*) over (
        partition by sire_name, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ),
      count(*) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      )
    ) as sire_venue_run_ratio
    ,safe_divide(
      count(*) over (
        partition by sire_name, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ),
      count(*) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      )
    ) as sire_distance_band_run_ratio
    ,safe_divide(
      count(*) over (
        partition by sire_name, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ),
      count(*) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      )
    ) as sire_distance_run_ratio
    -- 年齢帯別出走数カウント（低頻度マスク用）
    ,coalesce(count(case when age_band = '2yo' then 1 end) over (
      partition by sire_name
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as sire_age2_count
    ,coalesce(count(case when age_band = '3yo' then 1 end) over (
      partition by sire_name
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as sire_age3_count
    ,coalesce(count(case when age_band = '4yo' then 1 end) over (
      partition by sire_name
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as sire_age4_count
    ,coalesce(count(case when age_band = '5plus' then 1 end) over (
      partition by sire_name
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as sire_age5plus_count
    -- 年齢帯別TE（CASE WHEN 条件付き SUM/COUNT）
    ,safe_divide(
      coalesce(sum(case when age_band = '2yo' then is_top3 else null end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '2yo' then 1 end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_age2_te
    ,safe_divide(
      coalesce(sum(case when age_band = '3yo' then is_top3 else null end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '3yo' then 1 end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_age3_te
    ,safe_divide(
      coalesce(sum(case when age_band = '4yo' then is_top3 else null end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '4yo' then 1 end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_age4_te
    ,safe_divide(
      coalesce(sum(case when age_band = '5plus' then is_top3 else null end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '5plus' then 1 end) over (
        partition by sire_name
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as sire_age5plus_te
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
/* 低頻度マスク適用: 1軸は >= 20、2軸複合 >= 5、3軸複合 >= 3、年齢帯は >= 5 */
,temp_sire_te as (
  select
    race_id
    ,horse_number
    ,IF(sire_count >= 20, sire_te, NULL) as sire_te
    ,IF(sire_count >= 20, sire_course_type_te, NULL) as sire_course_type_te
    ,IF(sire_count >= 20, sire_venue_te, NULL) as sire_venue_te
    ,IF(sire_count >= 20, sire_distance_band_te, NULL) as sire_distance_band_te
    ,IF(sire_count >= 20, sire_distance_te, NULL) as sire_distance_te
    ,IF(sire_count >= 20, sire_direction_te, NULL) as sire_direction_te
    ,IF(sire_count >= 5, sire_course_type_venue_te, NULL) as sire_course_type_venue_te
    ,IF(sire_count >= 5, sire_course_type_distance_te, NULL) as sire_course_type_distance_te
    ,IF(sire_count >= 3, sire_course_type_distance_venue_te, NULL) as sire_course_type_distance_venue_te
    -- 年齢帯別TE（各年齢帯の産駒数 >= 5 でマスク）
    ,IF(sire_age2_count >= 5, sire_age2_te, NULL) as sire_age2_te
    ,IF(sire_age3_count >= 5, sire_age3_te, NULL) as sire_age3_te
    ,IF(sire_age4_count >= 5, sire_age4_te, NULL) as sire_age4_te
    ,IF(sire_age5plus_count >= 5, sire_age5plus_te, NULL) as sire_age5plus_te
    -- 出走比率（低頻度マスク適用: sire_count >= 20 と同一条件）
    ,IF(sire_count >= 20, sire_course_type_run_ratio, NULL) as sire_course_type_run_ratio
    ,IF(sire_count >= 20, sire_venue_run_ratio, NULL) as sire_venue_run_ratio
    ,IF(sire_count >= 20, sire_distance_band_run_ratio, NULL) as sire_distance_band_run_ratio
    ,IF(sire_count >= 20, sire_distance_run_ratio, NULL) as sire_distance_run_ratio
  from temp_sire_te_pre
)

/* 母馬（繁殖牝馬）競走実績特徴量（Issue #307 / Issue #325修正）
   pedigree.dam_id 経由から horse_results.horse_name = horse_master.dam_name への直接JOINに変更。
   horse_master 未収録の古い繁殖牝馬（2005〜2015年頃現役）も horse_results 経由で実績取得できる。
   同名馬が複数存在する場合は出走数最多の馬を母馬として選択する。 */
,temp_mare_race_base as (
  select
    h_m.horse_id
    ,ri_d.course_type
    ,ri_d.venue_code
    ,case
      when ri_d.distance < 1400 then 'sprint'
      when ri_d.distance < 1800 then 'mile'
      when ri_d.distance < 2200 then 'intermediate'
      else 'long'
    end as distance_band
    ,ri_d.direction
    ,ri_d.distance
    ,case when rr_d.finish_position between 1 and 3 then 1 else 0 end as is_top3
    ,date_diff(ri_d.race_date, hm_d.birth_date, year) as horse_age_at_race
  from `{project_id}`.raw.horse_master as h_m
  join (
    /* 同名馬が複数存在する場合は出走数最多の馬を母馬として採用 */
    select
      horse_name
      ,horse_id
      ,row_number() over (
        partition by horse_name
        order by race_count desc, horse_id
      ) as rn
    from (
      select horse_name, horse_id, count(*) as race_count
      from `{project_id}`.raw.horse_results
      where horse_name is not null
      group by horse_name, horse_id
    )
  ) as dam_id_lookup
    on dam_id_lookup.horse_name = h_m.dam_name
    and dam_id_lookup.rn = 1
  join `{project_id}`.raw.horse_results as hr_d
    on hr_d.horse_id = dam_id_lookup.horse_id
  join `{project_id}`.raw.race_results as rr_d
    on rr_d.race_id = hr_d.race_id
    and rr_d.horse_number = hr_d.horse_number
  join `{project_id}`.raw.race_info as ri_d
    on ri_d.race_id = hr_d.race_id
  left join (
    -- horse_master に同一 horse_id が複数行存在する場合、1行に絞る
    select horse_id, birth_date
    from `{project_id}`.raw.horse_master
    qualify row_number() over (partition by horse_id order by horse_id) = 1
  ) as hm_d on hm_d.horse_id = dam_id_lookup.horse_id
  where h_m.dam_name is not null
    and rr_d.finish_position > 0
    and ri_d.course_type != 'obstacle'
)
/* グループA-1: 全出走ベース距離統計 / グループA-2: 3着以内レース絞り距離統計 / グループA-3: 全体複勝率 */
,temp_mare_stats as (
  select
    horse_id
    ,count(*) as mare_race_count
    ,avg(distance) as mare_avg_race_distance
    ,max(distance) as mare_max_race_distance
    ,min(distance) as mare_min_race_distance
    ,countif(is_top3 = 1) as mare_placed_race_count
    ,avg(case when is_top3 = 1 then distance end) as mare_placed_avg_distance
    ,max(case when is_top3 = 1 then distance end) as mare_placed_max_distance
    ,min(case when is_top3 = 1 then distance end) as mare_placed_min_distance
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_place_rate
    ,safe_divide(countif(is_top3 = 1 and course_type = 'turf'), nullif(countif(course_type = 'turf'), 0)) as mare_turf_place_rate
    ,safe_divide(countif(is_top3 = 1 and course_type = 'dirt'), nullif(countif(course_type = 'dirt'), 0)) as mare_dirt_place_rate
    -- 母馬自身の早熟・晩成性（カテゴリC）
    ,safe_divide(
      countif(is_top3 = 1 and horse_age_at_race between 2 and 3),
      nullif(countif(horse_age_at_race between 2 and 3), 0)
    ) as mare_early_career_place_rate
    ,safe_divide(
      countif(is_top3 = 1 and horse_age_at_race >= 4),
      nullif(countif(horse_age_at_race >= 4), 0)
    ) as mare_late_career_place_rate
  from temp_mare_race_base
  group by horse_id
)
/* グループA-3: 競馬場別複勝率 */
,temp_mare_venue_stats as (
  select
    horse_id
    ,venue_code
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_venue_place_rate
  from temp_mare_race_base
  group by horse_id, venue_code
)
/* グループA-3: 距離帯別複勝率 */
,temp_mare_distance_band_stats as (
  select
    horse_id
    ,distance_band
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_distance_band_place_rate
  from temp_mare_race_base
  group by horse_id, distance_band
)
/* グループA-3: 距離別複勝率 */
,temp_mare_distance_stats as (
  select
    horse_id
    ,distance
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_distance_place_rate
  from temp_mare_race_base
  group by horse_id, distance
)
/* グループA-3: 回り方向別複勝率 */
,temp_mare_direction_stats as (
  select
    horse_id
    ,direction
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_direction_place_rate
  from temp_mare_race_base
  group by horse_id, direction
)
/* グループA-3: コース種別×競馬場別複勝率 */
,temp_mare_cv_stats as (
  select
    horse_id
    ,course_type
    ,venue_code
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_course_type_venue_place_rate
  from temp_mare_race_base
  group by horse_id, course_type, venue_code
)
/* グループA-3: コース種別×距離帯別複勝率 */
,temp_mare_cd_stats as (
  select
    horse_id
    ,course_type
    ,distance_band
    ,safe_divide(countif(is_top3 = 1), count(*)) as mare_course_type_distance_band_place_rate
  from temp_mare_race_base
  group by horse_id, course_type, distance_band
)

/* カテゴリB: 母馬産駒 Target Encoding（dam_name軸、スムージング係数m=10、同日除外）
   産駒数 < 10 の母馬は全TE値をNULLとして扱う（#293 種牡馬TE準拠） */
,temp_mare_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by dam_name
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as mare_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, course_type
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, course_type
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_course_type_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, distance_band
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, distance_band
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_distance_band_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, distance
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, distance
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, direction
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, direction
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_direction_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, course_type, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, course_type, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_course_type_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, course_type, distance
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, course_type, distance
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_course_type_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by dam_name, course_type, distance, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by dam_name, course_type, distance, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_course_type_distance_venue_te
    -- 出走比率（条件別出走数 / 全出走数、Issue #332）
    ,safe_divide(
      count(*) over (
        partition by dam_name, course_type
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ),
      count(*) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      )
    ) as mare_course_type_run_ratio
    ,safe_divide(
      count(*) over (
        partition by dam_name, venue_code
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ),
      count(*) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      )
    ) as mare_venue_run_ratio
    ,safe_divide(
      count(*) over (
        partition by dam_name, distance_band
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ),
      count(*) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      )
    ) as mare_distance_band_run_ratio
    ,safe_divide(
      count(*) over (
        partition by dam_name, distance
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ),
      count(*) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      )
    ) as mare_distance_run_ratio
    -- 年齢帯別出走数カウント（低頻度マスク用）
    ,coalesce(count(case when age_band = '2yo' then 1 end) over (
      partition by dam_name
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as mare_age2_count
    ,coalesce(count(case when age_band = '3yo' then 1 end) over (
      partition by dam_name
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as mare_age3_count
    ,coalesce(count(case when age_band = '4yo' then 1 end) over (
      partition by dam_name
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as mare_age4_count
    ,coalesce(count(case when age_band = '5plus' then 1 end) over (
      partition by dam_name
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as mare_age5plus_count
    -- 年齢帯別TE（CASE WHEN 条件付き SUM/COUNT）
    ,safe_divide(
      coalesce(sum(case when age_band = '2yo' then is_top3 else null end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '2yo' then 1 end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_age2_te
    ,safe_divide(
      coalesce(sum(case when age_band = '3yo' then is_top3 else null end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '3yo' then 1 end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_age3_te
    ,safe_divide(
      coalesce(sum(case when age_band = '4yo' then is_top3 else null end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '4yo' then 1 end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_age4_te
    ,safe_divide(
      coalesce(sum(case when age_band = '5plus' then is_top3 else null end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(case when age_band = '5plus' then 1 end) over (
        partition by dam_name
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ), 0) + 10
    ) as mare_age5plus_te
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
/* 低頻度マスク適用: 母馬の産駒数 < 3 は全TE値NULL、年齢帯別は >= 5 */
,temp_mare_te as (
  select
    race_id
    ,horse_number
    ,IF(mare_count >= 3, mare_te, NULL) as mare_te
    ,IF(mare_count >= 3, mare_course_type_te, NULL) as mare_course_type_te
    ,IF(mare_count >= 3, mare_venue_te, NULL) as mare_venue_te
    ,IF(mare_count >= 3, mare_distance_band_te, NULL) as mare_distance_band_te
    ,IF(mare_count >= 3, mare_distance_te, NULL) as mare_distance_te
    ,IF(mare_count >= 3, mare_direction_te, NULL) as mare_direction_te
    ,IF(mare_count >= 3, mare_course_type_venue_te, NULL) as mare_course_type_venue_te
    ,IF(mare_count >= 3, mare_course_type_distance_te, NULL) as mare_course_type_distance_te
    ,IF(mare_count >= 3, mare_course_type_distance_venue_te, NULL) as mare_course_type_distance_venue_te
    -- 年齢帯別TE（各年齢帯の産駒数 >= 5 でマスク）
    ,IF(mare_age2_count >= 5, mare_age2_te, NULL) as mare_age2_te
    ,IF(mare_age3_count >= 5, mare_age3_te, NULL) as mare_age3_te
    ,IF(mare_age4_count >= 5, mare_age4_te, NULL) as mare_age4_te
    ,IF(mare_age5plus_count >= 5, mare_age5plus_te, NULL) as mare_age5plus_te
    -- 出走比率（低頻度マスク適用: mare_count >= 3 と同一条件）
    ,IF(mare_count >= 3, mare_course_type_run_ratio, NULL) as mare_course_type_run_ratio
    ,IF(mare_count >= 3, mare_venue_run_ratio, NULL) as mare_venue_run_ratio
    ,IF(mare_count >= 3, mare_distance_band_run_ratio, NULL) as mare_distance_band_run_ratio
    ,IF(mare_count >= 3, mare_distance_run_ratio, NULL) as mare_distance_run_ratio
  from temp_mare_te_pre
)

/* 馬自身 Target Encoding（累積3着以内率、スムージング係数m=10、同日除外）
   出走数 < 5 の馬は全TE値をNULLとして扱う（若馬・低頻度の情報ノイズ除去） */
,temp_horse_te_pre as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    ,coalesce(count(*) over (
      partition by horse_id
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as horse_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, course_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_course_type_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_distance_band_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, direction
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_direction_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, jockey_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, jockey_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_jockey_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, season
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, season
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_season_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, course_type, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_course_type_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, course_type, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_course_type_distance_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, course_type, distance, venue_code
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_course_type_distance_venue_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance_change_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance_change_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_distance_change_te
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, weight_carried_change_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, weight_carried_change_type
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as horse_weight_carried_change_te
  from temp_te_history_base
    cross join temp_global_mean_te as g
)
/* 低頻度マスク適用: 馬の過去出走数 < 5 の場合は全TE値をNULLにする（若馬・低頻度の情報ノイズ除去） */
,temp_horse_te as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    ,IF(horse_count >= 5, horse_te, NULL) as horse_te
    ,IF(horse_count >= 5, horse_course_type_te, NULL) as horse_course_type_te
    ,IF(horse_count >= 5, horse_venue_te, NULL) as horse_venue_te
    ,IF(horse_count >= 5, horse_distance_band_te, NULL) as horse_distance_band_te
    ,IF(horse_count >= 5, horse_distance_te, NULL) as horse_distance_te
    ,IF(horse_count >= 5, horse_direction_te, NULL) as horse_direction_te
    ,IF(horse_count >= 5, horse_jockey_te, NULL) as horse_jockey_te
    ,IF(horse_count >= 5, horse_season_te, NULL) as horse_season_te
    ,IF(horse_count >= 2, horse_course_type_venue_te, NULL) as horse_course_type_venue_te
    ,IF(horse_count >= 2, horse_course_type_distance_te, NULL) as horse_course_type_distance_te
    ,IF(horse_count >= 2, horse_course_type_distance_venue_te, NULL) as horse_course_type_distance_venue_te
    ,IF(horse_count >= 5, horse_distance_change_te, NULL) as horse_distance_change_te
    ,IF(horse_count >= 5, horse_weight_carried_change_te, NULL) as horse_weight_carried_change_te
  from temp_horse_te_pre
)

/* グレード別 Target Encoding（Issue #347）
   馬ごとのG1/G2/G3グレード別 複勝率TE（スムージングm=10）、
   格上挑戦フラグ、G1出走経験フラグ、過去最高グレードを計算する。
   同日レース除外: RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING */
,temp_grade_te_pre as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    /* G1グレード別出走数（5年以内、当日行除外） */
    ,coalesce(countif(race_class = 'G1') over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g1_count
    /* G2グレード別出走数 */
    ,coalesce(countif(race_class = 'G2') over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g2_count
    /* G3グレード別出走数 */
    ,coalesce(countif(race_class = 'G3') over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g3_count
    /* G1グレード別 複勝数（スムージング分子） */
    ,coalesce(sum(case when race_class = 'G1' then is_top3 else 0 end) over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g1_top3_sum
    /* G2グレード別 複勝数 */
    ,coalesce(sum(case when race_class = 'G2' then is_top3 else 0 end) over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g2_top3_sum
    /* G3グレード別 複勝数 */
    ,coalesce(sum(case when race_class = 'G3' then is_top3 else 0 end) over (
      partition by horse_id
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ), 0) as g3_top3_sum
    /* 過去最高グレード（G1 > G2 > G3 > OP > その他）*/
    ,case
      when countif(race_class = 'G1') over (
        partition by horse_id
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ) > 0 then 'G1'
      when countif(race_class = 'G2') over (
        partition by horse_id
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ) > 0 then 'G2'
      when countif(race_class = 'G3') over (
        partition by horse_id
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ) > 0 then 'G3'
      when countif(race_class in ('OP', 'L')) over (
        partition by horse_id
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ) > 0 then 'OP'
      when count(*) over (
        partition by horse_id
        order by unix_date(race_date)
        range between unbounded preceding and 1 preceding
      ) > 0 then 'below_op'
      else null
    end as best_grade_achieved
    ,race_class as current_race_class
  from temp_te_history_base
)
,temp_grade_te as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    /* G1 TE: 出走数>=3 でマスク（スムージングm=10） */
    ,IF(g1_count >= 3,
      safe_divide(g1_top3_sum + 10 * g.global_top3_rate, g1_count + 10),
      NULL
    ) as horse_g1_te
    /* G2 TE: 出走数>=3 でマスク */
    ,IF(g2_count >= 3,
      safe_divide(g2_top3_sum + 10 * g.global_top3_rate, g2_count + 10),
      NULL
    ) as horse_g2_te
    /* G3 TE: 出走数>=3 でマスク */
    ,IF(g3_count >= 3,
      safe_divide(g3_top3_sum + 10 * g.global_top3_rate, g3_count + 10),
      NULL
    ) as horse_g3_te
    /* 格上挑戦フラグ: 今回のグレードが過去最高より上、または今回G1/G2/G3で経験なし */
    ,case
      when current_race_class = 'G1' and (best_grade_achieved is null or best_grade_achieved != 'G1') then 1
      when current_race_class = 'G2' and (best_grade_achieved is null or best_grade_achieved not in ('G1', 'G2')) then 1
      when current_race_class = 'G3' and (best_grade_achieved is null or best_grade_achieved not in ('G1', 'G2', 'G3')) then 1
      when current_race_class not in ('G1', 'G2', 'G3') then 0
      else 0
    end as grade_step_up_flag
    /* G1出走経験フラグ */
    ,IF(g1_count > 0, 1, 0) as g1_experience_flag
    /* 過去最高グレード */
    ,best_grade_achieved
  from temp_grade_te_pre
  cross join temp_global_mean_te as g
)

/* 馬TE_diff 集計用Stage1: 各レースのdiff値とレース内RANKを計算（時系列集計の入力）
   horse_te が NULL（出走5回未満）の場合、全diff・rankはNULL NULLS LASTにより末尾ランク化 */
,temp_horse_te_diff_pre as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,race_date
    ,horse_course_type_te - horse_te as h_course_type_diff
    ,horse_venue_te - horse_te as h_venue_diff
    ,horse_distance_band_te - horse_te as h_distance_band_diff
    ,horse_distance_te - horse_te as h_distance_diff
    ,horse_direction_te - horse_te as h_direction_diff
    ,horse_jockey_te - horse_te as h_jockey_diff
    ,horse_season_te - horse_te as h_season_diff
    ,horse_course_type_venue_te - horse_te as h_cv_diff
    ,horse_course_type_distance_te - horse_te as h_cd_diff
    ,horse_course_type_distance_venue_te - horse_te as h_cdv_diff
    ,horse_distance_change_te - horse_te as h_dc_diff
    ,horse_weight_carried_change_te - horse_te as h_wcc_diff
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_course_type_te - horse_te) DESC NULLS LAST) as h_course_type_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_venue_te - horse_te) DESC NULLS LAST) as h_venue_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_distance_band_te - horse_te) DESC NULLS LAST) as h_distance_band_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_distance_te - horse_te) DESC NULLS LAST) as h_distance_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_direction_te - horse_te) DESC NULLS LAST) as h_direction_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_jockey_te - horse_te) DESC NULLS LAST) as h_jockey_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_season_te - horse_te) DESC NULLS LAST) as h_season_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_course_type_venue_te - horse_te) DESC NULLS LAST) as h_cv_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_course_type_distance_te - horse_te) DESC NULLS LAST) as h_cd_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_course_type_distance_venue_te - horse_te) DESC NULLS LAST) as h_cdv_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_distance_change_te - horse_te) DESC NULLS LAST) as h_dc_diff_rank
    ,RANK() OVER (PARTITION BY race_id ORDER BY (horse_weight_carried_change_te - horse_te) DESC NULLS LAST) as h_wcc_diff_rank
  from temp_horse_te
)
/* 馬TE_diff 集計Stage2: horse_idごとに時系列でdiff平均・ランク平均を計算（当日行除外）
   diff がNULLの行（出走5回未満）はAVG計算で自動除外。
   ランク平均: diff がNULLの場合はIF()でNULLとして集計から除外（末尾ランク混入を防止） */
,temp_horse_te_diff_summary as (
  select
    race_id
    ,horse_number
    ,avg(h_course_type_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_course_type_te_diff_avg
    ,avg(h_venue_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_venue_te_diff_avg
    ,avg(h_distance_band_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_distance_band_te_diff_avg
    ,avg(h_distance_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_distance_te_diff_avg
    ,avg(h_direction_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_direction_te_diff_avg
    ,avg(h_jockey_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_jockey_te_diff_avg
    ,avg(h_season_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_season_te_diff_avg
    ,avg(h_cv_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cv_te_diff_avg
    ,avg(h_cd_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cd_te_diff_avg
    ,avg(h_cdv_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cdv_te_diff_avg
    ,avg(h_dc_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_dc_te_diff_avg
    ,avg(h_wcc_diff) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_wcc_te_diff_avg
    ,avg(IF(h_course_type_diff IS NOT NULL, h_course_type_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_course_type_te_diff_rank_avg
    ,avg(IF(h_venue_diff IS NOT NULL, h_venue_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_venue_te_diff_rank_avg
    ,avg(IF(h_distance_band_diff IS NOT NULL, h_distance_band_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_distance_band_te_diff_rank_avg
    ,avg(IF(h_distance_diff IS NOT NULL, h_distance_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_distance_te_diff_rank_avg
    ,avg(IF(h_direction_diff IS NOT NULL, h_direction_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_direction_te_diff_rank_avg
    ,avg(IF(h_jockey_diff IS NOT NULL, h_jockey_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_jockey_te_diff_rank_avg
    ,avg(IF(h_season_diff IS NOT NULL, h_season_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_season_te_diff_rank_avg
    ,avg(IF(h_cv_diff IS NOT NULL, h_cv_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cv_te_diff_rank_avg
    ,avg(IF(h_cd_diff IS NOT NULL, h_cd_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cd_te_diff_rank_avg
    ,avg(IF(h_cdv_diff IS NOT NULL, h_cdv_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_cdv_te_diff_rank_avg
    ,avg(IF(h_dc_diff IS NOT NULL, h_dc_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_dc_te_diff_rank_avg
    ,avg(IF(h_wcc_diff IS NOT NULL, h_wcc_diff_rank, NULL)) over (partition by horse_id order by unix_date(race_date) rows between unbounded preceding and 1 preceding) as horse_wcc_te_diff_rank_avg
  from temp_horse_te_diff_pre
)

/* 馬の距離帯別・距離別 TE 計算の元データ
   horse_results を起点にすることで、race_results にまだ存在しない当日予測レースも含める。 */
,temp_horse_distance_base as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,h_r.horse_id
    ,r_i.race_date
    ,r_i.distance
    ,case
      when r_i.distance < 1400 then 'sprint'
      when r_i.distance < 1800 then 'mile'
      when r_i.distance < 2200 then 'intermediate'
      else 'long'
    end as distance_band
    ,case when r_r.finish_position between 1 and 3 then 1 else 0 end as is_top3
    ,case when r_r.finish_position = 1 then 1 else 0 end as is_top1
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i
      on h_r.race_id = r_i.race_id
    left join `{project_id}`.raw.race_results as r_r
      on h_r.race_id = r_r.race_id
      and h_r.horse_number = r_r.horse_number
  where r_r.finish_position > 0 or r_r.race_id is null
)

/* 馬の距離帯別 TE（累積3着以内率・1着率、スムージング係数m=10、同日除外） */
,temp_horse_distance_band_te as (
  select
    race_id
    ,horse_number
    -- 距離帯別3着以内率
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_band_top3_finish_rate
    -- 距離帯別1着率
    ,safe_divide(
      coalesce(sum(is_top1) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_band_top1_finish_rate
    -- 距離帯成績 − 全体成績の差分
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) - safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_band_rate_diff
    -- その距離帯での初出走フラグ
    ,case
      when coalesce(count(*) over (
        partition by horse_id, distance_band
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) = 0 then 1
      else 0
    end as new_distance_band_flag
  from temp_horse_distance_base
    cross join temp_global_mean_te as g
)

/* 馬の距離別 TE（累積3着以内率・1着率、スムージング係数m=10、同日除外） */
,temp_horse_distance_te as (
  select
    race_id
    ,horse_number
    -- 距離別3着以内率
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_top3_finish_rate
    -- 距離別1着率
    ,safe_divide(
      coalesce(sum(is_top1) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_top1_finish_rate
    -- 距離別成績 − 全体成績の差分
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) - safe_divide(
      coalesce(sum(is_top3) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by horse_id
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as distance_rate_diff
    -- その距離での初出走フラグ
    ,case
      when coalesce(count(*) over (
        partition by horse_id, distance
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) = 0 then 1
      else 0
    end as new_distance_flag
  from temp_horse_distance_base
    cross join temp_global_mean_te as g
)

/* キャリア最長・最短距離フラグ特徴量（Issue #305）
   COUNT DISTINCT をウィンドウ関数化するため、まず (horse_id, distance) の初出フラグを付与 */
,temp_career_distance_flags as (
  select
    *
    ,case
      when row_number() over (
        partition by horse_id, distance
        order by unix_date(race_date), race_id
      ) = 1 then 1 else 0
    end as is_first_at_distance
    ,case
      when is_top3 = 1 and row_number() over (
        partition by horse_id, distance, is_top3
        order by unix_date(race_date), race_id
      ) = 1 then 1 else 0
    end as is_first_placed_at_distance
  from temp_horse_distance_base
)

/* キャリア距離集計（当レース除外: RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING） */
,temp_career_distance as (
  select
    race_id
    ,horse_number
    ,horse_id
    ,max(distance) over w as career_max_distance_so_far
    ,min(distance) over w as career_min_distance_so_far
    ,sum(is_first_at_distance) over w as career_distance_count
    ,max(case when is_top3 = 1 then distance end) over w as placed_max_distance_so_far
    ,min(case when is_top3 = 1 then distance end) over w as placed_min_distance_so_far
    ,sum(is_first_placed_at_distance) over w as placed_distance_count
  from temp_career_distance_flags
  window w as (
    partition by horse_id
    order by unix_date(race_date)
    range between unbounded preceding and 1 preceding
  )
)

/* 開催条件別ペース傾向スコア（venue_code × distance × course_type の過去平均脚質スコア、Issue #349）
   値が小さい（≒1〜2）→ 先行馬が多いコース、値が大きい（≒3〜4）→ 差し追込馬が多いコース
   ウィンドウ: UNBOUNDED PRECEDING AND 1 PRECEDING（当日レース除外）*/
,temp_course_pace_stats as (
  select
    race_id
    ,horse_number
    ,avg(avg_gate_style_score) over (
      partition by venue_code, distance, course_type
      order by unix_date(race_date)
      range between unbounded preceding and 1 preceding
    ) as course_pace_score
  from temp_past_race_features2
)

/* 開催条件別・脚質グループ別 TE 計算ベース: avg_gate_style_score と is_top3 を結合（Issue #349）
   gate_style_group = round(avg_gate_style_score): 1=front, 2=mid_front, 3=mid, 4=back */
,temp_gate_style_te_base as (
  select
    p.race_id
    ,p.horse_number
    ,p.race_date
    ,p.venue_code
    ,p.distance
    ,p.course_type
    ,h.is_top3
    ,cast(round(p.avg_gate_style_score) as int64) as gate_style_group
  from temp_past_race_features2 as p
    inner join temp_te_history_raw as h
      on p.race_id = h.race_id and p.horse_number = h.horse_number
  where p.avg_gate_style_score is not null
)

/* 開催条件別・脚質グループ別 TE 生値（スムージング係数m=10、直近5年、Issue #349） */
,temp_gate_style_te_pre as (
  select
    race_id
    ,horse_number
    ,coalesce(count(*) over (
      partition by venue_code, distance, course_type, gate_style_group
      order by unix_date(race_date)
      range between 1826 preceding and 1 preceding
    ), 0) as gs_course_count
    ,safe_divide(
      coalesce(sum(is_top3) over (
        partition by venue_code, distance, course_type, gate_style_group
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10 * g.global_top3_rate,
      coalesce(count(*) over (
        partition by venue_code, distance, course_type, gate_style_group
        order by unix_date(race_date)
        range between 1826 preceding and 1 preceding
      ), 0) + 10
    ) as gate_style_course_te
  from temp_gate_style_te_base
    cross join temp_global_mean_te as g
)

/* 開催条件別・脚質グループ別 TE（出走数 < 10 は NULL マスク、Issue #349）
   「差し馬がこのコース・距離で過去どれくらい複勝圏に入っているか」を表す */
,temp_gate_style_te as (
  select
    race_id
    ,horse_number
    ,IF(gs_course_count >= 10, gate_style_course_te, NULL) as gate_style_course_te
  from temp_gate_style_te_pre
)

/* 調教本追切データ (raw.cha_data から) */
,temp_training as (
  select
    race_id
    ,horse_number
    ,training_count
    ,training_furlongs
    ,last_3f_time as training_last_3f
    ,training_index as cha_training_index
    ,intensity_code as training_intensity
    -- 調教コースを坂路(1)/ウッド(2)/ダート(3)/芝(4)/その他(0) に分類
    ,case
      when training_course_code in ('01', '11') then 1
      when training_course_code in ('02', '12', '25') then 2
      when training_course_code in ('03', '13') then 3
      when training_course_code in ('04', '16') then 4
      else 0
    end as training_course_type
  from `{project_id}`.raw.cha_data
  where training_index is not null and training_index > 0
)

,temp_final_raw as (
select
  t_p_r_f.* except(
    -- gain=0 特徴量（Issue #296）
    running_style
    ,improvement
    ,stable_index
    ,blinker
    ,pace_forecast
    ,early_advantage
    ,behind_advantage
    ,small_number_early_advantage
    ,bracket_number
    ,condition_change_flag
    ,improvement_code_2
    ,improvement_code_3
    ,improvement_code_4
    ,improvement_code_5
    ,corner_position_1
    ,corner_position_2
    ,corner_position_3
    ,corner_position_4
    ,corner_position_5
    ,disadvantage_3
    ,disadvantage_5
    ,position_fault_2
    ,position_fault_3
    ,late_start_3
    ,late_start_5
    ,mean_corner_position
    ,ema_corner_position
    ,running_style_front_count
    ,is_sole_leader
    ,is_renso
    -- 過去2〜5走の個別カラム（Issue #292: _2~5系を直近1走+統計量に集約）
    -- idm_3 は idm_trend_3, finish_position_3 は finish_position_trend_3 の中間値として保持
    ,race_name_2
    ,race_name_3
    ,race_name_4
    ,race_name_5
    ,idm_2
    ,idm_3
    ,idm_4
    ,idm_5
    ,finish_position_2
    ,finish_position_3
    ,finish_position_4
    ,finish_position_5
    ,finish_position_rate_2
    ,finish_position_rate_3
    ,finish_position_rate_4
    ,finish_position_rate_5
    ,win_odds_2
    ,win_odds_3
    ,win_odds_4
    ,win_odds_5
    ,win_popularity_2
    ,win_popularity_3
    ,win_popularity_4
    ,win_popularity_5
    ,popularity_rate_2
    ,popularity_rate_3
    ,popularity_rate_4
    ,popularity_rate_5
    ,upside_rate_2
    ,upside_rate_3
    ,upside_rate_4
    ,upside_rate_5
    ,finish_time_2
    ,finish_time_3
    ,finish_time_4
    ,finish_time_5
    ,last_3f_2
    ,last_3f_3
    ,last_3f_4
    ,last_3f_5
    ,last_3f_rank_in_race_2
    ,last_3f_rank_in_race_3
    ,last_3f_rank_in_race_4
    ,last_3f_rank_in_race_5
    ,late_start_2
    ,late_start_4
    ,position_fault_4
    ,position_fault_5
    ,disadvantage_2
    ,disadvantage_4
  )
  ,t_h_m_f.* except(
    race_id
    ,race_date
    ,venue_code
    ,venue_name
    ,race_number
    ,race_name
    ,course_type
    ,distance
    ,direction
    ,age_condition
    ,race_class
    ,num_horses
    ,horse_number
    -- gain=0 特徴量（Issue #296）
    ,turf_condition_code
    ,turf_condition_inner
    ,straight_bias_outer
    ,straight_bias_outermost
    ,dirt_condition_code
    ,new_direction_flag
    ,new_surface_dist_flag
    ,new_track_dist_flag
  )
  -- 全ての差分をたして、激走フラグを作成する
  ,(
    coalesce(surface_top3_finish_rate_diff, 0) +
    coalesce(surface_top2_finish_rate_diff, 0) +
    coalesce(surface_top1_finish_rate_diff, 0) +
    coalesce(surface_dist_top3_finish_rate_diff, 0) +
    coalesce(surface_dist_top2_finish_rate_diff, 0) +
    coalesce(surface_dist_top1_finish_rate_diff, 0) +
    coalesce(track_dist_top3_finish_rate_diff, 0) +
    coalesce(track_dist_top2_finish_rate_diff, 0) +
    coalesce(track_dist_top1_finish_rate_diff, 0) +
    coalesce(rotation_top3_finish_rate_diff, 0) +
    coalesce(rotation_top2_finish_rate_diff, 0) +
    coalesce(rotation_top1_finish_rate_diff, 0) +
    coalesce(direction_top3_finish_rate_diff, 0) +
    coalesce(direction_top2_finish_rate_diff, 0) +
    coalesce(direction_top1_finish_rate_diff, 0) +
    coalesce(condition_top3_finish_rate_diff, 0) +
    coalesce(condition_top2_finish_rate_diff, 0) +
    coalesce(condition_top1_finish_rate_diff, 0) +
    coalesce(pace_top3_finish_rate_diff, 0) +
    coalesce(pace_top2_finish_rate_diff, 0) +
    coalesce(pace_top1_finish_rate_diff, 0) +
    coalesce(season_top3_finish_rate_diff, 0) +
    coalesce(season_top2_finish_rate_diff, 0) +
    coalesce(season_top1_finish_rate_diff, 0) +
    coalesce(bracket_top3_finish_rate_diff, 0) +
    coalesce(bracket_top2_finish_rate_diff, 0) +
    coalesce(bracket_top1_finish_rate_diff, 0)
  ) as total_diff_sum
  -- 騎手TE
  ,t_j_te.jockey_te
  ,t_j_te.jockey_course_type_te
  ,t_j_te.jockey_venue_te
  ,t_j_te.jockey_distance_band_te
  ,t_j_te.jockey_distance_te
  ,t_j_te.jockey_direction_te
  ,t_j_te.jockey_course_type_venue_te
  ,t_j_te.jockey_course_type_distance_te
  ,t_j_te.jockey_course_type_distance_venue_te
  -- 騎手TE差分（条件別TE − 全体TE）
  ,t_j_te.jockey_course_type_te - t_j_te.jockey_te as jockey_course_type_te_diff
  ,t_j_te.jockey_venue_te - t_j_te.jockey_te as jockey_venue_te_diff
  ,t_j_te.jockey_distance_band_te - t_j_te.jockey_te as jockey_distance_band_te_diff
  ,t_j_te.jockey_distance_te - t_j_te.jockey_te as jockey_distance_te_diff
  ,t_j_te.jockey_direction_te - t_j_te.jockey_te as jockey_direction_te_diff
  ,t_j_te.jockey_course_type_venue_te - t_j_te.jockey_te as jockey_course_type_venue_te_diff
  ,t_j_te.jockey_course_type_distance_te - t_j_te.jockey_te as jockey_course_type_distance_te_diff
  ,t_j_te.jockey_course_type_distance_venue_te - t_j_te.jockey_te as jockey_course_type_distance_venue_te_diff
  -- 騎手×馬コンビTE / 乗り替わりフラグ（Issue #345）
  ,t_jh_te.jockey_horse_combo_te
  ,t_jh_te.jockey_horse_combo_count
  ,case
    when t_j_c.prev1_jockey_code is null then null
    when (case when t_j_c.jockey_code = t_j_c.prev1_jockey_code then 1 else 0 end
         + case when t_j_c.jockey_code = t_j_c.prev2_jockey_code then 1 else 0 end
         + case when t_j_c.jockey_code = t_j_c.prev3_jockey_code then 1 else 0 end) >= 2
      then 1
    else 0
  end as is_regular_jockey
  ,case
    when t_j_c.prev1_jockey_code is null then null
    when t_j_c.jockey_code = t_j_c.prev1_jockey_code then 0
    when t_j_c.cur_jockey_te > t_j_c.prev_jockey_te then 1
    when t_j_c.cur_jockey_te < t_j_c.prev_jockey_te then -1
    else 0
  end as jockey_change_type
  -- 調教師TE
  ,t_tr_te.trainer_te
  ,t_tr_te.trainer_course_type_te
  ,t_tr_te.trainer_venue_te
  ,t_tr_te.trainer_distance_band_te
  ,t_tr_te.trainer_distance_te
  ,t_tr_te.trainer_direction_te
  ,t_tr_te.trainer_course_type_venue_te
  ,t_tr_te.trainer_course_type_distance_te
  ,t_tr_te.trainer_course_type_distance_venue_te
  -- 調教師TE差分（条件別TE − 全体TE）
  ,t_tr_te.trainer_course_type_te - t_tr_te.trainer_te as trainer_course_type_te_diff
  ,t_tr_te.trainer_venue_te - t_tr_te.trainer_te as trainer_venue_te_diff
  ,t_tr_te.trainer_distance_band_te - t_tr_te.trainer_te as trainer_distance_band_te_diff
  ,t_tr_te.trainer_distance_te - t_tr_te.trainer_te as trainer_distance_te_diff
  ,t_tr_te.trainer_direction_te - t_tr_te.trainer_te as trainer_direction_te_diff
  ,t_tr_te.trainer_course_type_venue_te - t_tr_te.trainer_te as trainer_course_type_venue_te_diff
  ,t_tr_te.trainer_course_type_distance_te - t_tr_te.trainer_te as trainer_course_type_distance_te_diff
  ,t_tr_te.trainer_course_type_distance_venue_te - t_tr_te.trainer_te as trainer_course_type_distance_venue_te_diff
  -- 種牡馬TE
  ,t_s_te.sire_te
  ,t_s_te.sire_course_type_te
  ,t_s_te.sire_venue_te
  ,t_s_te.sire_distance_band_te
  ,t_s_te.sire_distance_te
  ,t_s_te.sire_direction_te
  ,t_s_te.sire_course_type_venue_te
  ,t_s_te.sire_course_type_distance_te
  ,t_s_te.sire_course_type_distance_venue_te
  -- 種牡馬TE差分（条件別TE − 全体TE）
  ,t_s_te.sire_course_type_te - t_s_te.sire_te as sire_course_type_te_diff
  ,t_s_te.sire_venue_te - t_s_te.sire_te as sire_venue_te_diff
  ,t_s_te.sire_distance_band_te - t_s_te.sire_te as sire_distance_band_te_diff
  ,t_s_te.sire_distance_te - t_s_te.sire_te as sire_distance_te_diff
  ,t_s_te.sire_direction_te - t_s_te.sire_te as sire_direction_te_diff
  ,t_s_te.sire_course_type_venue_te - t_s_te.sire_te as sire_course_type_venue_te_diff
  ,t_s_te.sire_course_type_distance_te - t_s_te.sire_te as sire_course_type_distance_te_diff
  ,t_s_te.sire_course_type_distance_venue_te - t_s_te.sire_te as sire_course_type_distance_venue_te_diff
  -- 種牡馬年齢帯別TE（早熟・晩成性特徴量）
  ,t_s_te.sire_age2_te
  ,t_s_te.sire_age3_te
  ,t_s_te.sire_age4_te
  ,t_s_te.sire_age5plus_te
  ,case
    when t_p_r_f.horse_age = 2 then t_s_te.sire_age2_te
    when t_p_r_f.horse_age = 3 then t_s_te.sire_age3_te
    when t_p_r_f.horse_age = 4 then t_s_te.sire_age4_te
    else t_s_te.sire_age5plus_te
  end as sire_current_age_te
  ,(coalesce(t_s_te.sire_age2_te, t_s_te.sire_te) + coalesce(t_s_te.sire_age3_te, t_s_te.sire_te)) / 2
   - (coalesce(t_s_te.sire_age4_te, t_s_te.sire_te) + coalesce(t_s_te.sire_age5plus_te, t_s_te.sire_te)) / 2
   as sire_precocity_diff
  ,case
    when t_p_r_f.horse_age = 2 then t_s_te.sire_age2_te - t_s_te.sire_te
    when t_p_r_f.horse_age = 3 then t_s_te.sire_age3_te - t_s_te.sire_te
    when t_p_r_f.horse_age = 4 then t_s_te.sire_age4_te - t_s_te.sire_te
    else t_s_te.sire_age5plus_te - t_s_te.sire_te
  end as sire_age_vs_career_diff
  -- 種牡馬産駒出走比率（Issue #332）
  ,t_s_te.sire_course_type_run_ratio
  ,t_s_te.sire_venue_run_ratio
  ,t_s_te.sire_distance_band_run_ratio
  ,t_s_te.sire_distance_run_ratio
  -- 馬自身TE
  ,t_h_te.horse_te
  ,t_h_te.horse_course_type_te
  ,t_h_te.horse_venue_te
  ,t_h_te.horse_distance_band_te
  ,t_h_te.horse_distance_te
  ,t_h_te.horse_direction_te
  ,t_h_te.horse_jockey_te
  ,t_h_te.horse_season_te
  ,t_h_te.horse_course_type_venue_te
  ,t_h_te.horse_course_type_distance_te
  ,t_h_te.horse_course_type_distance_venue_te
  ,t_h_te.horse_distance_change_te
  ,t_h_te.horse_weight_carried_change_te
  -- 馬自身TE差分（条件別TE − 全体TE）
  ,t_h_te.horse_course_type_te - t_h_te.horse_te as horse_course_type_te_diff
  ,t_h_te.horse_venue_te - t_h_te.horse_te as horse_venue_te_diff
  ,t_h_te.horse_distance_band_te - t_h_te.horse_te as horse_distance_band_te_diff
  ,t_h_te.horse_distance_te - t_h_te.horse_te as horse_distance_te_diff
  ,t_h_te.horse_direction_te - t_h_te.horse_te as horse_direction_te_diff
  ,t_h_te.horse_jockey_te - t_h_te.horse_te as horse_jockey_te_diff
  ,t_h_te.horse_season_te - t_h_te.horse_te as horse_season_te_diff
  ,t_h_te.horse_course_type_venue_te - t_h_te.horse_te as horse_course_type_venue_te_diff
  ,t_h_te.horse_course_type_distance_te - t_h_te.horse_te as horse_course_type_distance_te_diff
  ,t_h_te.horse_course_type_distance_venue_te - t_h_te.horse_te as horse_course_type_distance_venue_te_diff
  ,t_h_te.horse_distance_change_te - t_h_te.horse_te as horse_distance_change_te_diff
  ,t_h_te.horse_weight_carried_change_te - t_h_te.horse_te as horse_weight_carried_change_te_diff
  -- 馬TE_diff 時系列集計（Issue #341）: 馬ごとの過去レースでのdiff値平均・ランク平均
  ,t_h_te_diff_s.horse_course_type_te_diff_avg
  ,t_h_te_diff_s.horse_venue_te_diff_avg
  ,t_h_te_diff_s.horse_distance_band_te_diff_avg
  ,t_h_te_diff_s.horse_distance_te_diff_avg
  ,t_h_te_diff_s.horse_direction_te_diff_avg
  ,t_h_te_diff_s.horse_jockey_te_diff_avg
  ,t_h_te_diff_s.horse_season_te_diff_avg
  ,t_h_te_diff_s.horse_cv_te_diff_avg
  ,t_h_te_diff_s.horse_cd_te_diff_avg
  ,t_h_te_diff_s.horse_cdv_te_diff_avg
  ,t_h_te_diff_s.horse_dc_te_diff_avg
  ,t_h_te_diff_s.horse_wcc_te_diff_avg
  ,t_h_te_diff_s.horse_course_type_te_diff_rank_avg
  ,t_h_te_diff_s.horse_venue_te_diff_rank_avg
  ,t_h_te_diff_s.horse_distance_band_te_diff_rank_avg
  ,t_h_te_diff_s.horse_distance_te_diff_rank_avg
  ,t_h_te_diff_s.horse_direction_te_diff_rank_avg
  ,t_h_te_diff_s.horse_jockey_te_diff_rank_avg
  ,t_h_te_diff_s.horse_season_te_diff_rank_avg
  ,t_h_te_diff_s.horse_cv_te_diff_rank_avg
  ,t_h_te_diff_s.horse_cd_te_diff_rank_avg
  ,t_h_te_diff_s.horse_cdv_te_diff_rank_avg
  ,t_h_te_diff_s.horse_dc_te_diff_rank_avg
  ,t_h_te_diff_s.horse_wcc_te_diff_rank_avg
  -- 距離帯別特徴量
  ,t_h_db_te.distance_band_top3_finish_rate
  ,t_h_db_te.distance_band_top1_finish_rate
  ,t_h_db_te.distance_band_rate_diff
  ,t_h_db_te.new_distance_band_flag
  -- 距離別特徴量
  ,t_h_d_te.distance_top3_finish_rate
  ,t_h_d_te.distance_top1_finish_rate
  ,t_h_d_te.distance_rate_diff
  -- new_distance_flag: gain=0 のため除去（Issue #296）
  -- キャリア最長・最短距離フラグ（グループ1: 全出走, Issue #305）
  ,t_p_r_f.distance > t_c_d.career_max_distance_so_far as is_career_max_distance
  ,t_p_r_f.distance - t_c_d.career_max_distance_so_far as career_max_distance_diff
  ,t_p_r_f.distance < t_c_d.career_min_distance_so_far as is_career_min_distance
  ,t_p_r_f.distance - t_c_d.career_min_distance_so_far as career_min_distance_diff
  ,t_c_d.career_max_distance_so_far - t_c_d.career_min_distance_so_far as career_distance_range
  ,t_c_d.career_distance_count
  -- キャリア最長・最短距離フラグ（グループ2: 3着以内レースのみ, Issue #305）
  ,t_p_r_f.distance > t_c_d.placed_max_distance_so_far as is_beyond_placed_max_distance
  ,t_p_r_f.distance - t_c_d.placed_max_distance_so_far as placed_max_distance_diff
  ,t_p_r_f.distance < t_c_d.placed_min_distance_so_far as is_below_placed_min_distance
  ,t_p_r_f.distance - t_c_d.placed_min_distance_so_far as placed_min_distance_diff
  ,t_c_d.placed_max_distance_so_far - t_c_d.placed_min_distance_so_far as placed_distance_range
  ,t_c_d.placed_distance_count
  -- 調教本追切特徴量 (CHAファイル由来)
  ,t_cha.cha_training_index
  ,t_cha.training_last_3f
  ,t_cha.training_furlongs
  ,t_cha.training_intensity
  ,t_cha.training_course_type
  ,t_cha.training_count
  -- 母馬競走実績特徴量（カテゴリA グループA-1: 全出走ベース距離統計）
  ,t_m_s.mare_race_count
  ,t_m_s.mare_avg_race_distance
  ,t_m_s.mare_max_race_distance
  ,t_m_s.mare_min_race_distance
  ,t_m_s.mare_max_race_distance - t_m_s.mare_min_race_distance as mare_distance_range
  ,t_p_r_f.distance - t_m_s.mare_avg_race_distance as mare_distance_diff
  ,t_p_r_f.distance - t_m_s.mare_max_race_distance as mare_max_distance_diff
  ,t_p_r_f.distance - t_m_s.mare_min_race_distance as mare_min_distance_diff
  -- 母馬競走実績特徴量（カテゴリA グループA-2: 3着以内レース絞り距離統計）
  ,t_m_s.mare_placed_race_count
  ,t_m_s.mare_placed_avg_distance
  ,t_m_s.mare_placed_max_distance
  ,t_m_s.mare_placed_min_distance
  ,t_m_s.mare_placed_max_distance - t_m_s.mare_placed_min_distance as mare_placed_distance_range
  ,t_p_r_f.distance - t_m_s.mare_placed_max_distance as mare_placed_max_distance_diff
  ,t_p_r_f.distance - t_m_s.mare_placed_min_distance as mare_placed_min_distance_diff
  -- 母馬競走実績特徴量（カテゴリA グループA-3: コース別複勝率）
  ,t_m_s.mare_place_rate
  ,t_m_s.mare_turf_place_rate
  ,t_m_s.mare_dirt_place_rate
  ,t_m_v.mare_venue_place_rate
  ,t_m_db.mare_distance_band_place_rate
  ,t_m_d.mare_distance_place_rate
  ,t_m_dir.mare_direction_place_rate
  ,t_m_cv.mare_course_type_venue_place_rate
  ,t_m_cd.mare_course_type_distance_band_place_rate
  -- 母馬競走実績特徴量（カテゴリA グループA-4: diff特徴量）
  ,t_m_s.mare_turf_place_rate - t_m_s.mare_place_rate as mare_turf_place_diff
  ,t_m_v.mare_venue_place_rate - t_m_s.mare_place_rate as mare_venue_place_rate_diff
  ,t_m_db.mare_distance_band_place_rate - t_m_s.mare_place_rate as mare_distance_band_place_rate_diff
  ,t_m_d.mare_distance_place_rate - t_m_s.mare_place_rate as mare_distance_place_rate_diff
  ,t_m_dir.mare_direction_place_rate - t_m_s.mare_place_rate as mare_direction_place_rate_diff
  ,t_m_cv.mare_course_type_venue_place_rate - t_m_s.mare_place_rate as mare_course_type_venue_place_rate_diff
  ,t_m_cd.mare_course_type_distance_band_place_rate - t_m_s.mare_place_rate as mare_course_type_distance_band_place_rate_diff
  -- 母馬産駒TE（カテゴリB）
  ,t_m_te.mare_te
  ,t_m_te.mare_course_type_te
  ,t_m_te.mare_venue_te
  ,t_m_te.mare_distance_band_te
  ,t_m_te.mare_distance_te
  ,t_m_te.mare_direction_te
  ,t_m_te.mare_course_type_venue_te
  ,t_m_te.mare_course_type_distance_te
  ,t_m_te.mare_course_type_distance_venue_te
  -- 母馬産駒TE差分（条件別TE − 全体TE）
  ,t_m_te.mare_course_type_te - t_m_te.mare_te as mare_course_type_te_diff
  ,t_m_te.mare_venue_te - t_m_te.mare_te as mare_venue_te_diff
  ,t_m_te.mare_distance_band_te - t_m_te.mare_te as mare_distance_band_te_diff
  ,t_m_te.mare_distance_te - t_m_te.mare_te as mare_distance_te_diff
  ,t_m_te.mare_direction_te - t_m_te.mare_te as mare_direction_te_diff
  ,t_m_te.mare_course_type_venue_te - t_m_te.mare_te as mare_course_type_venue_te_diff
  ,t_m_te.mare_course_type_distance_te - t_m_te.mare_te as mare_course_type_distance_te_diff
  ,t_m_te.mare_course_type_distance_venue_te - t_m_te.mare_te as mare_course_type_distance_venue_te_diff
  -- 母馬産駒年齢帯別TE（早熟・晩成性特徴量）
  ,t_m_te.mare_age2_te
  ,t_m_te.mare_age3_te
  ,t_m_te.mare_age4_te
  ,t_m_te.mare_age5plus_te
  ,case
    when t_p_r_f.horse_age = 2 then t_m_te.mare_age2_te
    when t_p_r_f.horse_age = 3 then t_m_te.mare_age3_te
    when t_p_r_f.horse_age = 4 then t_m_te.mare_age4_te
    else t_m_te.mare_age5plus_te
  end as mare_current_age_te
  ,(coalesce(t_m_te.mare_age2_te, t_m_te.mare_te) + coalesce(t_m_te.mare_age3_te, t_m_te.mare_te)) / 2
   - (coalesce(t_m_te.mare_age4_te, t_m_te.mare_te) + coalesce(t_m_te.mare_age5plus_te, t_m_te.mare_te)) / 2
   as mare_precocity_diff
  ,case
    when t_p_r_f.horse_age = 2 then t_m_te.mare_age2_te - t_m_te.mare_te
    when t_p_r_f.horse_age = 3 then t_m_te.mare_age3_te - t_m_te.mare_te
    when t_p_r_f.horse_age = 4 then t_m_te.mare_age4_te - t_m_te.mare_te
    else t_m_te.mare_age5plus_te - t_m_te.mare_te
  end as mare_age_vs_career_diff
  -- 母馬産駒出走比率（Issue #332）
  ,t_m_te.mare_course_type_run_ratio
  ,t_m_te.mare_venue_run_ratio
  ,t_m_te.mare_distance_band_run_ratio
  ,t_m_te.mare_distance_run_ratio
  -- 母馬自身の早熟・晩成性（カテゴリC）
  ,t_m_s.mare_early_career_place_rate
  ,t_m_s.mare_late_career_place_rate
  ,t_m_s.mare_early_career_place_rate - t_m_s.mare_late_career_place_rate as mare_precocity_index
  -- 前走〜5走前の馬場バイアス×コース取りの複合スコア（Issue #309 拡張）
  ,CASE t_p_r_f.course_position_prev1
    WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
    WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
    WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
    WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
    WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
    ELSE NULL
  END as prev1_course_bias_score
  ,CASE t_p_r_f.course_position_prev1
    WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost < 0
    WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner < 0
    WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2 < 0
    WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer < 0
    WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost < 0
    ELSE NULL
  END as prev1_course_bias_disadvantage_flag
  ,CASE t_p_r_f.course_position_prev2
    WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
    WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
    WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
    WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
    WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
    ELSE NULL
  END as prev2_course_bias_score
  ,CASE t_p_r_f.course_position_prev2
    WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost < 0
    WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner < 0
    WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2 < 0
    WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer < 0
    WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost < 0
    ELSE NULL
  END as prev2_course_bias_disadvantage_flag
  ,CASE t_p_r_f.course_position_prev3
    WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
    WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
    WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
    WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
    WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
    ELSE NULL
  END as prev3_course_bias_score
  ,CASE t_p_r_f.course_position_prev3
    WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost < 0
    WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner < 0
    WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2 < 0
    WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer < 0
    WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost < 0
    ELSE NULL
  END as prev3_course_bias_disadvantage_flag
  ,CASE t_p_r_f.course_position_prev4
    WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
    WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
    WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
    WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
    WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
    ELSE NULL
  END as prev4_course_bias_score
  ,CASE t_p_r_f.course_position_prev4
    WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost < 0
    WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner < 0
    WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2 < 0
    WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer < 0
    WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost < 0
    ELSE NULL
  END as prev4_course_bias_disadvantage_flag
  ,CASE t_p_r_f.course_position_prev5
    WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
    WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
    WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
    WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
    WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
    ELSE NULL
  END as prev5_course_bias_score
  ,CASE t_p_r_f.course_position_prev5
    WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost < 0
    WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner < 0
    WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2 < 0
    WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer < 0
    WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost < 0
    ELSE NULL
  END as prev5_course_bias_disadvantage_flag
  -- 枠番×馬場バイアス交差特徴量（Issue #310）
  -- グループ1: 枠順バイアス有利不利スコア
  ,CASE
    WHEN t_p_r_f.horse_number_ratio <= 0.25 THEN t_h_m_f.straight_bias_innermost
    WHEN t_p_r_f.horse_number_ratio <= 0.50 THEN t_h_m_f.straight_bias_inner
    WHEN t_p_r_f.horse_number_ratio <= 0.75 THEN t_h_m_f.straight_bias_outer
    ELSE                                          t_h_m_f.straight_bias_outermost
  END as gate_bias_score
  ,CASE
    WHEN t_p_r_f.horse_number_ratio <= 0.25 THEN t_h_m_f.straight_bias_innermost > 0
    WHEN t_p_r_f.horse_number_ratio <= 0.50 THEN t_h_m_f.straight_bias_inner > 0
    WHEN t_p_r_f.horse_number_ratio <= 0.75 THEN t_h_m_f.straight_bias_outer > 0
    ELSE                                          t_h_m_f.straight_bias_outermost > 0
  END as gate_bias_advantage_flag
  -- グループ2: 馬場バイアスの強度指標
  ,t_h_m_f.straight_bias_innermost - t_h_m_f.straight_bias_outermost as straight_bias_range
  ,ABS(t_h_m_f.straight_bias_innermost - t_h_m_f.straight_bias_outermost) >= 3 as is_strong_bias_race
  -- グループ3: コース取り傾向 × 当日バイアスの複合リスクスコア
  ,t_p_r_f.ema_course_position * t_h_m_f.straight_bias_inner as course_position_bias_risk
  -- 馬場バイアス×コース取りによるIDM補正特徴量（Issue #311）
  -- グループ1: 前走〜5走前のゾーン中立IDM（ゾーン固有バイアスを除去した実力値）
  ,t_p_r_f.idm_1
    - CASE t_p_r_f.course_position_prev1
        WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev1
    AS idm_zone_neutral_1
  ,t_p_r_f.idm_2
    - CASE t_p_r_f.course_position_prev2
        WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev2
    AS idm_zone_neutral_2
  ,t_p_r_f.idm_3
    - CASE t_p_r_f.course_position_prev3
        WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev3
    AS idm_zone_neutral_3
  ,t_p_r_f.idm_4
    - CASE t_p_r_f.course_position_prev4
        WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev4
    AS idm_zone_neutral_4
  ,t_p_r_f.idm_5
    - CASE t_p_r_f.course_position_prev5
        WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev5
    AS idm_zone_neutral_5
  -- グループ1: 前走〜5走前のポテンシャルIDM（最有利ゾーンで走った場合の推定能力）
  ,t_p_r_f.idm_1
    + GREATEST(
        COALESCE(t_p_r_f.prev1_straight_bias_innermost, t_p_r_f.track_bias_prev1),
        COALESCE(t_p_r_f.prev1_straight_bias_inner,     t_p_r_f.track_bias_prev1),
        COALESCE(t_p_r_f.prev1_straight_bias_outer,     t_p_r_f.track_bias_prev1),
        COALESCE(t_p_r_f.prev1_straight_bias_outermost, t_p_r_f.track_bias_prev1)
      )
    - CASE t_p_r_f.course_position_prev1
        WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_potential_1
  ,t_p_r_f.idm_2
    + GREATEST(
        COALESCE(t_p_r_f.prev2_straight_bias_innermost, t_p_r_f.track_bias_prev2),
        COALESCE(t_p_r_f.prev2_straight_bias_inner,     t_p_r_f.track_bias_prev2),
        COALESCE(t_p_r_f.prev2_straight_bias_outer,     t_p_r_f.track_bias_prev2),
        COALESCE(t_p_r_f.prev2_straight_bias_outermost, t_p_r_f.track_bias_prev2)
      )
    - CASE t_p_r_f.course_position_prev2
        WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_potential_2
  ,t_p_r_f.idm_3
    + GREATEST(
        COALESCE(t_p_r_f.prev3_straight_bias_innermost, t_p_r_f.track_bias_prev3),
        COALESCE(t_p_r_f.prev3_straight_bias_inner,     t_p_r_f.track_bias_prev3),
        COALESCE(t_p_r_f.prev3_straight_bias_outer,     t_p_r_f.track_bias_prev3),
        COALESCE(t_p_r_f.prev3_straight_bias_outermost, t_p_r_f.track_bias_prev3)
      )
    - CASE t_p_r_f.course_position_prev3
        WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_potential_3
  ,t_p_r_f.idm_4
    + GREATEST(
        COALESCE(t_p_r_f.prev4_straight_bias_innermost, t_p_r_f.track_bias_prev4),
        COALESCE(t_p_r_f.prev4_straight_bias_inner,     t_p_r_f.track_bias_prev4),
        COALESCE(t_p_r_f.prev4_straight_bias_outer,     t_p_r_f.track_bias_prev4),
        COALESCE(t_p_r_f.prev4_straight_bias_outermost, t_p_r_f.track_bias_prev4)
      )
    - CASE t_p_r_f.course_position_prev4
        WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_potential_4
  ,t_p_r_f.idm_5
    + GREATEST(
        COALESCE(t_p_r_f.prev5_straight_bias_innermost, t_p_r_f.track_bias_prev5),
        COALESCE(t_p_r_f.prev5_straight_bias_inner,     t_p_r_f.track_bias_prev5),
        COALESCE(t_p_r_f.prev5_straight_bias_outer,     t_p_r_f.track_bias_prev5),
        COALESCE(t_p_r_f.prev5_straight_bias_outermost, t_p_r_f.track_bias_prev5)
      )
    - CASE t_p_r_f.course_position_prev5
        WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_potential_5
  -- グループ2: 補正IDMの集計特徴量（BigQueryは同一SELECT内のエイリアス参照不可のためインライン展開）
  ,SAFE_DIVIDE(
    COALESCE(
      t_p_r_f.idm_1
      - CASE t_p_r_f.course_position_prev1
          WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
          ELSE NULL
        END
      + t_p_r_f.track_bias_prev1,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_2
      - CASE t_p_r_f.course_position_prev2
          WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
          ELSE NULL
        END
      + t_p_r_f.track_bias_prev2,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_3
      - CASE t_p_r_f.course_position_prev3
          WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
          ELSE NULL
        END
      + t_p_r_f.track_bias_prev3,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_4
      - CASE t_p_r_f.course_position_prev4
          WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
          ELSE NULL
        END
      + t_p_r_f.track_bias_prev4,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_5
      - CASE t_p_r_f.course_position_prev5
          WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
          ELSE NULL
        END
      + t_p_r_f.track_bias_prev5,
      0
    ),
    NULLIF(
      CASE WHEN t_p_r_f.idm_1 IS NOT NULL AND t_p_r_f.course_position_prev1 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_2 IS NOT NULL AND t_p_r_f.course_position_prev2 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_3 IS NOT NULL AND t_p_r_f.course_position_prev3 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_4 IS NOT NULL AND t_p_r_f.course_position_prev4 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_5 IS NOT NULL AND t_p_r_f.course_position_prev5 IS NOT NULL THEN 1 ELSE 0 END,
      0
    )
  ) AS mean_idm_zone_neutral
  ,SAFE_DIVIDE(
    COALESCE(
      (t_p_r_f.idm_1
       - CASE t_p_r_f.course_position_prev1
           WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev1) * 1.5,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_2
       - CASE t_p_r_f.course_position_prev2
           WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev2) * 1.25,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_3
       - CASE t_p_r_f.course_position_prev3
           WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev3) * 1.0,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_4
       - CASE t_p_r_f.course_position_prev4
           WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev4) * 0.75,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_5
       - CASE t_p_r_f.course_position_prev5
           WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev5) * 0.5,
      0
    ),
    NULLIF(
      CASE WHEN t_p_r_f.idm_1 IS NOT NULL AND t_p_r_f.course_position_prev1 IS NOT NULL THEN 1.5 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_2 IS NOT NULL AND t_p_r_f.course_position_prev2 IS NOT NULL THEN 1.25 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_3 IS NOT NULL AND t_p_r_f.course_position_prev3 IS NOT NULL THEN 1.0 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_4 IS NOT NULL AND t_p_r_f.course_position_prev4 IS NOT NULL THEN 0.75 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_5 IS NOT NULL AND t_p_r_f.course_position_prev5 IS NOT NULL THEN 0.5 ELSE 0 END,
      0
    )
  ) AS ema_idm_zone_neutral
  ,SAFE_DIVIDE(
    COALESCE(
      t_p_r_f.idm_1
      + GREATEST(
          COALESCE(t_p_r_f.prev1_straight_bias_innermost, t_p_r_f.track_bias_prev1),
          COALESCE(t_p_r_f.prev1_straight_bias_inner,     t_p_r_f.track_bias_prev1),
          COALESCE(t_p_r_f.prev1_straight_bias_outer,     t_p_r_f.track_bias_prev1),
          COALESCE(t_p_r_f.prev1_straight_bias_outermost, t_p_r_f.track_bias_prev1)
        )
      - CASE t_p_r_f.course_position_prev1
          WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
          ELSE NULL
        END,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_2
      + GREATEST(
          COALESCE(t_p_r_f.prev2_straight_bias_innermost, t_p_r_f.track_bias_prev2),
          COALESCE(t_p_r_f.prev2_straight_bias_inner,     t_p_r_f.track_bias_prev2),
          COALESCE(t_p_r_f.prev2_straight_bias_outer,     t_p_r_f.track_bias_prev2),
          COALESCE(t_p_r_f.prev2_straight_bias_outermost, t_p_r_f.track_bias_prev2)
        )
      - CASE t_p_r_f.course_position_prev2
          WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
          ELSE NULL
        END,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_3
      + GREATEST(
          COALESCE(t_p_r_f.prev3_straight_bias_innermost, t_p_r_f.track_bias_prev3),
          COALESCE(t_p_r_f.prev3_straight_bias_inner,     t_p_r_f.track_bias_prev3),
          COALESCE(t_p_r_f.prev3_straight_bias_outer,     t_p_r_f.track_bias_prev3),
          COALESCE(t_p_r_f.prev3_straight_bias_outermost, t_p_r_f.track_bias_prev3)
        )
      - CASE t_p_r_f.course_position_prev3
          WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
          ELSE NULL
        END,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_4
      + GREATEST(
          COALESCE(t_p_r_f.prev4_straight_bias_innermost, t_p_r_f.track_bias_prev4),
          COALESCE(t_p_r_f.prev4_straight_bias_inner,     t_p_r_f.track_bias_prev4),
          COALESCE(t_p_r_f.prev4_straight_bias_outer,     t_p_r_f.track_bias_prev4),
          COALESCE(t_p_r_f.prev4_straight_bias_outermost, t_p_r_f.track_bias_prev4)
        )
      - CASE t_p_r_f.course_position_prev4
          WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
          ELSE NULL
        END,
      0
    ) +
    COALESCE(
      t_p_r_f.idm_5
      + GREATEST(
          COALESCE(t_p_r_f.prev5_straight_bias_innermost, t_p_r_f.track_bias_prev5),
          COALESCE(t_p_r_f.prev5_straight_bias_inner,     t_p_r_f.track_bias_prev5),
          COALESCE(t_p_r_f.prev5_straight_bias_outer,     t_p_r_f.track_bias_prev5),
          COALESCE(t_p_r_f.prev5_straight_bias_outermost, t_p_r_f.track_bias_prev5)
        )
      - CASE t_p_r_f.course_position_prev5
          WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
          WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
          WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
          WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
          WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
          ELSE NULL
        END,
      0
    ),
    NULLIF(
      CASE WHEN t_p_r_f.idm_1 IS NOT NULL AND t_p_r_f.course_position_prev1 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_2 IS NOT NULL AND t_p_r_f.course_position_prev2 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_3 IS NOT NULL AND t_p_r_f.course_position_prev3 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_4 IS NOT NULL AND t_p_r_f.course_position_prev4 IS NOT NULL THEN 1 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_5 IS NOT NULL AND t_p_r_f.course_position_prev5 IS NOT NULL THEN 1 ELSE 0 END,
      0
    )
  ) AS mean_idm_zone_potential
  ,SAFE_DIVIDE(
    COALESCE(
      (t_p_r_f.idm_1
       + GREATEST(
           COALESCE(t_p_r_f.prev1_straight_bias_innermost, t_p_r_f.track_bias_prev1),
           COALESCE(t_p_r_f.prev1_straight_bias_inner,     t_p_r_f.track_bias_prev1),
           COALESCE(t_p_r_f.prev1_straight_bias_outer,     t_p_r_f.track_bias_prev1),
           COALESCE(t_p_r_f.prev1_straight_bias_outermost, t_p_r_f.track_bias_prev1)
         )
       - CASE t_p_r_f.course_position_prev1
           WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
           ELSE NULL
         END) * 1.5,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_2
       + GREATEST(
           COALESCE(t_p_r_f.prev2_straight_bias_innermost, t_p_r_f.track_bias_prev2),
           COALESCE(t_p_r_f.prev2_straight_bias_inner,     t_p_r_f.track_bias_prev2),
           COALESCE(t_p_r_f.prev2_straight_bias_outer,     t_p_r_f.track_bias_prev2),
           COALESCE(t_p_r_f.prev2_straight_bias_outermost, t_p_r_f.track_bias_prev2)
         )
       - CASE t_p_r_f.course_position_prev2
           WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
           ELSE NULL
         END) * 1.25,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_3
       + GREATEST(
           COALESCE(t_p_r_f.prev3_straight_bias_innermost, t_p_r_f.track_bias_prev3),
           COALESCE(t_p_r_f.prev3_straight_bias_inner,     t_p_r_f.track_bias_prev3),
           COALESCE(t_p_r_f.prev3_straight_bias_outer,     t_p_r_f.track_bias_prev3),
           COALESCE(t_p_r_f.prev3_straight_bias_outermost, t_p_r_f.track_bias_prev3)
         )
       - CASE t_p_r_f.course_position_prev3
           WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
           ELSE NULL
         END) * 1.0,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_4
       + GREATEST(
           COALESCE(t_p_r_f.prev4_straight_bias_innermost, t_p_r_f.track_bias_prev4),
           COALESCE(t_p_r_f.prev4_straight_bias_inner,     t_p_r_f.track_bias_prev4),
           COALESCE(t_p_r_f.prev4_straight_bias_outer,     t_p_r_f.track_bias_prev4),
           COALESCE(t_p_r_f.prev4_straight_bias_outermost, t_p_r_f.track_bias_prev4)
         )
       - CASE t_p_r_f.course_position_prev4
           WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
           ELSE NULL
         END) * 0.75,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_5
       + GREATEST(
           COALESCE(t_p_r_f.prev5_straight_bias_innermost, t_p_r_f.track_bias_prev5),
           COALESCE(t_p_r_f.prev5_straight_bias_inner,     t_p_r_f.track_bias_prev5),
           COALESCE(t_p_r_f.prev5_straight_bias_outer,     t_p_r_f.track_bias_prev5),
           COALESCE(t_p_r_f.prev5_straight_bias_outermost, t_p_r_f.track_bias_prev5)
         )
       - CASE t_p_r_f.course_position_prev5
           WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
           ELSE NULL
         END) * 0.5,
      0
    ),
    NULLIF(
      CASE WHEN t_p_r_f.idm_1 IS NOT NULL AND t_p_r_f.course_position_prev1 IS NOT NULL THEN 1.5 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_2 IS NOT NULL AND t_p_r_f.course_position_prev2 IS NOT NULL THEN 1.25 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_3 IS NOT NULL AND t_p_r_f.course_position_prev3 IS NOT NULL THEN 1.0 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_4 IS NOT NULL AND t_p_r_f.course_position_prev4 IS NOT NULL THEN 0.75 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_5 IS NOT NULL AND t_p_r_f.course_position_prev5 IS NOT NULL THEN 0.5 ELSE 0 END,
      0
    )
  ) AS ema_idm_zone_potential
  -- グループ3: 補正量（= ゾーン中立IDM - IDM = track_bias - course_bias）
  ,t_p_r_f.track_bias_prev1
    - CASE t_p_r_f.course_position_prev1
        WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_correction_1
  ,t_p_r_f.track_bias_prev2
    - CASE t_p_r_f.course_position_prev2
        WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_correction_2
  ,t_p_r_f.track_bias_prev3
    - CASE t_p_r_f.course_position_prev3
        WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_correction_3
  ,t_p_r_f.track_bias_prev4
    - CASE t_p_r_f.course_position_prev4
        WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_correction_4
  ,t_p_r_f.track_bias_prev5
    - CASE t_p_r_f.course_position_prev5
        WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
        ELSE NULL
      END
    AS idm_zone_correction_5
  -- グループ4: 補正後IDMの現在の出走条件との乖離・トレンド
  ,SAFE_DIVIDE(
    COALESCE(
      (t_p_r_f.idm_1
       - CASE t_p_r_f.course_position_prev1
           WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev1) * 1.5,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_2
       - CASE t_p_r_f.course_position_prev2
           WHEN 1 THEN t_p_r_f.prev2_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev2_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev2_straight_bias_inner + t_p_r_f.prev2_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev2_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev2_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev2) * 1.25,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_3
       - CASE t_p_r_f.course_position_prev3
           WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev3) * 1.0,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_4
       - CASE t_p_r_f.course_position_prev4
           WHEN 1 THEN t_p_r_f.prev4_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev4_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev4_straight_bias_inner + t_p_r_f.prev4_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev4_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev4_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev4) * 0.75,
      0
    ) +
    COALESCE(
      (t_p_r_f.idm_5
       - CASE t_p_r_f.course_position_prev5
           WHEN 1 THEN t_p_r_f.prev5_straight_bias_innermost
           WHEN 2 THEN t_p_r_f.prev5_straight_bias_inner
           WHEN 3 THEN (t_p_r_f.prev5_straight_bias_inner + t_p_r_f.prev5_straight_bias_outer) / 2
           WHEN 4 THEN t_p_r_f.prev5_straight_bias_outer
           WHEN 5 THEN t_p_r_f.prev5_straight_bias_outermost
           ELSE NULL
         END
       + t_p_r_f.track_bias_prev5) * 0.5,
      0
    ),
    NULLIF(
      CASE WHEN t_p_r_f.idm_1 IS NOT NULL AND t_p_r_f.course_position_prev1 IS NOT NULL THEN 1.5 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_2 IS NOT NULL AND t_p_r_f.course_position_prev2 IS NOT NULL THEN 1.25 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_3 IS NOT NULL AND t_p_r_f.course_position_prev3 IS NOT NULL THEN 1.0 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_4 IS NOT NULL AND t_p_r_f.course_position_prev4 IS NOT NULL THEN 0.75 ELSE 0 END +
      CASE WHEN t_p_r_f.idm_5 IS NOT NULL AND t_p_r_f.course_position_prev5 IS NOT NULL THEN 0.5 ELSE 0 END,
      0
    )
  ) - t_p_r_f.ema_idm AS ema_idm_zone_neutral_diff
  ,(t_p_r_f.idm_1
    - CASE t_p_r_f.course_position_prev1
        WHEN 1 THEN t_p_r_f.prev1_straight_bias_innermost
        WHEN 2 THEN t_p_r_f.prev1_straight_bias_inner
        WHEN 3 THEN (t_p_r_f.prev1_straight_bias_inner + t_p_r_f.prev1_straight_bias_outer) / 2
        WHEN 4 THEN t_p_r_f.prev1_straight_bias_outer
        WHEN 5 THEN t_p_r_f.prev1_straight_bias_outermost
        ELSE NULL
      END
    + t_p_r_f.track_bias_prev1)
  - (t_p_r_f.idm_3
     - CASE t_p_r_f.course_position_prev3
         WHEN 1 THEN t_p_r_f.prev3_straight_bias_innermost
         WHEN 2 THEN t_p_r_f.prev3_straight_bias_inner
         WHEN 3 THEN (t_p_r_f.prev3_straight_bias_inner + t_p_r_f.prev3_straight_bias_outer) / 2
         WHEN 4 THEN t_p_r_f.prev3_straight_bias_outer
         WHEN 5 THEN t_p_r_f.prev3_straight_bias_outermost
         ELSE NULL
       END
     + t_p_r_f.track_bias_prev3)
  AS idm_zone_neutral_trend
  /* 同一レース内展開指標（avg_gate_style_scoreに基づく集計、Issue #343） */
  ,countif(t_p_r_f.avg_gate_style_score <= 2.5) over (partition by t_p_r_f.race_id) as same_race_front_count
  ,rank() over (partition by t_p_r_f.race_id order by t_p_r_f.avg_gate_style_score nulls last) as same_race_style_rank
  /* 開催条件別ペース傾向・脚質適性特徴量（Issue #349） */
  ,t_cps.course_pace_score
  ,-(abs(t_p_r_f.avg_gate_style_score - t_cps.course_pace_score)) as gate_style_advantage_score
  ,case
    when t_p_r_f.avg_gate_style_score is null or t_cps.course_pace_score is null then null
    when abs(t_p_r_f.avg_gate_style_score - t_cps.course_pace_score) <= 0.5 then 1
    else 0
  end as gate_style_advantage_flag
  ,t_gs_te.gate_style_course_te
  /* グレード別TE・格上挑戦フラグ特徴量（Issue #347） */
  ,t_g_te.horse_g1_te
  ,t_g_te.horse_g2_te
  ,t_g_te.horse_g3_te
  ,t_g_te.grade_step_up_flag
  ,t_g_te.g1_experience_flag
  ,t_g_te.best_grade_achieved
from
  temp_past_race_features2 as t_p_r_f
  left join temp_horse_master_feature2 as t_h_m_f
    on t_p_r_f.race_id = t_h_m_f.race_id
    and t_p_r_f.horse_number = t_h_m_f.horse_number
  left join temp_jockey_te as t_j_te
    on t_p_r_f.race_id = t_j_te.race_id
    and t_p_r_f.horse_number = t_j_te.horse_number
  left join temp_jockey_horse_combo_te as t_jh_te
    on t_p_r_f.race_id = t_jh_te.race_id
    and t_p_r_f.horse_number = t_jh_te.horse_number
  left join temp_jockey_change as t_j_c
    on t_p_r_f.race_id = t_j_c.race_id
    and t_p_r_f.horse_number = t_j_c.horse_number
  left join temp_trainer_te as t_tr_te
    on t_p_r_f.race_id = t_tr_te.race_id
    and t_p_r_f.horse_number = t_tr_te.horse_number
  left join temp_sire_te as t_s_te
    on t_p_r_f.race_id = t_s_te.race_id
    and t_p_r_f.horse_number = t_s_te.horse_number
  left join temp_horse_te as t_h_te
    on t_p_r_f.race_id = t_h_te.race_id
    and t_p_r_f.horse_number = t_h_te.horse_number
  left join temp_horse_distance_band_te as t_h_db_te
    on t_p_r_f.race_id = t_h_db_te.race_id
    and t_p_r_f.horse_number = t_h_db_te.horse_number
  left join temp_horse_distance_te as t_h_d_te
    on t_p_r_f.race_id = t_h_d_te.race_id
    and t_p_r_f.horse_number = t_h_d_te.horse_number
  left join temp_training as t_cha
    on t_p_r_f.race_id = t_cha.race_id
    and t_p_r_f.horse_number = t_cha.horse_number
  left join temp_career_distance as t_c_d
    on t_p_r_f.race_id = t_c_d.race_id
    and t_p_r_f.horse_number = t_c_d.horse_number
  left join temp_mare_stats as t_m_s
    on t_p_r_f.horse_id = t_m_s.horse_id
  left join temp_mare_venue_stats as t_m_v
    on t_p_r_f.horse_id = t_m_v.horse_id
    and t_p_r_f.venue_code = t_m_v.venue_code
  left join temp_mare_distance_band_stats as t_m_db
    on t_p_r_f.horse_id = t_m_db.horse_id
    and t_p_r_f.distance_band = t_m_db.distance_band
  left join temp_mare_distance_stats as t_m_d
    on t_p_r_f.horse_id = t_m_d.horse_id
    and t_p_r_f.distance = t_m_d.distance
  left join temp_mare_direction_stats as t_m_dir
    on t_p_r_f.horse_id = t_m_dir.horse_id
    and t_p_r_f.direction = t_m_dir.direction
  left join temp_mare_cv_stats as t_m_cv
    on t_p_r_f.horse_id = t_m_cv.horse_id
    and t_p_r_f.course_type = t_m_cv.course_type
    and t_p_r_f.venue_code = t_m_cv.venue_code
  left join temp_mare_cd_stats as t_m_cd
    on t_p_r_f.horse_id = t_m_cd.horse_id
    and t_p_r_f.course_type = t_m_cd.course_type
    and t_p_r_f.distance_band = t_m_cd.distance_band
  left join temp_mare_te as t_m_te
    on t_p_r_f.race_id = t_m_te.race_id
    and t_p_r_f.horse_number = t_m_te.horse_number
  left join temp_horse_te_diff_summary as t_h_te_diff_s
    on t_p_r_f.race_id = t_h_te_diff_s.race_id
    and t_p_r_f.horse_number = t_h_te_diff_s.horse_number
  left join temp_course_pace_stats as t_cps
    on t_p_r_f.race_id = t_cps.race_id
    and t_p_r_f.horse_number = t_cps.horse_number
  left join temp_gate_style_te as t_gs_te
    on t_p_r_f.race_id = t_gs_te.race_id
    and t_p_r_f.horse_number = t_gs_te.horse_number
  left join temp_grade_te as t_g_te
    on t_p_r_f.race_id = t_g_te.race_id
    and t_p_r_f.horse_number = t_g_te.horse_number
)

-- NULL補完: 同一レース内の中央値で補完し、全員NULLの場合はフォールバック値を使用（Issue #330）
,temp_null_fill_med as (
  select
    *
    ,percentile_cont(jockey_te, 0.5) over (partition by race_id) as _jockey_te_med
    ,percentile_cont(jockey_course_type_te, 0.5) over (partition by race_id) as _jockey_course_type_te_med
    ,percentile_cont(jockey_venue_te, 0.5) over (partition by race_id) as _jockey_venue_te_med
    ,percentile_cont(jockey_distance_band_te, 0.5) over (partition by race_id) as _jockey_distance_band_te_med
    ,percentile_cont(jockey_distance_te, 0.5) over (partition by race_id) as _jockey_distance_te_med
    ,percentile_cont(jockey_direction_te, 0.5) over (partition by race_id) as _jockey_direction_te_med
    ,percentile_cont(jockey_course_type_venue_te, 0.5) over (partition by race_id) as _jockey_course_type_venue_te_med
    ,percentile_cont(jockey_course_type_distance_te, 0.5) over (partition by race_id) as _jockey_course_type_distance_te_med
    ,percentile_cont(jockey_course_type_distance_venue_te, 0.5) over (partition by race_id) as _jockey_course_type_distance_venue_te_med
    ,percentile_cont(jockey_course_type_te_diff, 0.5) over (partition by race_id) as _jockey_course_type_te_diff_med
    ,percentile_cont(jockey_venue_te_diff, 0.5) over (partition by race_id) as _jockey_venue_te_diff_med
    ,percentile_cont(jockey_distance_band_te_diff, 0.5) over (partition by race_id) as _jockey_distance_band_te_diff_med
    ,percentile_cont(jockey_distance_te_diff, 0.5) over (partition by race_id) as _jockey_distance_te_diff_med
    ,percentile_cont(jockey_direction_te_diff, 0.5) over (partition by race_id) as _jockey_direction_te_diff_med
    ,percentile_cont(jockey_course_type_venue_te_diff, 0.5) over (partition by race_id) as _jockey_course_type_venue_te_diff_med
    ,percentile_cont(jockey_course_type_distance_te_diff, 0.5) over (partition by race_id) as _jockey_course_type_distance_te_diff_med
    ,percentile_cont(jockey_course_type_distance_venue_te_diff, 0.5) over (partition by race_id) as _jockey_course_type_distance_venue_te_diff_med
    ,percentile_cont(jockey_horse_combo_te, 0.5) over (partition by race_id) as _jockey_horse_combo_te_med
    ,percentile_cont(trainer_te, 0.5) over (partition by race_id) as _trainer_te_med
    ,percentile_cont(trainer_course_type_te, 0.5) over (partition by race_id) as _trainer_course_type_te_med
    ,percentile_cont(trainer_venue_te, 0.5) over (partition by race_id) as _trainer_venue_te_med
    ,percentile_cont(trainer_distance_band_te, 0.5) over (partition by race_id) as _trainer_distance_band_te_med
    ,percentile_cont(trainer_distance_te, 0.5) over (partition by race_id) as _trainer_distance_te_med
    ,percentile_cont(trainer_direction_te, 0.5) over (partition by race_id) as _trainer_direction_te_med
    ,percentile_cont(trainer_course_type_venue_te, 0.5) over (partition by race_id) as _trainer_course_type_venue_te_med
    ,percentile_cont(trainer_course_type_distance_te, 0.5) over (partition by race_id) as _trainer_course_type_distance_te_med
    ,percentile_cont(trainer_course_type_distance_venue_te, 0.5) over (partition by race_id) as _trainer_course_type_distance_venue_te_med
    ,percentile_cont(trainer_course_type_te_diff, 0.5) over (partition by race_id) as _trainer_course_type_te_diff_med
    ,percentile_cont(trainer_venue_te_diff, 0.5) over (partition by race_id) as _trainer_venue_te_diff_med
    ,percentile_cont(trainer_distance_band_te_diff, 0.5) over (partition by race_id) as _trainer_distance_band_te_diff_med
    ,percentile_cont(trainer_distance_te_diff, 0.5) over (partition by race_id) as _trainer_distance_te_diff_med
    ,percentile_cont(trainer_direction_te_diff, 0.5) over (partition by race_id) as _trainer_direction_te_diff_med
    ,percentile_cont(trainer_course_type_venue_te_diff, 0.5) over (partition by race_id) as _trainer_course_type_venue_te_diff_med
    ,percentile_cont(trainer_course_type_distance_te_diff, 0.5) over (partition by race_id) as _trainer_course_type_distance_te_diff_med
    ,percentile_cont(trainer_course_type_distance_venue_te_diff, 0.5) over (partition by race_id) as _trainer_course_type_distance_venue_te_diff_med
    ,percentile_cont(sire_te, 0.5) over (partition by race_id) as _sire_te_med
    ,percentile_cont(sire_course_type_te, 0.5) over (partition by race_id) as _sire_course_type_te_med
    ,percentile_cont(sire_venue_te, 0.5) over (partition by race_id) as _sire_venue_te_med
    ,percentile_cont(sire_distance_band_te, 0.5) over (partition by race_id) as _sire_distance_band_te_med
    ,percentile_cont(sire_distance_te, 0.5) over (partition by race_id) as _sire_distance_te_med
    ,percentile_cont(sire_direction_te, 0.5) over (partition by race_id) as _sire_direction_te_med
    ,percentile_cont(sire_course_type_venue_te, 0.5) over (partition by race_id) as _sire_course_type_venue_te_med
    ,percentile_cont(sire_course_type_distance_te, 0.5) over (partition by race_id) as _sire_course_type_distance_te_med
    ,percentile_cont(sire_course_type_distance_venue_te, 0.5) over (partition by race_id) as _sire_course_type_distance_venue_te_med
    ,percentile_cont(sire_course_type_te_diff, 0.5) over (partition by race_id) as _sire_course_type_te_diff_med
    ,percentile_cont(sire_venue_te_diff, 0.5) over (partition by race_id) as _sire_venue_te_diff_med
    ,percentile_cont(sire_distance_band_te_diff, 0.5) over (partition by race_id) as _sire_distance_band_te_diff_med
    ,percentile_cont(sire_distance_te_diff, 0.5) over (partition by race_id) as _sire_distance_te_diff_med
    ,percentile_cont(sire_direction_te_diff, 0.5) over (partition by race_id) as _sire_direction_te_diff_med
    ,percentile_cont(sire_course_type_venue_te_diff, 0.5) over (partition by race_id) as _sire_course_type_venue_te_diff_med
    ,percentile_cont(sire_course_type_distance_te_diff, 0.5) over (partition by race_id) as _sire_course_type_distance_te_diff_med
    ,percentile_cont(sire_course_type_distance_venue_te_diff, 0.5) over (partition by race_id) as _sire_course_type_distance_venue_te_diff_med
    ,percentile_cont(sire_age2_te, 0.5) over (partition by race_id) as _sire_age2_te_med
    ,percentile_cont(sire_age3_te, 0.5) over (partition by race_id) as _sire_age3_te_med
    ,percentile_cont(sire_age4_te, 0.5) over (partition by race_id) as _sire_age4_te_med
    ,percentile_cont(sire_age5plus_te, 0.5) over (partition by race_id) as _sire_age5plus_te_med
    ,percentile_cont(sire_current_age_te, 0.5) over (partition by race_id) as _sire_current_age_te_med
    ,percentile_cont(sire_precocity_diff, 0.5) over (partition by race_id) as _sire_precocity_diff_med
    ,percentile_cont(sire_age_vs_career_diff, 0.5) over (partition by race_id) as _sire_age_vs_career_diff_med
    ,percentile_cont(sire_course_type_run_ratio, 0.5) over (partition by race_id) as _sire_course_type_run_ratio_med
    ,percentile_cont(sire_venue_run_ratio, 0.5) over (partition by race_id) as _sire_venue_run_ratio_med
    ,percentile_cont(sire_distance_band_run_ratio, 0.5) over (partition by race_id) as _sire_distance_band_run_ratio_med
    ,percentile_cont(sire_distance_run_ratio, 0.5) over (partition by race_id) as _sire_distance_run_ratio_med
    ,percentile_cont(horse_te, 0.5) over (partition by race_id) as _horse_te_med
    ,percentile_cont(horse_course_type_te, 0.5) over (partition by race_id) as _horse_course_type_te_med
    ,percentile_cont(horse_venue_te, 0.5) over (partition by race_id) as _horse_venue_te_med
    ,percentile_cont(horse_distance_band_te, 0.5) over (partition by race_id) as _horse_distance_band_te_med
    ,percentile_cont(horse_distance_te, 0.5) over (partition by race_id) as _horse_distance_te_med
    ,percentile_cont(horse_direction_te, 0.5) over (partition by race_id) as _horse_direction_te_med
    ,percentile_cont(horse_jockey_te, 0.5) over (partition by race_id) as _horse_jockey_te_med
    ,percentile_cont(horse_season_te, 0.5) over (partition by race_id) as _horse_season_te_med
    ,percentile_cont(horse_course_type_venue_te, 0.5) over (partition by race_id) as _horse_course_type_venue_te_med
    ,percentile_cont(horse_course_type_distance_te, 0.5) over (partition by race_id) as _horse_course_type_distance_te_med
    ,percentile_cont(horse_course_type_distance_venue_te, 0.5) over (partition by race_id) as _horse_course_type_distance_venue_te_med
    ,percentile_cont(horse_distance_change_te, 0.5) over (partition by race_id) as _horse_distance_change_te_med
    ,percentile_cont(horse_weight_carried_change_te, 0.5) over (partition by race_id) as _horse_weight_carried_change_te_med
    ,percentile_cont(horse_course_type_te_diff, 0.5) over (partition by race_id) as _horse_course_type_te_diff_med
    ,percentile_cont(horse_venue_te_diff, 0.5) over (partition by race_id) as _horse_venue_te_diff_med
    ,percentile_cont(horse_distance_band_te_diff, 0.5) over (partition by race_id) as _horse_distance_band_te_diff_med
    ,percentile_cont(horse_distance_te_diff, 0.5) over (partition by race_id) as _horse_distance_te_diff_med
    ,percentile_cont(horse_direction_te_diff, 0.5) over (partition by race_id) as _horse_direction_te_diff_med
    ,percentile_cont(horse_jockey_te_diff, 0.5) over (partition by race_id) as _horse_jockey_te_diff_med
    ,percentile_cont(horse_season_te_diff, 0.5) over (partition by race_id) as _horse_season_te_diff_med
    ,percentile_cont(horse_course_type_venue_te_diff, 0.5) over (partition by race_id) as _horse_course_type_venue_te_diff_med
    ,percentile_cont(horse_course_type_distance_te_diff, 0.5) over (partition by race_id) as _horse_course_type_distance_te_diff_med
    ,percentile_cont(horse_course_type_distance_venue_te_diff, 0.5) over (partition by race_id) as _horse_course_type_distance_venue_te_diff_med
    ,percentile_cont(horse_distance_change_te_diff, 0.5) over (partition by race_id) as _horse_distance_change_te_diff_med
    ,percentile_cont(horse_weight_carried_change_te_diff, 0.5) over (partition by race_id) as _horse_weight_carried_change_te_diff_med
    ,percentile_cont(horse_course_type_te_diff_avg, 0.5) over (partition by race_id) as _horse_course_type_te_diff_avg_med
    ,percentile_cont(horse_venue_te_diff_avg, 0.5) over (partition by race_id) as _horse_venue_te_diff_avg_med
    ,percentile_cont(horse_distance_band_te_diff_avg, 0.5) over (partition by race_id) as _horse_distance_band_te_diff_avg_med
    ,percentile_cont(horse_distance_te_diff_avg, 0.5) over (partition by race_id) as _horse_distance_te_diff_avg_med
    ,percentile_cont(horse_direction_te_diff_avg, 0.5) over (partition by race_id) as _horse_direction_te_diff_avg_med
    ,percentile_cont(horse_jockey_te_diff_avg, 0.5) over (partition by race_id) as _horse_jockey_te_diff_avg_med
    ,percentile_cont(horse_season_te_diff_avg, 0.5) over (partition by race_id) as _horse_season_te_diff_avg_med
    ,percentile_cont(horse_cv_te_diff_avg, 0.5) over (partition by race_id) as _horse_cv_te_diff_avg_med
    ,percentile_cont(horse_cd_te_diff_avg, 0.5) over (partition by race_id) as _horse_cd_te_diff_avg_med
    ,percentile_cont(horse_cdv_te_diff_avg, 0.5) over (partition by race_id) as _horse_cdv_te_diff_avg_med
    ,percentile_cont(horse_dc_te_diff_avg, 0.5) over (partition by race_id) as _horse_dc_te_diff_avg_med
    ,percentile_cont(horse_wcc_te_diff_avg, 0.5) over (partition by race_id) as _horse_wcc_te_diff_avg_med
    ,percentile_cont(horse_course_type_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_course_type_te_diff_rank_avg_med
    ,percentile_cont(horse_venue_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_venue_te_diff_rank_avg_med
    ,percentile_cont(horse_distance_band_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_distance_band_te_diff_rank_avg_med
    ,percentile_cont(horse_distance_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_distance_te_diff_rank_avg_med
    ,percentile_cont(horse_direction_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_direction_te_diff_rank_avg_med
    ,percentile_cont(horse_jockey_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_jockey_te_diff_rank_avg_med
    ,percentile_cont(horse_season_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_season_te_diff_rank_avg_med
    ,percentile_cont(horse_cv_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_cv_te_diff_rank_avg_med
    ,percentile_cont(horse_cd_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_cd_te_diff_rank_avg_med
    ,percentile_cont(horse_cdv_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_cdv_te_diff_rank_avg_med
    ,percentile_cont(horse_dc_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_dc_te_diff_rank_avg_med
    ,percentile_cont(horse_wcc_te_diff_rank_avg, 0.5) over (partition by race_id) as _horse_wcc_te_diff_rank_avg_med
    ,percentile_cont(mare_te, 0.5) over (partition by race_id) as _mare_te_med
    ,percentile_cont(mare_course_type_te, 0.5) over (partition by race_id) as _mare_course_type_te_med
    ,percentile_cont(mare_venue_te, 0.5) over (partition by race_id) as _mare_venue_te_med
    ,percentile_cont(mare_distance_band_te, 0.5) over (partition by race_id) as _mare_distance_band_te_med
    ,percentile_cont(mare_distance_te, 0.5) over (partition by race_id) as _mare_distance_te_med
    ,percentile_cont(mare_direction_te, 0.5) over (partition by race_id) as _mare_direction_te_med
    ,percentile_cont(mare_course_type_venue_te, 0.5) over (partition by race_id) as _mare_course_type_venue_te_med
    ,percentile_cont(mare_course_type_distance_te, 0.5) over (partition by race_id) as _mare_course_type_distance_te_med
    ,percentile_cont(mare_course_type_distance_venue_te, 0.5) over (partition by race_id) as _mare_course_type_distance_venue_te_med
    ,percentile_cont(mare_course_type_te_diff, 0.5) over (partition by race_id) as _mare_course_type_te_diff_med
    ,percentile_cont(mare_venue_te_diff, 0.5) over (partition by race_id) as _mare_venue_te_diff_med
    ,percentile_cont(mare_distance_band_te_diff, 0.5) over (partition by race_id) as _mare_distance_band_te_diff_med
    ,percentile_cont(mare_distance_te_diff, 0.5) over (partition by race_id) as _mare_distance_te_diff_med
    ,percentile_cont(mare_direction_te_diff, 0.5) over (partition by race_id) as _mare_direction_te_diff_med
    ,percentile_cont(mare_course_type_venue_te_diff, 0.5) over (partition by race_id) as _mare_course_type_venue_te_diff_med
    ,percentile_cont(mare_course_type_distance_te_diff, 0.5) over (partition by race_id) as _mare_course_type_distance_te_diff_med
    ,percentile_cont(mare_course_type_distance_venue_te_diff, 0.5) over (partition by race_id) as _mare_course_type_distance_venue_te_diff_med
    ,percentile_cont(mare_age2_te, 0.5) over (partition by race_id) as _mare_age2_te_med
    ,percentile_cont(mare_age3_te, 0.5) over (partition by race_id) as _mare_age3_te_med
    ,percentile_cont(mare_age4_te, 0.5) over (partition by race_id) as _mare_age4_te_med
    ,percentile_cont(mare_age5plus_te, 0.5) over (partition by race_id) as _mare_age5plus_te_med
    ,percentile_cont(mare_current_age_te, 0.5) over (partition by race_id) as _mare_current_age_te_med
    ,percentile_cont(mare_precocity_diff, 0.5) over (partition by race_id) as _mare_precocity_diff_med
    ,percentile_cont(mare_age_vs_career_diff, 0.5) over (partition by race_id) as _mare_age_vs_career_diff_med
    ,percentile_cont(mare_course_type_run_ratio, 0.5) over (partition by race_id) as _mare_course_type_run_ratio_med
    ,percentile_cont(mare_venue_run_ratio, 0.5) over (partition by race_id) as _mare_venue_run_ratio_med
    ,percentile_cont(mare_distance_band_run_ratio, 0.5) over (partition by race_id) as _mare_distance_band_run_ratio_med
    ,percentile_cont(mare_distance_run_ratio, 0.5) over (partition by race_id) as _mare_distance_run_ratio_med
    ,percentile_cont(idm_1, 0.5) over (partition by race_id) as _idm_1_med
    ,percentile_cont(finish_position_1, 0.5) over (partition by race_id) as _finish_position_1_med
    ,percentile_cont(finish_position_rate_1, 0.5) over (partition by race_id) as _finish_position_rate_1_med
    ,percentile_cont(win_odds_1, 0.5) over (partition by race_id) as _win_odds_1_med
    ,percentile_cont(win_popularity_1, 0.5) over (partition by race_id) as _win_popularity_1_med
    ,percentile_cont(popularity_rate_1, 0.5) over (partition by race_id) as _popularity_rate_1_med
    ,percentile_cont(upside_rate_1, 0.5) over (partition by race_id) as _upside_rate_1_med
    ,percentile_cont(finish_time_1, 0.5) over (partition by race_id) as _finish_time_1_med
    ,percentile_cont(last_3f_1, 0.5) over (partition by race_id) as _last_3f_1_med
    ,percentile_cont(last_3f_rank_in_race_1, 0.5) over (partition by race_id) as _last_3f_rank_in_race_1_med
    ,percentile_cont(race_date_diff_1, 0.5) over (partition by race_id) as _race_date_diff_1_med
    ,percentile_cont(race_date_diff_2, 0.5) over (partition by race_id) as _race_date_diff_2_med
    ,percentile_cont(race_date_diff_3, 0.5) over (partition by race_id) as _race_date_diff_3_med
    ,percentile_cont(race_date_diff_4, 0.5) over (partition by race_id) as _race_date_diff_4_med
    ,percentile_cont(race_date_diff_5, 0.5) over (partition by race_id) as _race_date_diff_5_med
    ,percentile_cont(mean_idm, 0.5) over (partition by race_id) as _mean_idm_med
    ,percentile_cont(ema_idm, 0.5) over (partition by race_id) as _ema_idm_med
    ,percentile_cont(max_idm, 0.5) over (partition by race_id) as _max_idm_med
    ,percentile_cont(min_idm, 0.5) over (partition by race_id) as _min_idm_med
    ,percentile_cont(idm_diff, 0.5) over (partition by race_id) as _idm_diff_med
    ,percentile_cont(mean_idm_diff, 0.5) over (partition by race_id) as _mean_idm_diff_med
    ,percentile_cont(ema_idm_diff, 0.5) over (partition by race_id) as _ema_idm_diff_med
    ,percentile_cont(max_idm_diff, 0.5) over (partition by race_id) as _max_idm_diff_med
    ,percentile_cont(mean_finish_position_rate, 0.5) over (partition by race_id) as _mean_finish_position_rate_med
    ,percentile_cont(ema_finish_position_rate, 0.5) over (partition by race_id) as _ema_finish_position_rate_med
    ,percentile_cont(max_finish_position_rate, 0.5) over (partition by race_id) as _max_finish_position_rate_med
    ,percentile_cont(min_finish_position_rate, 0.5) over (partition by race_id) as _min_finish_position_rate_med
    ,percentile_cont(mean_popularity_rate, 0.5) over (partition by race_id) as _mean_popularity_rate_med
    ,percentile_cont(ema_popularity_rate, 0.5) over (partition by race_id) as _ema_popularity_rate_med
    ,percentile_cont(max_popularity_rate, 0.5) over (partition by race_id) as _max_popularity_rate_med
    ,percentile_cont(min_popularity_rate, 0.5) over (partition by race_id) as _min_popularity_rate_med
    ,percentile_cont(mean_upside_rate, 0.5) over (partition by race_id) as _mean_upside_rate_med
    ,percentile_cont(ema_upside_rate, 0.5) over (partition by race_id) as _ema_upside_rate_med
    ,percentile_cont(max_upside_rate, 0.5) over (partition by race_id) as _max_upside_rate_med
    ,percentile_cont(min_upside_rate, 0.5) over (partition by race_id) as _min_upside_rate_med
    ,percentile_cont(finish_time_normalized, 0.5) over (partition by race_id) as _finish_time_normalized_med
    ,percentile_cont(last_3f_normalized, 0.5) over (partition by race_id) as _last_3f_normalized_med
    ,percentile_cont(idm_trend_3, 0.5) over (partition by race_id) as _idm_trend_3_med
    ,percentile_cont(finish_position_trend_3, 0.5) over (partition by race_id) as _finish_position_trend_3_med
    ,percentile_cont(mean_corner_gain_1to4, 0.5) over (partition by race_id) as _mean_corner_gain_1to4_med
    ,percentile_cont(ema_corner_gain_1to4, 0.5) over (partition by race_id) as _ema_corner_gain_1to4_med
    ,percentile_cont(corner1_to_finish_delta_prev_1, 0.5) over (partition by race_id) as _corner1_to_finish_delta_prev_1_med
    ,percentile_cont(distance_band_top3_finish_rate, 0.5) over (partition by race_id) as _distance_band_top3_finish_rate_med
    ,percentile_cont(distance_band_top1_finish_rate, 0.5) over (partition by race_id) as _distance_band_top1_finish_rate_med
    ,percentile_cont(distance_band_rate_diff, 0.5) over (partition by race_id) as _distance_band_rate_diff_med
    ,percentile_cont(distance_top3_finish_rate, 0.5) over (partition by race_id) as _distance_top3_finish_rate_med
    ,percentile_cont(distance_top1_finish_rate, 0.5) over (partition by race_id) as _distance_top1_finish_rate_med
    ,percentile_cont(distance_rate_diff, 0.5) over (partition by race_id) as _distance_rate_diff_med
    ,percentile_cont(career_max_distance_diff, 0.5) over (partition by race_id) as _career_max_distance_diff_med
    ,percentile_cont(career_min_distance_diff, 0.5) over (partition by race_id) as _career_min_distance_diff_med
    ,percentile_cont(career_distance_range, 0.5) over (partition by race_id) as _career_distance_range_med
    ,percentile_cont(career_distance_count, 0.5) over (partition by race_id) as _career_distance_count_med
    ,percentile_cont(placed_max_distance_diff, 0.5) over (partition by race_id) as _placed_max_distance_diff_med
    ,percentile_cont(placed_min_distance_diff, 0.5) over (partition by race_id) as _placed_min_distance_diff_med
    ,percentile_cont(placed_distance_range, 0.5) over (partition by race_id) as _placed_distance_range_med
    ,percentile_cont(placed_distance_count, 0.5) over (partition by race_id) as _placed_distance_count_med
    ,percentile_cont(cha_training_index, 0.5) over (partition by race_id) as _cha_training_index_med
    ,percentile_cont(training_last_3f, 0.5) over (partition by race_id) as _training_last_3f_med
    ,percentile_cont(training_furlongs, 0.5) over (partition by race_id) as _training_furlongs_med
    ,percentile_cont(training_intensity, 0.5) over (partition by race_id) as _training_intensity_med
    ,percentile_cont(training_count, 0.5) over (partition by race_id) as _training_count_med
    ,percentile_cont(mare_race_count, 0.5) over (partition by race_id) as _mare_race_count_med
    ,percentile_cont(mare_avg_race_distance, 0.5) over (partition by race_id) as _mare_avg_race_distance_med
    ,percentile_cont(mare_max_race_distance, 0.5) over (partition by race_id) as _mare_max_race_distance_med
    ,percentile_cont(mare_min_race_distance, 0.5) over (partition by race_id) as _mare_min_race_distance_med
    ,percentile_cont(mare_distance_range, 0.5) over (partition by race_id) as _mare_distance_range_med
    ,percentile_cont(mare_distance_diff, 0.5) over (partition by race_id) as _mare_distance_diff_med
    ,percentile_cont(mare_max_distance_diff, 0.5) over (partition by race_id) as _mare_max_distance_diff_med
    ,percentile_cont(mare_min_distance_diff, 0.5) over (partition by race_id) as _mare_min_distance_diff_med
    ,percentile_cont(mare_placed_race_count, 0.5) over (partition by race_id) as _mare_placed_race_count_med
    ,percentile_cont(mare_placed_avg_distance, 0.5) over (partition by race_id) as _mare_placed_avg_distance_med
    ,percentile_cont(mare_placed_max_distance, 0.5) over (partition by race_id) as _mare_placed_max_distance_med
    ,percentile_cont(mare_placed_min_distance, 0.5) over (partition by race_id) as _mare_placed_min_distance_med
    ,percentile_cont(mare_placed_distance_range, 0.5) over (partition by race_id) as _mare_placed_distance_range_med
    ,percentile_cont(mare_placed_max_distance_diff, 0.5) over (partition by race_id) as _mare_placed_max_distance_diff_med
    ,percentile_cont(mare_placed_min_distance_diff, 0.5) over (partition by race_id) as _mare_placed_min_distance_diff_med
    ,percentile_cont(mare_place_rate, 0.5) over (partition by race_id) as _mare_place_rate_med
    ,percentile_cont(mare_turf_place_rate, 0.5) over (partition by race_id) as _mare_turf_place_rate_med
    ,percentile_cont(mare_dirt_place_rate, 0.5) over (partition by race_id) as _mare_dirt_place_rate_med
    ,percentile_cont(mare_venue_place_rate, 0.5) over (partition by race_id) as _mare_venue_place_rate_med
    ,percentile_cont(mare_distance_band_place_rate, 0.5) over (partition by race_id) as _mare_distance_band_place_rate_med
    ,percentile_cont(mare_distance_place_rate, 0.5) over (partition by race_id) as _mare_distance_place_rate_med
    ,percentile_cont(mare_direction_place_rate, 0.5) over (partition by race_id) as _mare_direction_place_rate_med
    ,percentile_cont(mare_course_type_venue_place_rate, 0.5) over (partition by race_id) as _mare_course_type_venue_place_rate_med
    ,percentile_cont(mare_course_type_distance_band_place_rate, 0.5) over (partition by race_id) as _mare_course_type_distance_band_place_rate_med
    ,percentile_cont(mare_turf_place_diff, 0.5) over (partition by race_id) as _mare_turf_place_diff_med
    ,percentile_cont(mare_venue_place_rate_diff, 0.5) over (partition by race_id) as _mare_venue_place_rate_diff_med
    ,percentile_cont(mare_distance_band_place_rate_diff, 0.5) over (partition by race_id) as _mare_distance_band_place_rate_diff_med
    ,percentile_cont(mare_distance_place_rate_diff, 0.5) over (partition by race_id) as _mare_distance_place_rate_diff_med
    ,percentile_cont(mare_direction_place_rate_diff, 0.5) over (partition by race_id) as _mare_direction_place_rate_diff_med
    ,percentile_cont(mare_course_type_venue_place_rate_diff, 0.5) over (partition by race_id) as _mare_course_type_venue_place_rate_diff_med
    ,percentile_cont(mare_course_type_distance_band_place_rate_diff, 0.5) over (partition by race_id) as _mare_course_type_distance_band_place_rate_diff_med
    ,percentile_cont(mare_early_career_place_rate, 0.5) over (partition by race_id) as _mare_early_career_place_rate_med
    ,percentile_cont(mare_late_career_place_rate, 0.5) over (partition by race_id) as _mare_late_career_place_rate_med
    ,percentile_cont(mare_precocity_index, 0.5) over (partition by race_id) as _mare_precocity_index_med
    ,percentile_cont(prev1_course_bias_score, 0.5) over (partition by race_id) as _prev1_course_bias_score_med
    ,percentile_cont(prev2_course_bias_score, 0.5) over (partition by race_id) as _prev2_course_bias_score_med
    ,percentile_cont(prev3_course_bias_score, 0.5) over (partition by race_id) as _prev3_course_bias_score_med
    ,percentile_cont(prev4_course_bias_score, 0.5) over (partition by race_id) as _prev4_course_bias_score_med
    ,percentile_cont(prev5_course_bias_score, 0.5) over (partition by race_id) as _prev5_course_bias_score_med
    ,percentile_cont(gate_bias_score, 0.5) over (partition by race_id) as _gate_bias_score_med
    ,percentile_cont(straight_bias_range, 0.5) over (partition by race_id) as _straight_bias_range_med
    ,percentile_cont(course_position_bias_risk, 0.5) over (partition by race_id) as _course_position_bias_risk_med
    ,percentile_cont(idm_zone_neutral_1, 0.5) over (partition by race_id) as _idm_zone_neutral_1_med
    ,percentile_cont(idm_zone_neutral_2, 0.5) over (partition by race_id) as _idm_zone_neutral_2_med
    ,percentile_cont(idm_zone_neutral_3, 0.5) over (partition by race_id) as _idm_zone_neutral_3_med
    ,percentile_cont(idm_zone_neutral_4, 0.5) over (partition by race_id) as _idm_zone_neutral_4_med
    ,percentile_cont(idm_zone_neutral_5, 0.5) over (partition by race_id) as _idm_zone_neutral_5_med
    ,percentile_cont(idm_zone_neutral_trend, 0.5) over (partition by race_id) as _idm_zone_neutral_trend_med
    ,percentile_cont(course_pace_score, 0.5) over (partition by race_id) as _course_pace_score_med
    ,percentile_cont(gate_style_advantage_score, 0.5) over (partition by race_id) as _gate_style_advantage_score_med
    ,percentile_cont(gate_style_course_te, 0.5) over (partition by race_id) as _gate_style_course_te_med
    ,percentile_cont(last3f_rank_improvement_3, 0.5) over (partition by race_id) as _last3f_rank_improvement_3_med
    ,percentile_cont(last3f_rank_avg_3, 0.5) over (partition by race_id) as _last3f_rank_avg_3_med
  from temp_final_raw
)
,temp_null_filled as (
  select * except(
    _jockey_te_med,
    _jockey_course_type_te_med,
    _jockey_venue_te_med,
    _jockey_distance_band_te_med,
    _jockey_distance_te_med,
    _jockey_direction_te_med,
    _jockey_course_type_venue_te_med,
    _jockey_course_type_distance_te_med,
    _jockey_course_type_distance_venue_te_med,
    _jockey_course_type_te_diff_med,
    _jockey_venue_te_diff_med,
    _jockey_distance_band_te_diff_med,
    _jockey_distance_te_diff_med,
    _jockey_direction_te_diff_med,
    _jockey_course_type_venue_te_diff_med,
    _jockey_course_type_distance_te_diff_med,
    _jockey_course_type_distance_venue_te_diff_med,
    _jockey_horse_combo_te_med,
    _trainer_te_med,
    _trainer_course_type_te_med,
    _trainer_venue_te_med,
    _trainer_distance_band_te_med,
    _trainer_distance_te_med,
    _trainer_direction_te_med,
    _trainer_course_type_venue_te_med,
    _trainer_course_type_distance_te_med,
    _trainer_course_type_distance_venue_te_med,
    _trainer_course_type_te_diff_med,
    _trainer_venue_te_diff_med,
    _trainer_distance_band_te_diff_med,
    _trainer_distance_te_diff_med,
    _trainer_direction_te_diff_med,
    _trainer_course_type_venue_te_diff_med,
    _trainer_course_type_distance_te_diff_med,
    _trainer_course_type_distance_venue_te_diff_med,
    _sire_te_med,
    _sire_course_type_te_med,
    _sire_venue_te_med,
    _sire_distance_band_te_med,
    _sire_distance_te_med,
    _sire_direction_te_med,
    _sire_course_type_venue_te_med,
    _sire_course_type_distance_te_med,
    _sire_course_type_distance_venue_te_med,
    _sire_course_type_te_diff_med,
    _sire_venue_te_diff_med,
    _sire_distance_band_te_diff_med,
    _sire_distance_te_diff_med,
    _sire_direction_te_diff_med,
    _sire_course_type_venue_te_diff_med,
    _sire_course_type_distance_te_diff_med,
    _sire_course_type_distance_venue_te_diff_med,
    _sire_age2_te_med,
    _sire_age3_te_med,
    _sire_age4_te_med,
    _sire_age5plus_te_med,
    _sire_current_age_te_med,
    _sire_precocity_diff_med,
    _sire_age_vs_career_diff_med,
    _sire_course_type_run_ratio_med,
    _sire_venue_run_ratio_med,
    _sire_distance_band_run_ratio_med,
    _sire_distance_run_ratio_med,
    _horse_te_med,
    _horse_course_type_te_med,
    _horse_venue_te_med,
    _horse_distance_band_te_med,
    _horse_distance_te_med,
    _horse_direction_te_med,
    _horse_jockey_te_med,
    _horse_season_te_med,
    _horse_course_type_venue_te_med,
    _horse_course_type_distance_te_med,
    _horse_course_type_distance_venue_te_med,
    _horse_distance_change_te_med,
    _horse_weight_carried_change_te_med,
    _horse_course_type_te_diff_med,
    _horse_venue_te_diff_med,
    _horse_distance_band_te_diff_med,
    _horse_distance_te_diff_med,
    _horse_direction_te_diff_med,
    _horse_jockey_te_diff_med,
    _horse_season_te_diff_med,
    _horse_course_type_venue_te_diff_med,
    _horse_course_type_distance_te_diff_med,
    _horse_course_type_distance_venue_te_diff_med,
    _horse_distance_change_te_diff_med,
    _horse_weight_carried_change_te_diff_med,
    _horse_course_type_te_diff_avg_med,
    _horse_venue_te_diff_avg_med,
    _horse_distance_band_te_diff_avg_med,
    _horse_distance_te_diff_avg_med,
    _horse_direction_te_diff_avg_med,
    _horse_jockey_te_diff_avg_med,
    _horse_season_te_diff_avg_med,
    _horse_cv_te_diff_avg_med,
    _horse_cd_te_diff_avg_med,
    _horse_cdv_te_diff_avg_med,
    _horse_dc_te_diff_avg_med,
    _horse_wcc_te_diff_avg_med,
    _horse_course_type_te_diff_rank_avg_med,
    _horse_venue_te_diff_rank_avg_med,
    _horse_distance_band_te_diff_rank_avg_med,
    _horse_distance_te_diff_rank_avg_med,
    _horse_direction_te_diff_rank_avg_med,
    _horse_jockey_te_diff_rank_avg_med,
    _horse_season_te_diff_rank_avg_med,
    _horse_cv_te_diff_rank_avg_med,
    _horse_cd_te_diff_rank_avg_med,
    _horse_cdv_te_diff_rank_avg_med,
    _horse_dc_te_diff_rank_avg_med,
    _horse_wcc_te_diff_rank_avg_med,
    _mare_te_med,
    _mare_course_type_te_med,
    _mare_venue_te_med,
    _mare_distance_band_te_med,
    _mare_distance_te_med,
    _mare_direction_te_med,
    _mare_course_type_venue_te_med,
    _mare_course_type_distance_te_med,
    _mare_course_type_distance_venue_te_med,
    _mare_course_type_te_diff_med,
    _mare_venue_te_diff_med,
    _mare_distance_band_te_diff_med,
    _mare_distance_te_diff_med,
    _mare_direction_te_diff_med,
    _mare_course_type_venue_te_diff_med,
    _mare_course_type_distance_te_diff_med,
    _mare_course_type_distance_venue_te_diff_med,
    _mare_age2_te_med,
    _mare_age3_te_med,
    _mare_age4_te_med,
    _mare_age5plus_te_med,
    _mare_current_age_te_med,
    _mare_precocity_diff_med,
    _mare_age_vs_career_diff_med,
    _mare_course_type_run_ratio_med,
    _mare_venue_run_ratio_med,
    _mare_distance_band_run_ratio_med,
    _mare_distance_run_ratio_med,
    _idm_1_med,
    _finish_position_1_med,
    _finish_position_rate_1_med,
    _win_odds_1_med,
    _win_popularity_1_med,
    _popularity_rate_1_med,
    _upside_rate_1_med,
    _finish_time_1_med,
    _last_3f_1_med,
    _last_3f_rank_in_race_1_med,
    _race_date_diff_1_med,
    _race_date_diff_2_med,
    _race_date_diff_3_med,
    _race_date_diff_4_med,
    _race_date_diff_5_med,
    _mean_idm_med,
    _ema_idm_med,
    _max_idm_med,
    _min_idm_med,
    _idm_diff_med,
    _mean_idm_diff_med,
    _ema_idm_diff_med,
    _max_idm_diff_med,
    _mean_finish_position_rate_med,
    _ema_finish_position_rate_med,
    _max_finish_position_rate_med,
    _min_finish_position_rate_med,
    _mean_popularity_rate_med,
    _ema_popularity_rate_med,
    _max_popularity_rate_med,
    _min_popularity_rate_med,
    _mean_upside_rate_med,
    _ema_upside_rate_med,
    _max_upside_rate_med,
    _min_upside_rate_med,
    _finish_time_normalized_med,
    _last_3f_normalized_med,
    _idm_trend_3_med,
    _finish_position_trend_3_med,
    _mean_corner_gain_1to4_med,
    _ema_corner_gain_1to4_med,
    _corner1_to_finish_delta_prev_1_med,
    _distance_band_top3_finish_rate_med,
    _distance_band_top1_finish_rate_med,
    _distance_band_rate_diff_med,
    _distance_top3_finish_rate_med,
    _distance_top1_finish_rate_med,
    _distance_rate_diff_med,
    _career_max_distance_diff_med,
    _career_min_distance_diff_med,
    _career_distance_range_med,
    _career_distance_count_med,
    _placed_max_distance_diff_med,
    _placed_min_distance_diff_med,
    _placed_distance_range_med,
    _placed_distance_count_med,
    _cha_training_index_med,
    _training_last_3f_med,
    _training_furlongs_med,
    _training_intensity_med,
    _training_count_med,
    _mare_race_count_med,
    _mare_avg_race_distance_med,
    _mare_max_race_distance_med,
    _mare_min_race_distance_med,
    _mare_distance_range_med,
    _mare_distance_diff_med,
    _mare_max_distance_diff_med,
    _mare_min_distance_diff_med,
    _mare_placed_race_count_med,
    _mare_placed_avg_distance_med,
    _mare_placed_max_distance_med,
    _mare_placed_min_distance_med,
    _mare_placed_distance_range_med,
    _mare_placed_max_distance_diff_med,
    _mare_placed_min_distance_diff_med,
    _mare_place_rate_med,
    _mare_turf_place_rate_med,
    _mare_dirt_place_rate_med,
    _mare_venue_place_rate_med,
    _mare_distance_band_place_rate_med,
    _mare_distance_place_rate_med,
    _mare_direction_place_rate_med,
    _mare_course_type_venue_place_rate_med,
    _mare_course_type_distance_band_place_rate_med,
    _mare_turf_place_diff_med,
    _mare_venue_place_rate_diff_med,
    _mare_distance_band_place_rate_diff_med,
    _mare_distance_place_rate_diff_med,
    _mare_direction_place_rate_diff_med,
    _mare_course_type_venue_place_rate_diff_med,
    _mare_course_type_distance_band_place_rate_diff_med,
    _mare_early_career_place_rate_med,
    _mare_late_career_place_rate_med,
    _mare_precocity_index_med,
    _prev1_course_bias_score_med,
    _prev2_course_bias_score_med,
    _prev3_course_bias_score_med,
    _prev4_course_bias_score_med,
    _prev5_course_bias_score_med,
    _gate_bias_score_med,
    _straight_bias_range_med,
    _course_position_bias_risk_med,
    _idm_zone_neutral_1_med,
    _idm_zone_neutral_2_med,
    _idm_zone_neutral_3_med,
    _idm_zone_neutral_4_med,
    _idm_zone_neutral_5_med,
    _idm_zone_neutral_trend_med,
    _course_pace_score_med,
    _gate_style_advantage_score_med,
    _gate_style_course_te_med,
    _last3f_rank_improvement_3_med,
    _last3f_rank_avg_3_med
  ) replace (
  -- NULL補完: 同一レース内の中央値で補完し、全員NULLの場合はフォールバック値を使用（Issue #330）
  -- TE系（低頻度マスクによりNULLになる列）: 同一レース内中央値 → フォールバック0.22（グローバル複勝率）
  coalesce(jockey_te, _jockey_te_med, 0.22) as jockey_te,
  coalesce(jockey_course_type_te, _jockey_course_type_te_med, 0.22) as jockey_course_type_te,
  coalesce(jockey_venue_te, _jockey_venue_te_med, 0.22) as jockey_venue_te,
  coalesce(jockey_distance_band_te, _jockey_distance_band_te_med, 0.22) as jockey_distance_band_te,
  coalesce(jockey_distance_te, _jockey_distance_te_med, 0.22) as jockey_distance_te,
  coalesce(jockey_direction_te, _jockey_direction_te_med, 0.22) as jockey_direction_te,
  coalesce(jockey_course_type_venue_te, _jockey_course_type_venue_te_med, 0.22) as jockey_course_type_venue_te,
  coalesce(jockey_course_type_distance_te, _jockey_course_type_distance_te_med, 0.22) as jockey_course_type_distance_te,
  coalesce(jockey_course_type_distance_venue_te, _jockey_course_type_distance_venue_te_med, 0.22) as jockey_course_type_distance_venue_te,
  coalesce(jockey_course_type_te_diff, _jockey_course_type_te_diff_med, 0.0) as jockey_course_type_te_diff,
  coalesce(jockey_venue_te_diff, _jockey_venue_te_diff_med, 0.0) as jockey_venue_te_diff,
  coalesce(jockey_distance_band_te_diff, _jockey_distance_band_te_diff_med, 0.0) as jockey_distance_band_te_diff,
  coalesce(jockey_distance_te_diff, _jockey_distance_te_diff_med, 0.0) as jockey_distance_te_diff,
  coalesce(jockey_direction_te_diff, _jockey_direction_te_diff_med, 0.0) as jockey_direction_te_diff,
  coalesce(jockey_course_type_venue_te_diff, _jockey_course_type_venue_te_diff_med, 0.0) as jockey_course_type_venue_te_diff,
  coalesce(jockey_course_type_distance_te_diff, _jockey_course_type_distance_te_diff_med, 0.0) as jockey_course_type_distance_te_diff,
  coalesce(jockey_course_type_distance_venue_te_diff, _jockey_course_type_distance_venue_te_diff_med, 0.0) as jockey_course_type_distance_venue_te_diff,
  coalesce(jockey_horse_combo_te, _jockey_horse_combo_te_med, 0.22) as jockey_horse_combo_te,
  coalesce(is_regular_jockey, 0) as is_regular_jockey,
  coalesce(jockey_change_type, 0) as jockey_change_type,
  coalesce(trainer_te, _trainer_te_med, 0.22) as trainer_te,
  coalesce(trainer_course_type_te, _trainer_course_type_te_med, 0.22) as trainer_course_type_te,
  coalesce(trainer_venue_te, _trainer_venue_te_med, 0.22) as trainer_venue_te,
  coalesce(trainer_distance_band_te, _trainer_distance_band_te_med, 0.22) as trainer_distance_band_te,
  coalesce(trainer_distance_te, _trainer_distance_te_med, 0.22) as trainer_distance_te,
  coalesce(trainer_direction_te, _trainer_direction_te_med, 0.22) as trainer_direction_te,
  coalesce(trainer_course_type_venue_te, _trainer_course_type_venue_te_med, 0.22) as trainer_course_type_venue_te,
  coalesce(trainer_course_type_distance_te, _trainer_course_type_distance_te_med, 0.22) as trainer_course_type_distance_te,
  coalesce(trainer_course_type_distance_venue_te, _trainer_course_type_distance_venue_te_med, 0.22) as trainer_course_type_distance_venue_te,
  coalesce(trainer_course_type_te_diff, _trainer_course_type_te_diff_med, 0.0) as trainer_course_type_te_diff,
  coalesce(trainer_venue_te_diff, _trainer_venue_te_diff_med, 0.0) as trainer_venue_te_diff,
  coalesce(trainer_distance_band_te_diff, _trainer_distance_band_te_diff_med, 0.0) as trainer_distance_band_te_diff,
  coalesce(trainer_distance_te_diff, _trainer_distance_te_diff_med, 0.0) as trainer_distance_te_diff,
  coalesce(trainer_direction_te_diff, _trainer_direction_te_diff_med, 0.0) as trainer_direction_te_diff,
  coalesce(trainer_course_type_venue_te_diff, _trainer_course_type_venue_te_diff_med, 0.0) as trainer_course_type_venue_te_diff,
  coalesce(trainer_course_type_distance_te_diff, _trainer_course_type_distance_te_diff_med, 0.0) as trainer_course_type_distance_te_diff,
  coalesce(trainer_course_type_distance_venue_te_diff, _trainer_course_type_distance_venue_te_diff_med, 0.0) as trainer_course_type_distance_venue_te_diff,
  coalesce(sire_te, _sire_te_med, 0.22) as sire_te,
  coalesce(sire_course_type_te, _sire_course_type_te_med, 0.22) as sire_course_type_te,
  coalesce(sire_venue_te, _sire_venue_te_med, 0.22) as sire_venue_te,
  coalesce(sire_distance_band_te, _sire_distance_band_te_med, 0.22) as sire_distance_band_te,
  coalesce(sire_distance_te, _sire_distance_te_med, 0.22) as sire_distance_te,
  coalesce(sire_direction_te, _sire_direction_te_med, 0.22) as sire_direction_te,
  coalesce(sire_course_type_venue_te, _sire_course_type_venue_te_med, 0.22) as sire_course_type_venue_te,
  coalesce(sire_course_type_distance_te, _sire_course_type_distance_te_med, 0.22) as sire_course_type_distance_te,
  coalesce(sire_course_type_distance_venue_te, _sire_course_type_distance_venue_te_med, 0.22) as sire_course_type_distance_venue_te,
  coalesce(sire_course_type_te_diff, _sire_course_type_te_diff_med, 0.0) as sire_course_type_te_diff,
  coalesce(sire_venue_te_diff, _sire_venue_te_diff_med, 0.0) as sire_venue_te_diff,
  coalesce(sire_distance_band_te_diff, _sire_distance_band_te_diff_med, 0.0) as sire_distance_band_te_diff,
  coalesce(sire_distance_te_diff, _sire_distance_te_diff_med, 0.0) as sire_distance_te_diff,
  coalesce(sire_direction_te_diff, _sire_direction_te_diff_med, 0.0) as sire_direction_te_diff,
  coalesce(sire_course_type_venue_te_diff, _sire_course_type_venue_te_diff_med, 0.0) as sire_course_type_venue_te_diff,
  coalesce(sire_course_type_distance_te_diff, _sire_course_type_distance_te_diff_med, 0.0) as sire_course_type_distance_te_diff,
  coalesce(sire_course_type_distance_venue_te_diff, _sire_course_type_distance_venue_te_diff_med, 0.0) as sire_course_type_distance_venue_te_diff,
  coalesce(sire_age2_te, _sire_age2_te_med, 0.22) as sire_age2_te,
  coalesce(sire_age3_te, _sire_age3_te_med, 0.22) as sire_age3_te,
  coalesce(sire_age4_te, _sire_age4_te_med, 0.22) as sire_age4_te,
  coalesce(sire_age5plus_te, _sire_age5plus_te_med, 0.22) as sire_age5plus_te,
  coalesce(sire_current_age_te, _sire_current_age_te_med, 0.22) as sire_current_age_te,
  coalesce(sire_precocity_diff, _sire_precocity_diff_med, 0.0) as sire_precocity_diff,
  coalesce(sire_age_vs_career_diff, _sire_age_vs_career_diff_med, 0.0) as sire_age_vs_career_diff,
  coalesce(sire_course_type_run_ratio, _sire_course_type_run_ratio_med, 0.5) as sire_course_type_run_ratio,
  coalesce(sire_venue_run_ratio, _sire_venue_run_ratio_med, 0.0) as sire_venue_run_ratio,
  coalesce(sire_distance_band_run_ratio, _sire_distance_band_run_ratio_med, 0.25) as sire_distance_band_run_ratio,
  coalesce(sire_distance_run_ratio, _sire_distance_run_ratio_med, 0.0) as sire_distance_run_ratio,
  coalesce(horse_te, _horse_te_med, 0.22) as horse_te,
  coalesce(horse_course_type_te, _horse_course_type_te_med, 0.22) as horse_course_type_te,
  coalesce(horse_venue_te, _horse_venue_te_med, 0.22) as horse_venue_te,
  coalesce(horse_distance_band_te, _horse_distance_band_te_med, 0.22) as horse_distance_band_te,
  coalesce(horse_distance_te, _horse_distance_te_med, 0.22) as horse_distance_te,
  coalesce(horse_direction_te, _horse_direction_te_med, 0.22) as horse_direction_te,
  coalesce(horse_jockey_te, _horse_jockey_te_med, 0.22) as horse_jockey_te,
  coalesce(horse_season_te, _horse_season_te_med, 0.22) as horse_season_te,
  coalesce(horse_course_type_venue_te, _horse_course_type_venue_te_med, 0.22) as horse_course_type_venue_te,
  coalesce(horse_course_type_distance_te, _horse_course_type_distance_te_med, 0.22) as horse_course_type_distance_te,
  coalesce(horse_course_type_distance_venue_te, _horse_course_type_distance_venue_te_med, 0.22) as horse_course_type_distance_venue_te,
  coalesce(horse_distance_change_te, _horse_distance_change_te_med, 0.22) as horse_distance_change_te,
  coalesce(horse_weight_carried_change_te, _horse_weight_carried_change_te_med, 0.22) as horse_weight_carried_change_te,
  coalesce(horse_course_type_te_diff, _horse_course_type_te_diff_med, 0.0) as horse_course_type_te_diff,
  coalesce(horse_venue_te_diff, _horse_venue_te_diff_med, 0.0) as horse_venue_te_diff,
  coalesce(horse_distance_band_te_diff, _horse_distance_band_te_diff_med, 0.0) as horse_distance_band_te_diff,
  coalesce(horse_distance_te_diff, _horse_distance_te_diff_med, 0.0) as horse_distance_te_diff,
  coalesce(horse_direction_te_diff, _horse_direction_te_diff_med, 0.0) as horse_direction_te_diff,
  coalesce(horse_jockey_te_diff, _horse_jockey_te_diff_med, 0.0) as horse_jockey_te_diff,
  coalesce(horse_season_te_diff, _horse_season_te_diff_med, 0.0) as horse_season_te_diff,
  coalesce(horse_course_type_venue_te_diff, _horse_course_type_venue_te_diff_med, 0.0) as horse_course_type_venue_te_diff,
  coalesce(horse_course_type_distance_te_diff, _horse_course_type_distance_te_diff_med, 0.0) as horse_course_type_distance_te_diff,
  coalesce(horse_course_type_distance_venue_te_diff, _horse_course_type_distance_venue_te_diff_med, 0.0) as horse_course_type_distance_venue_te_diff,
  coalesce(horse_distance_change_te_diff, _horse_distance_change_te_diff_med, 0.0) as horse_distance_change_te_diff,
  coalesce(horse_weight_carried_change_te_diff, _horse_weight_carried_change_te_diff_med, 0.0) as horse_weight_carried_change_te_diff,
  coalesce(horse_course_type_te_diff_avg, _horse_course_type_te_diff_avg_med, 0.0) as horse_course_type_te_diff_avg,
  coalesce(horse_venue_te_diff_avg, _horse_venue_te_diff_avg_med, 0.0) as horse_venue_te_diff_avg,
  coalesce(horse_distance_band_te_diff_avg, _horse_distance_band_te_diff_avg_med, 0.0) as horse_distance_band_te_diff_avg,
  coalesce(horse_distance_te_diff_avg, _horse_distance_te_diff_avg_med, 0.0) as horse_distance_te_diff_avg,
  coalesce(horse_direction_te_diff_avg, _horse_direction_te_diff_avg_med, 0.0) as horse_direction_te_diff_avg,
  coalesce(horse_jockey_te_diff_avg, _horse_jockey_te_diff_avg_med, 0.0) as horse_jockey_te_diff_avg,
  coalesce(horse_season_te_diff_avg, _horse_season_te_diff_avg_med, 0.0) as horse_season_te_diff_avg,
  coalesce(horse_cv_te_diff_avg, _horse_cv_te_diff_avg_med, 0.0) as horse_cv_te_diff_avg,
  coalesce(horse_cd_te_diff_avg, _horse_cd_te_diff_avg_med, 0.0) as horse_cd_te_diff_avg,
  coalesce(horse_cdv_te_diff_avg, _horse_cdv_te_diff_avg_med, 0.0) as horse_cdv_te_diff_avg,
  coalesce(horse_dc_te_diff_avg, _horse_dc_te_diff_avg_med, 0.0) as horse_dc_te_diff_avg,
  coalesce(horse_wcc_te_diff_avg, _horse_wcc_te_diff_avg_med, 0.0) as horse_wcc_te_diff_avg,
  coalesce(horse_course_type_te_diff_rank_avg, _horse_course_type_te_diff_rank_avg_med, 0.0) as horse_course_type_te_diff_rank_avg,
  coalesce(horse_venue_te_diff_rank_avg, _horse_venue_te_diff_rank_avg_med, 0.0) as horse_venue_te_diff_rank_avg,
  coalesce(horse_distance_band_te_diff_rank_avg, _horse_distance_band_te_diff_rank_avg_med, 0.0) as horse_distance_band_te_diff_rank_avg,
  coalesce(horse_distance_te_diff_rank_avg, _horse_distance_te_diff_rank_avg_med, 0.0) as horse_distance_te_diff_rank_avg,
  coalesce(horse_direction_te_diff_rank_avg, _horse_direction_te_diff_rank_avg_med, 0.0) as horse_direction_te_diff_rank_avg,
  coalesce(horse_jockey_te_diff_rank_avg, _horse_jockey_te_diff_rank_avg_med, 0.0) as horse_jockey_te_diff_rank_avg,
  coalesce(horse_season_te_diff_rank_avg, _horse_season_te_diff_rank_avg_med, 0.0) as horse_season_te_diff_rank_avg,
  coalesce(horse_cv_te_diff_rank_avg, _horse_cv_te_diff_rank_avg_med, 0.0) as horse_cv_te_diff_rank_avg,
  coalesce(horse_cd_te_diff_rank_avg, _horse_cd_te_diff_rank_avg_med, 0.0) as horse_cd_te_diff_rank_avg,
  coalesce(horse_cdv_te_diff_rank_avg, _horse_cdv_te_diff_rank_avg_med, 0.0) as horse_cdv_te_diff_rank_avg,
  coalesce(horse_dc_te_diff_rank_avg, _horse_dc_te_diff_rank_avg_med, 0.0) as horse_dc_te_diff_rank_avg,
  coalesce(horse_wcc_te_diff_rank_avg, _horse_wcc_te_diff_rank_avg_med, 0.0) as horse_wcc_te_diff_rank_avg,
  coalesce(mare_te, _mare_te_med, 0.22) as mare_te,
  coalesce(mare_course_type_te, _mare_course_type_te_med, 0.22) as mare_course_type_te,
  coalesce(mare_venue_te, _mare_venue_te_med, 0.22) as mare_venue_te,
  coalesce(mare_distance_band_te, _mare_distance_band_te_med, 0.22) as mare_distance_band_te,
  coalesce(mare_distance_te, _mare_distance_te_med, 0.22) as mare_distance_te,
  coalesce(mare_direction_te, _mare_direction_te_med, 0.22) as mare_direction_te,
  coalesce(mare_course_type_venue_te, _mare_course_type_venue_te_med, 0.22) as mare_course_type_venue_te,
  coalesce(mare_course_type_distance_te, _mare_course_type_distance_te_med, 0.22) as mare_course_type_distance_te,
  coalesce(mare_course_type_distance_venue_te, _mare_course_type_distance_venue_te_med, 0.22) as mare_course_type_distance_venue_te,
  coalesce(mare_course_type_te_diff, _mare_course_type_te_diff_med, 0.0) as mare_course_type_te_diff,
  coalesce(mare_venue_te_diff, _mare_venue_te_diff_med, 0.0) as mare_venue_te_diff,
  coalesce(mare_distance_band_te_diff, _mare_distance_band_te_diff_med, 0.0) as mare_distance_band_te_diff,
  coalesce(mare_distance_te_diff, _mare_distance_te_diff_med, 0.0) as mare_distance_te_diff,
  coalesce(mare_direction_te_diff, _mare_direction_te_diff_med, 0.0) as mare_direction_te_diff,
  coalesce(mare_course_type_venue_te_diff, _mare_course_type_venue_te_diff_med, 0.0) as mare_course_type_venue_te_diff,
  coalesce(mare_course_type_distance_te_diff, _mare_course_type_distance_te_diff_med, 0.0) as mare_course_type_distance_te_diff,
  coalesce(mare_course_type_distance_venue_te_diff, _mare_course_type_distance_venue_te_diff_med, 0.0) as mare_course_type_distance_venue_te_diff,
  coalesce(mare_age2_te, _mare_age2_te_med, 0.22) as mare_age2_te,
  coalesce(mare_age3_te, _mare_age3_te_med, 0.22) as mare_age3_te,
  coalesce(mare_age4_te, _mare_age4_te_med, 0.22) as mare_age4_te,
  coalesce(mare_age5plus_te, _mare_age5plus_te_med, 0.22) as mare_age5plus_te,
  coalesce(mare_current_age_te, _mare_current_age_te_med, 0.22) as mare_current_age_te,
  coalesce(mare_precocity_diff, _mare_precocity_diff_med, 0.0) as mare_precocity_diff,
  coalesce(mare_age_vs_career_diff, _mare_age_vs_career_diff_med, 0.0) as mare_age_vs_career_diff,
  coalesce(mare_course_type_run_ratio, _mare_course_type_run_ratio_med, 0.5) as mare_course_type_run_ratio,
  coalesce(mare_venue_run_ratio, _mare_venue_run_ratio_med, 0.0) as mare_venue_run_ratio,
  coalesce(mare_distance_band_run_ratio, _mare_distance_band_run_ratio_med, 0.25) as mare_distance_band_run_ratio,
  coalesce(mare_distance_run_ratio, _mare_distance_run_ratio_med, 0.0) as mare_distance_run_ratio,
  -- 過去走特徴量（デビュー馬・初出走条件の馬でNULLになる列）: 同一レース内中央値 → フォールバック0
  coalesce(idm_1, _idm_1_med, 0) as idm_1,
  coalesce(finish_position_1, _finish_position_1_med, 0) as finish_position_1,
  coalesce(finish_position_rate_1, _finish_position_rate_1_med, 0) as finish_position_rate_1,
  coalesce(win_odds_1, _win_odds_1_med, 0) as win_odds_1,
  coalesce(win_popularity_1, _win_popularity_1_med, 0) as win_popularity_1,
  coalesce(popularity_rate_1, _popularity_rate_1_med, 0) as popularity_rate_1,
  coalesce(upside_rate_1, _upside_rate_1_med, 0) as upside_rate_1,
  coalesce(finish_time_1, _finish_time_1_med, 0) as finish_time_1,
  coalesce(last_3f_1, _last_3f_1_med, 0) as last_3f_1,
  coalesce(last_3f_rank_in_race_1, _last_3f_rank_in_race_1_med, 0) as last_3f_rank_in_race_1,
  coalesce(race_date_diff_1, _race_date_diff_1_med, 0) as race_date_diff_1,
  coalesce(race_date_diff_2, _race_date_diff_2_med, 0) as race_date_diff_2,
  coalesce(race_date_diff_3, _race_date_diff_3_med, 0) as race_date_diff_3,
  coalesce(race_date_diff_4, _race_date_diff_4_med, 0) as race_date_diff_4,
  coalesce(race_date_diff_5, _race_date_diff_5_med, 0) as race_date_diff_5,
  coalesce(mean_idm, _mean_idm_med, 0) as mean_idm,
  coalesce(ema_idm, _ema_idm_med, 0) as ema_idm,
  coalesce(max_idm, _max_idm_med, 0) as max_idm,
  coalesce(min_idm, _min_idm_med, 0) as min_idm,
  coalesce(idm_diff, _idm_diff_med, 0) as idm_diff,
  coalesce(mean_idm_diff, _mean_idm_diff_med, 0) as mean_idm_diff,
  coalesce(ema_idm_diff, _ema_idm_diff_med, 0) as ema_idm_diff,
  coalesce(max_idm_diff, _max_idm_diff_med, 0) as max_idm_diff,
  coalesce(mean_finish_position_rate, _mean_finish_position_rate_med, 0) as mean_finish_position_rate,
  coalesce(ema_finish_position_rate, _ema_finish_position_rate_med, 0) as ema_finish_position_rate,
  coalesce(max_finish_position_rate, _max_finish_position_rate_med, 0) as max_finish_position_rate,
  coalesce(min_finish_position_rate, _min_finish_position_rate_med, 0) as min_finish_position_rate,
  coalesce(mean_popularity_rate, _mean_popularity_rate_med, 0) as mean_popularity_rate,
  coalesce(ema_popularity_rate, _ema_popularity_rate_med, 0) as ema_popularity_rate,
  coalesce(max_popularity_rate, _max_popularity_rate_med, 0) as max_popularity_rate,
  coalesce(min_popularity_rate, _min_popularity_rate_med, 0) as min_popularity_rate,
  coalesce(mean_upside_rate, _mean_upside_rate_med, 0) as mean_upside_rate,
  coalesce(ema_upside_rate, _ema_upside_rate_med, 0) as ema_upside_rate,
  coalesce(max_upside_rate, _max_upside_rate_med, 0) as max_upside_rate,
  coalesce(min_upside_rate, _min_upside_rate_med, 0) as min_upside_rate,
  coalesce(finish_time_normalized, _finish_time_normalized_med, 0) as finish_time_normalized,
  coalesce(last_3f_normalized, _last_3f_normalized_med, 0) as last_3f_normalized,
  coalesce(idm_trend_3, _idm_trend_3_med, 0) as idm_trend_3,
  coalesce(finish_position_trend_3, _finish_position_trend_3_med, 0) as finish_position_trend_3,
  coalesce(mean_corner_gain_1to4, _mean_corner_gain_1to4_med, 0) as mean_corner_gain_1to4,
  coalesce(ema_corner_gain_1to4, _ema_corner_gain_1to4_med, 0) as ema_corner_gain_1to4,
  coalesce(corner1_to_finish_delta_prev_1, _corner1_to_finish_delta_prev_1_med, 0) as corner1_to_finish_delta_prev_1,
  -- 距離帯別・距離別特徴量（出走歴なしでNULLになる列）
  coalesce(distance_band_top3_finish_rate, _distance_band_top3_finish_rate_med, 0) as distance_band_top3_finish_rate,
  coalesce(distance_band_top1_finish_rate, _distance_band_top1_finish_rate_med, 0) as distance_band_top1_finish_rate,
  coalesce(distance_band_rate_diff, _distance_band_rate_diff_med, 0) as distance_band_rate_diff,
  coalesce(distance_top3_finish_rate, _distance_top3_finish_rate_med, 0) as distance_top3_finish_rate,
  coalesce(distance_top1_finish_rate, _distance_top1_finish_rate_med, 0) as distance_top1_finish_rate,
  coalesce(distance_rate_diff, _distance_rate_diff_med, 0) as distance_rate_diff,
  -- キャリア最長・最短距離特徴量（デビュー馬でNULLになる列）
  coalesce(career_max_distance_diff, _career_max_distance_diff_med, 0) as career_max_distance_diff,
  coalesce(career_min_distance_diff, _career_min_distance_diff_med, 0) as career_min_distance_diff,
  coalesce(career_distance_range, _career_distance_range_med, 0) as career_distance_range,
  coalesce(career_distance_count, _career_distance_count_med, 0) as career_distance_count,
  coalesce(placed_max_distance_diff, _placed_max_distance_diff_med, 0) as placed_max_distance_diff,
  coalesce(placed_min_distance_diff, _placed_min_distance_diff_med, 0) as placed_min_distance_diff,
  coalesce(placed_distance_range, _placed_distance_range_med, 0) as placed_distance_range,
  coalesce(placed_distance_count, _placed_distance_count_med, 0) as placed_distance_count,
  -- 調教特徴量（CHAデータ未取得馬でNULLになる列）
  coalesce(cha_training_index, _cha_training_index_med, 0) as cha_training_index,
  coalesce(training_last_3f, _training_last_3f_med, 0) as training_last_3f,
  coalesce(training_furlongs, _training_furlongs_med, 0) as training_furlongs,
  coalesce(training_intensity, _training_intensity_med, 0) as training_intensity,
  coalesce(training_count, _training_count_med, 0) as training_count,
  -- 母馬実績特徴量（血統情報なし・実績なしでNULLになる列）
  coalesce(mare_race_count, _mare_race_count_med, 0) as mare_race_count,
  coalesce(mare_avg_race_distance, _mare_avg_race_distance_med, 0) as mare_avg_race_distance,
  coalesce(mare_max_race_distance, _mare_max_race_distance_med, 0) as mare_max_race_distance,
  coalesce(mare_min_race_distance, _mare_min_race_distance_med, 0) as mare_min_race_distance,
  coalesce(mare_distance_range, _mare_distance_range_med, 0) as mare_distance_range,
  coalesce(mare_distance_diff, _mare_distance_diff_med, 0) as mare_distance_diff,
  coalesce(mare_max_distance_diff, _mare_max_distance_diff_med, 0) as mare_max_distance_diff,
  coalesce(mare_min_distance_diff, _mare_min_distance_diff_med, 0) as mare_min_distance_diff,
  coalesce(mare_placed_race_count, _mare_placed_race_count_med, 0) as mare_placed_race_count,
  coalesce(mare_placed_avg_distance, _mare_placed_avg_distance_med, 0) as mare_placed_avg_distance,
  coalesce(mare_placed_max_distance, _mare_placed_max_distance_med, 0) as mare_placed_max_distance,
  coalesce(mare_placed_min_distance, _mare_placed_min_distance_med, 0) as mare_placed_min_distance,
  coalesce(mare_placed_distance_range, _mare_placed_distance_range_med, 0) as mare_placed_distance_range,
  coalesce(mare_placed_max_distance_diff, _mare_placed_max_distance_diff_med, 0) as mare_placed_max_distance_diff,
  coalesce(mare_placed_min_distance_diff, _mare_placed_min_distance_diff_med, 0) as mare_placed_min_distance_diff,
  coalesce(mare_place_rate, _mare_place_rate_med, 0) as mare_place_rate,
  coalesce(mare_turf_place_rate, _mare_turf_place_rate_med, 0) as mare_turf_place_rate,
  coalesce(mare_dirt_place_rate, _mare_dirt_place_rate_med, 0) as mare_dirt_place_rate,
  coalesce(mare_venue_place_rate, _mare_venue_place_rate_med, 0) as mare_venue_place_rate,
  coalesce(mare_distance_band_place_rate, _mare_distance_band_place_rate_med, 0) as mare_distance_band_place_rate,
  coalesce(mare_distance_place_rate, _mare_distance_place_rate_med, 0) as mare_distance_place_rate,
  coalesce(mare_direction_place_rate, _mare_direction_place_rate_med, 0) as mare_direction_place_rate,
  coalesce(mare_course_type_venue_place_rate, _mare_course_type_venue_place_rate_med, 0) as mare_course_type_venue_place_rate,
  coalesce(mare_course_type_distance_band_place_rate, _mare_course_type_distance_band_place_rate_med, 0) as mare_course_type_distance_band_place_rate,
  coalesce(mare_turf_place_diff, _mare_turf_place_diff_med, 0) as mare_turf_place_diff,
  coalesce(mare_venue_place_rate_diff, _mare_venue_place_rate_diff_med, 0) as mare_venue_place_rate_diff,
  coalesce(mare_distance_band_place_rate_diff, _mare_distance_band_place_rate_diff_med, 0) as mare_distance_band_place_rate_diff,
  coalesce(mare_distance_place_rate_diff, _mare_distance_place_rate_diff_med, 0) as mare_distance_place_rate_diff,
  coalesce(mare_direction_place_rate_diff, _mare_direction_place_rate_diff_med, 0) as mare_direction_place_rate_diff,
  coalesce(mare_course_type_venue_place_rate_diff, _mare_course_type_venue_place_rate_diff_med, 0) as mare_course_type_venue_place_rate_diff,
  coalesce(mare_course_type_distance_band_place_rate_diff, _mare_course_type_distance_band_place_rate_diff_med, 0) as mare_course_type_distance_band_place_rate_diff,
  coalesce(mare_early_career_place_rate, _mare_early_career_place_rate_med, 0) as mare_early_career_place_rate,
  coalesce(mare_late_career_place_rate, _mare_late_career_place_rate_med, 0) as mare_late_career_place_rate,
  coalesce(mare_precocity_index, _mare_precocity_index_med, 0) as mare_precocity_index,
  -- バイアス補正特徴量（前走データなし・コース取り不明でNULLになる列）
  coalesce(prev1_course_bias_score, _prev1_course_bias_score_med, 0) as prev1_course_bias_score,
  coalesce(prev2_course_bias_score, _prev2_course_bias_score_med, 0) as prev2_course_bias_score,
  coalesce(prev3_course_bias_score, _prev3_course_bias_score_med, 0) as prev3_course_bias_score,
  coalesce(prev4_course_bias_score, _prev4_course_bias_score_med, 0) as prev4_course_bias_score,
  coalesce(prev5_course_bias_score, _prev5_course_bias_score_med, 0) as prev5_course_bias_score,
  coalesce(gate_bias_score, _gate_bias_score_med, 0) as gate_bias_score,
  coalesce(straight_bias_range, _straight_bias_range_med, 0) as straight_bias_range,
  coalesce(course_position_bias_risk, _course_position_bias_risk_med, 0) as course_position_bias_risk,
  coalesce(idm_zone_neutral_1, _idm_zone_neutral_1_med, 0) as idm_zone_neutral_1,
  coalesce(idm_zone_neutral_2, _idm_zone_neutral_2_med, 0) as idm_zone_neutral_2,
  coalesce(idm_zone_neutral_3, _idm_zone_neutral_3_med, 0) as idm_zone_neutral_3,
  coalesce(idm_zone_neutral_4, _idm_zone_neutral_4_med, 0) as idm_zone_neutral_4,
  coalesce(idm_zone_neutral_5, _idm_zone_neutral_5_med, 0) as idm_zone_neutral_5,
  coalesce(idm_zone_neutral_trend, _idm_zone_neutral_trend_med, 0) as idm_zone_neutral_trend,
  coalesce(course_pace_score, _course_pace_score_med, 2.5) as course_pace_score,
  coalesce(gate_style_advantage_score, _gate_style_advantage_score_med, 0.0) as gate_style_advantage_score,
  coalesce(gate_style_course_te, _gate_style_course_te_med, 0.22) as gate_style_course_te,
  coalesce(last3f_rank_improvement_3, _last3f_rank_improvement_3_med, 0) as last3f_rank_improvement_3,
  coalesce(last3f_rank_avg_3, _last3f_rank_avg_3_med, 0) as last3f_rank_avg_3
  )
  from temp_null_fill_med
)

-- 同一レース内RANK特徴量を追加（Issue #333）
select *
  -- 騎手TE系 RANK（17列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_te DESC NULLS LAST) AS jockey_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_te DESC NULLS LAST) AS jockey_course_type_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_venue_te DESC NULLS LAST) AS jockey_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_distance_band_te DESC NULLS LAST) AS jockey_distance_band_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_distance_te DESC NULLS LAST) AS jockey_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_direction_te DESC NULLS LAST) AS jockey_direction_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_venue_te DESC NULLS LAST) AS jockey_course_type_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_distance_te DESC NULLS LAST) AS jockey_course_type_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_distance_venue_te DESC NULLS LAST) AS jockey_course_type_distance_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_te_diff DESC NULLS LAST) AS jockey_course_type_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_venue_te_diff DESC NULLS LAST) AS jockey_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_distance_band_te_diff DESC NULLS LAST) AS jockey_distance_band_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_distance_te_diff DESC NULLS LAST) AS jockey_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_direction_te_diff DESC NULLS LAST) AS jockey_direction_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_venue_te_diff DESC NULLS LAST) AS jockey_course_type_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_distance_te_diff DESC NULLS LAST) AS jockey_course_type_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_course_type_distance_venue_te_diff DESC NULLS LAST) AS jockey_course_type_distance_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY jockey_horse_combo_te DESC NULLS LAST) AS jockey_horse_combo_te_rank
  -- 調教師TE系 RANK（17列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_te DESC NULLS LAST) AS trainer_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_te DESC NULLS LAST) AS trainer_course_type_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_venue_te DESC NULLS LAST) AS trainer_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_distance_band_te DESC NULLS LAST) AS trainer_distance_band_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_distance_te DESC NULLS LAST) AS trainer_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_direction_te DESC NULLS LAST) AS trainer_direction_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_venue_te DESC NULLS LAST) AS trainer_course_type_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_distance_te DESC NULLS LAST) AS trainer_course_type_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_distance_venue_te DESC NULLS LAST) AS trainer_course_type_distance_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_te_diff DESC NULLS LAST) AS trainer_course_type_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_venue_te_diff DESC NULLS LAST) AS trainer_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_distance_band_te_diff DESC NULLS LAST) AS trainer_distance_band_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_distance_te_diff DESC NULLS LAST) AS trainer_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_direction_te_diff DESC NULLS LAST) AS trainer_direction_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_venue_te_diff DESC NULLS LAST) AS trainer_course_type_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_distance_te_diff DESC NULLS LAST) AS trainer_course_type_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY trainer_course_type_distance_venue_te_diff DESC NULLS LAST) AS trainer_course_type_distance_venue_te_diff_rank
  -- 種牡馬TE系 RANK（22列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_te DESC NULLS LAST) AS sire_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_te DESC NULLS LAST) AS sire_course_type_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_venue_te DESC NULLS LAST) AS sire_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_band_te DESC NULLS LAST) AS sire_distance_band_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_te DESC NULLS LAST) AS sire_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_direction_te DESC NULLS LAST) AS sire_direction_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_venue_te DESC NULLS LAST) AS sire_course_type_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_distance_te DESC NULLS LAST) AS sire_course_type_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_distance_venue_te DESC NULLS LAST) AS sire_course_type_distance_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_te_diff DESC NULLS LAST) AS sire_course_type_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_venue_te_diff DESC NULLS LAST) AS sire_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_band_te_diff DESC NULLS LAST) AS sire_distance_band_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_te_diff DESC NULLS LAST) AS sire_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_direction_te_diff DESC NULLS LAST) AS sire_direction_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_venue_te_diff DESC NULLS LAST) AS sire_course_type_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_distance_te_diff DESC NULLS LAST) AS sire_course_type_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_distance_venue_te_diff DESC NULLS LAST) AS sire_course_type_distance_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_age2_te DESC NULLS LAST) AS sire_age2_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_age3_te DESC NULLS LAST) AS sire_age3_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_age4_te DESC NULLS LAST) AS sire_age4_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_age5plus_te DESC NULLS LAST) AS sire_age5plus_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_current_age_te DESC NULLS LAST) AS sire_current_age_te_rank
  -- 母馬TE系 RANK（22列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_te DESC NULLS LAST) AS mare_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_te DESC NULLS LAST) AS mare_course_type_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_venue_te DESC NULLS LAST) AS mare_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_band_te DESC NULLS LAST) AS mare_distance_band_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_te DESC NULLS LAST) AS mare_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_direction_te DESC NULLS LAST) AS mare_direction_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_venue_te DESC NULLS LAST) AS mare_course_type_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_distance_te DESC NULLS LAST) AS mare_course_type_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_distance_venue_te DESC NULLS LAST) AS mare_course_type_distance_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_te_diff DESC NULLS LAST) AS mare_course_type_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_venue_te_diff DESC NULLS LAST) AS mare_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_band_te_diff DESC NULLS LAST) AS mare_distance_band_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_te_diff DESC NULLS LAST) AS mare_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_direction_te_diff DESC NULLS LAST) AS mare_direction_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_venue_te_diff DESC NULLS LAST) AS mare_course_type_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_distance_te_diff DESC NULLS LAST) AS mare_course_type_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_distance_venue_te_diff DESC NULLS LAST) AS mare_course_type_distance_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_age2_te DESC NULLS LAST) AS mare_age2_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_age3_te DESC NULLS LAST) AS mare_age3_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_age4_te DESC NULLS LAST) AS mare_age4_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_age5plus_te DESC NULLS LAST) AS mare_age5plus_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_current_age_te DESC NULLS LAST) AS mare_current_age_te_rank
  -- 馬自身TE系 RANK（25列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_te DESC NULLS LAST) AS horse_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_te DESC NULLS LAST) AS horse_course_type_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_venue_te DESC NULLS LAST) AS horse_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_band_te DESC NULLS LAST) AS horse_distance_band_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_te DESC NULLS LAST) AS horse_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_direction_te DESC NULLS LAST) AS horse_direction_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_jockey_te DESC NULLS LAST) AS horse_jockey_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_season_te DESC NULLS LAST) AS horse_season_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_venue_te DESC NULLS LAST) AS horse_course_type_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_distance_te DESC NULLS LAST) AS horse_course_type_distance_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_distance_venue_te DESC NULLS LAST) AS horse_course_type_distance_venue_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_change_te DESC NULLS LAST) AS horse_distance_change_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_weight_carried_change_te DESC NULLS LAST) AS horse_weight_carried_change_te_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_te_diff DESC NULLS LAST) AS horse_course_type_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_venue_te_diff DESC NULLS LAST) AS horse_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_band_te_diff DESC NULLS LAST) AS horse_distance_band_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_te_diff DESC NULLS LAST) AS horse_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_direction_te_diff DESC NULLS LAST) AS horse_direction_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_jockey_te_diff DESC NULLS LAST) AS horse_jockey_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_season_te_diff DESC NULLS LAST) AS horse_season_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_venue_te_diff DESC NULLS LAST) AS horse_course_type_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_distance_te_diff DESC NULLS LAST) AS horse_course_type_distance_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_course_type_distance_venue_te_diff DESC NULLS LAST) AS horse_course_type_distance_venue_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_distance_change_te_diff DESC NULLS LAST) AS horse_distance_change_te_diff_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY horse_weight_carried_change_te_diff DESC NULLS LAST) AS horse_weight_carried_change_te_diff_rank
  -- 出走比率系 RANK（sire 4列 + mare 4列 = 8列）
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_course_type_run_ratio DESC NULLS LAST) AS sire_course_type_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_venue_run_ratio DESC NULLS LAST) AS sire_venue_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_band_run_ratio DESC NULLS LAST) AS sire_distance_band_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY sire_distance_run_ratio DESC NULLS LAST) AS sire_distance_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_course_type_run_ratio DESC NULLS LAST) AS mare_course_type_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_venue_run_ratio DESC NULLS LAST) AS mare_venue_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_band_run_ratio DESC NULLS LAST) AS mare_distance_band_run_ratio_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY mare_distance_run_ratio DESC NULLS LAST) AS mare_distance_run_ratio_rank
  -- 近走上がり3F レース内相対順位 RANK（Issue #346、ASC=末脚が強い方が上位）
  ,RANK() OVER (PARTITION BY race_id ORDER BY last3f_rank_improvement_3 ASC NULLS LAST) AS last3f_rank_improvement_3_rank
  ,RANK() OVER (PARTITION BY race_id ORDER BY last3f_rank_avg_3 ASC NULLS LAST) AS last3f_rank_avg_3_rank
from temp_null_filled
