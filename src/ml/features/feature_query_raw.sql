/* race_idごとの出走頭数を馬番の最大値から算出 */
with temp_race_horse_count as (
  select
    race_id
    ,max(horse_number) as num_horses
  -- race_resultsを参照すると、当日の出走情報の取得が誤っているときに意図しない挙動となるため、horse_resultsを参照して出走頭数を算出する
  from `{project_id}`.raw.horse_results
  group by race_id
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
    ,r_r_1.disadvantage as disadvantage_1
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
    ,r_r_2.disadvantage as disadvantage_2
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
    ,r_r_3.disadvantage as disadvantage_3
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
    ,r_r_4.disadvantage as disadvantage_4
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
    ,r_r_5.disadvantage as disadvantage_5
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

select
  t_p_r_f.*
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
from
  temp_past_race_features2 as t_p_r_f
  left join temp_horse_master_feature2 as t_h_m_f
    on t_p_r_f.race_id = t_h_m_f.race_id
    and t_p_r_f.horse_number = t_h_m_f.horse_number
