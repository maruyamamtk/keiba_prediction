/* entity_te_daily 日次TE計算クエリ
 *
 * 指定日（{target_date}）時点での各エンティティの Target Encoding 値を GROUP BY で計算する。
 * ウィンドウ関数の RANGE BETWEEN を使わず、WHERE 句で期間を絞ることで高速化。
 *
 * 出力: features.entity_te_daily に WRITE_APPEND で追記する
 *   entity_type, entity_id, condition_type, condition_key, as_of_date, cnt, sum_top3, sum_top1
 *
 * 注意:
 *   - {target_date} 当日のレースを除外する（race_date < '{target_date}'）
 *   - 騎手・調教師・種牡馬・馬は直近 1826 日（5年）の窓、母馬は全期間
 *   - 障害戦（course_type='obstacle'）は除外
 */

/* ========= 共通ベーステーブル（全エンティティ共通） ========= */
with history_all as (
  /* 全期間の実績（母馬TE用：UNBOUNDED PRECEDING に相当） */
  select
    h_r.horse_id
    ,h_r.jockey_code
    ,h_r.trainer_code
    ,h_r.weight_carried
    ,h_m.sire_name
    ,h_m.dam_name
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
    ,case when r_r.finish_position between 1 and 3 then 1 else 0 end as is_top3
    ,case when r_r.finish_position = 1 then 1 else 0 end as is_top1
    ,case
      when extract(month from r_i.race_date) in (3, 4, 5) then 'spring'
      when extract(month from r_i.race_date) in (6, 7, 8) then 'summer'
      when extract(month from r_i.race_date) in (9, 10, 11) then 'autumn'
      else 'winter'
    end as season
    ,date_diff(r_i.race_date, h_m.birth_date, year) as horse_age
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i
      on h_r.race_id = r_i.race_id
    inner join `{project_id}`.raw.horse_master as h_m
      on h_r.horse_id = h_m.horse_id
    left join `{project_id}`.raw.race_results as r_r
      on h_r.race_id = r_r.race_id
      and h_r.horse_number = r_r.horse_number
  where
    (r_r.finish_position > 0 or r_r.race_id is null)
    and r_i.race_date < date('{target_date}')
    and coalesce(r_i.course_type, '') != 'obstacle'
)
,history_window as (
  /* 直近 1826 日（5年）の実績（騎手・調教師・種牡馬・馬TE用） */
  select * from history_all
  where race_date >= date_sub(date('{target_date}'), interval 1826 day)
)
,history_with_change as (
  /* 距離変化・斤量変化タイプを計算（LAG ウィンドウ関数が必要だが、これは高速） */
  select
    *
    ,lag(distance) over (partition by horse_id order by race_date) as prev_distance
    ,lag(weight_carried) over (partition by horse_id order by race_date) as prev_weight_carried
  from history_window
)
,history_change as (
  /* 変化タイプを確定 */
  select
    *
    ,case
      when prev_distance is null then null
      when distance > prev_distance then 'extension'
      when distance < prev_distance then 'shortening'
      else 'same'
    end as distance_change_type
    ,case
      when prev_weight_carried is null then null
      when weight_carried > prev_weight_carried then 'increase'
      when weight_carried < prev_weight_carried then 'decrease'
      else 'same'
    end as weight_carried_change_type
  from history_with_change
)

/* ========= 騎手 Target Encoding（直近 1826 日） ========= */
-- base
select 'jockey' as entity_type, jockey_code as entity_id, 'base' as condition_type, '' as condition_key
  ,date('{target_date}') as as_of_date, count(*) as cnt, sum(is_top3) as sum_top3, null as sum_top1
from history_window group by jockey_code

union all select 'jockey', jockey_code, 'course_type', course_type, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, course_type

union all select 'jockey', jockey_code, 'venue', venue_code, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, venue_code

union all select 'jockey', jockey_code, 'distance_band', distance_band, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, distance_band

union all select 'jockey', jockey_code, 'distance', cast(distance as string), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, distance

union all select 'jockey', jockey_code, 'direction', direction, date('{target_date}'), count(*), sum(is_top3), null
from history_window where direction is not null group by jockey_code, direction

union all select 'jockey', jockey_code, 'cv', concat(course_type, '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, course_type, venue_code

union all select 'jockey', jockey_code, 'cd', concat(course_type, '_', cast(distance as string)), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, course_type, distance

union all select 'jockey', jockey_code, 'cdv', concat(course_type, '_', cast(distance as string), '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by jockey_code, course_type, distance, venue_code

/* ========= 調教師 Target Encoding（直近 1826 日） ========= */
union all select 'trainer', trainer_code, 'base', '', date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code

union all select 'trainer', trainer_code, 'course_type', course_type, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, course_type

union all select 'trainer', trainer_code, 'venue', venue_code, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, venue_code

union all select 'trainer', trainer_code, 'distance_band', distance_band, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, distance_band

union all select 'trainer', trainer_code, 'distance', cast(distance as string), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, distance

union all select 'trainer', trainer_code, 'direction', direction, date('{target_date}'), count(*), sum(is_top3), null
from history_window where direction is not null group by trainer_code, direction

union all select 'trainer', trainer_code, 'cv', concat(course_type, '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, course_type, venue_code

union all select 'trainer', trainer_code, 'cd', concat(course_type, '_', cast(distance as string)), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, course_type, distance

union all select 'trainer', trainer_code, 'cdv', concat(course_type, '_', cast(distance as string), '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by trainer_code, course_type, distance, venue_code

/* ========= 種牡馬 Target Encoding（直近 1826 日） ========= */
union all select 'sire', sire_name, 'base', '', date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name

union all select 'sire', sire_name, 'course_type', course_type, date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, course_type

union all select 'sire', sire_name, 'venue', venue_code, date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, venue_code

union all select 'sire', sire_name, 'distance_band', distance_band, date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, distance_band

union all select 'sire', sire_name, 'distance', cast(distance as string), date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, distance

union all select 'sire', sire_name, 'direction', direction, date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null and direction is not null group by sire_name, direction

union all select 'sire', sire_name, 'cv', concat(course_type, '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, course_type, venue_code

union all select 'sire', sire_name, 'cd', concat(course_type, '_', cast(distance as string)), date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, course_type, distance

union all select 'sire', sire_name, 'cdv', concat(course_type, '_', cast(distance as string), '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null group by sire_name, course_type, distance, venue_code

/* 種牡馬 年齢帯別 TE */
union all select 'sire', sire_name
  ,case when horse_age = 2 then 'age_band' else 'age_band' end as condition_type
  ,case
    when horse_age = 2 then '2yo'
    when horse_age = 3 then '3yo'
    when horse_age = 4 then '4yo'
    else '5plus'
  end as condition_key
  ,date('{target_date}'), count(*), sum(is_top3), null
from history_window where sire_name is not null
group by sire_name, case when horse_age = 2 then '2yo' when horse_age = 3 then '3yo' when horse_age = 4 then '4yo' else '5plus' end

/* ========= 母馬産駒 Target Encoding（全期間：UNBOUNDED PRECEDING に相当） ========= */
union all select 'mare', dam_name, 'base', '', date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name

union all select 'mare', dam_name, 'course_type', course_type, date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, course_type

union all select 'mare', dam_name, 'venue', venue_code, date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, venue_code

union all select 'mare', dam_name, 'distance_band', distance_band, date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, distance_band

union all select 'mare', dam_name, 'distance', cast(distance as string), date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, distance

union all select 'mare', dam_name, 'direction', direction, date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null and direction is not null group by dam_name, direction

union all select 'mare', dam_name, 'cv', concat(course_type, '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, course_type, venue_code

union all select 'mare', dam_name, 'cd', concat(course_type, '_', cast(distance as string)), date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, course_type, distance

union all select 'mare', dam_name, 'cdv', concat(course_type, '_', cast(distance as string), '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null group by dam_name, course_type, distance, venue_code

/* 母馬産駒 年齢帯別 TE（全期間） */
union all select 'mare', dam_name
  ,'age_band'
  ,case
    when horse_age = 2 then '2yo'
    when horse_age = 3 then '3yo'
    when horse_age = 4 then '4yo'
    else '5plus'
  end as condition_key
  ,date('{target_date}'), count(*), sum(is_top3), null
from history_all where dam_name is not null
group by dam_name, case when horse_age = 2 then '2yo' when horse_age = 3 then '3yo' when horse_age = 4 then '4yo' else '5plus' end

/* ========= 馬自身 Target Encoding（直近 1826 日） ========= */
union all select 'horse', horse_id, 'base', '', date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id

union all select 'horse', horse_id, 'course_type', course_type, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, course_type

union all select 'horse', horse_id, 'venue', venue_code, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, venue_code

union all select 'horse', horse_id, 'distance_band', distance_band, date('{target_date}'), count(*), sum(is_top3), cast(sum(is_top1) as int64)
from history_window group by horse_id, distance_band

union all select 'horse', horse_id, 'distance', cast(distance as string), date('{target_date}'), count(*), sum(is_top3), cast(sum(is_top1) as int64)
from history_window group by horse_id, distance

union all select 'horse', horse_id, 'direction', direction, date('{target_date}'), count(*), sum(is_top3), null
from history_window where direction is not null group by horse_id, direction

union all select 'horse', horse_id, 'cv', concat(course_type, '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, course_type, venue_code

union all select 'horse', horse_id, 'cd', concat(course_type, '_', cast(distance as string)), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, course_type, distance

union all select 'horse', horse_id, 'cdv', concat(course_type, '_', cast(distance as string), '_', venue_code), date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, course_type, distance, venue_code

/* 馬自身 × 騎手 TE */
union all select 'horse', horse_id, 'jockey', jockey_code, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, jockey_code

/* 馬自身 × シーズン TE */
union all select 'horse', horse_id, 'season', season, date('{target_date}'), count(*), sum(is_top3), null
from history_window group by horse_id, season

/* 馬自身 × 距離変化 TE */
union all select 'horse', horse_id, 'distance_change', distance_change_type, date('{target_date}'), count(*), sum(is_top3), null
from history_change where distance_change_type is not null group by horse_id, distance_change_type

/* 馬自身 × 斤量変化 TE */
union all select 'horse', horse_id, 'weight_carried_change', weight_carried_change_type, date('{target_date}'), count(*), sum(is_top3), null
from history_change where weight_carried_change_type is not null group by horse_id, weight_carried_change_type
