/* TEスムージング用グローバル平均（当日予測向け軽量版）
   当日レースを除く直近 1826 日のレース結果から全体3着以内率を計算する */
,temp_global_mean_te as (
  select
    avg(case when r_r.finish_position between 1 and 3 then 1.0 else 0.0 end) as global_top3_rate
  from `{project_id}`.raw.race_results as r_r
    inner join `{project_id}`.raw.race_info as r_i on r_r.race_id = r_i.race_id
  where r_r.finish_position > 0
    and r_i.race_date < date('{target_date}')
    and date_diff(date('{target_date}'), r_i.race_date, day) <= 1826
)

/* entity_te_daily の当日分をメモリに展開（全エンティティ・全条件共通） */
,temp_entity_te as (
  select entity_type, entity_id, condition_type, condition_key, cnt, sum_top3, sum_top1
  from `{project_id}`.features.entity_te_daily
  where as_of_date = date('{target_date}')
)

/* 騎手 Target Encoding（entity_te_daily JOIN版、スムージング係数m=10）
   jockey_count（base cnt）>= 20 の場合のみ各TE値を返す（低頻度マスク）
   2軸複合は >= 5、3軸複合は >= 3 */
,temp_jockey_te as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_base.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_base.cnt, 0) + 10), null) as jockey_te
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_ct.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_ct.cnt, 0) + 10), null) as jockey_course_type_te
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_venue.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_venue.cnt, 0) + 10), null) as jockey_venue_te
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_db.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_db.cnt, 0) + 10), null) as jockey_distance_band_te
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_dist.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_dist.cnt, 0) + 10), null) as jockey_distance_te
    ,if(coalesce(jte_base.cnt, 0) >= 20,
        safe_divide(coalesce(jte_dir.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_dir.cnt, 0) + 10), null) as jockey_direction_te
    ,if(coalesce(jte_base.cnt, 0) >= 5,
        safe_divide(coalesce(jte_cv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_cv.cnt, 0) + 10), null) as jockey_course_type_venue_te
    ,if(coalesce(jte_base.cnt, 0) >= 5,
        safe_divide(coalesce(jte_cd.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_cd.cnt, 0) + 10), null) as jockey_course_type_distance_te
    ,if(coalesce(jte_base.cnt, 0) >= 3,
        safe_divide(coalesce(jte_cdv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(jte_cdv.cnt, 0) + 10), null) as jockey_course_type_distance_venue_te
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i on h_r.race_id = r_i.race_id
    cross join temp_global_mean_te as g
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'base') as jte_base
      on jte_base.entity_id = h_r.jockey_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'course_type') as jte_ct
      on jte_ct.entity_id = h_r.jockey_code and jte_ct.condition_key = r_i.course_type
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'venue') as jte_venue
      on jte_venue.entity_id = h_r.jockey_code and jte_venue.condition_key = r_i.venue_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'distance_band') as jte_db
      on jte_db.entity_id = h_r.jockey_code
      and jte_db.condition_key = case
        when r_i.distance < 1400 then 'sprint'
        when r_i.distance < 1800 then 'mile'
        when r_i.distance < 2200 then 'intermediate'
        else 'long' end
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'distance') as jte_dist
      on jte_dist.entity_id = h_r.jockey_code
      and jte_dist.condition_key = cast(r_i.distance as string)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'direction') as jte_dir
      on jte_dir.entity_id = h_r.jockey_code and jte_dir.condition_key = r_i.direction
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'cv') as jte_cv
      on jte_cv.entity_id = h_r.jockey_code
      and jte_cv.condition_key = concat(r_i.course_type, '_', r_i.venue_code)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'cd') as jte_cd
      on jte_cd.entity_id = h_r.jockey_code
      and jte_cd.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string))
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'jockey' and condition_type = 'cdv') as jte_cdv
      on jte_cdv.entity_id = h_r.jockey_code
      and jte_cdv.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string), '_', r_i.venue_code)
  where r_i.race_date = date('{target_date}')
    and coalesce(r_i.course_type, '') != 'obstacle'
)

/* 調教師 Target Encoding（entity_te_daily JOIN版） */
,temp_trainer_te as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_base.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_base.cnt, 0) + 10), null) as trainer_te
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_ct.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_ct.cnt, 0) + 10), null) as trainer_course_type_te
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_venue.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_venue.cnt, 0) + 10), null) as trainer_venue_te
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_db.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_db.cnt, 0) + 10), null) as trainer_distance_band_te
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_dist.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_dist.cnt, 0) + 10), null) as trainer_distance_te
    ,if(coalesce(tte_base.cnt, 0) >= 20,
        safe_divide(coalesce(tte_dir.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_dir.cnt, 0) + 10), null) as trainer_direction_te
    ,if(coalesce(tte_base.cnt, 0) >= 5,
        safe_divide(coalesce(tte_cv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_cv.cnt, 0) + 10), null) as trainer_course_type_venue_te
    ,if(coalesce(tte_base.cnt, 0) >= 5,
        safe_divide(coalesce(tte_cd.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_cd.cnt, 0) + 10), null) as trainer_course_type_distance_te
    ,if(coalesce(tte_base.cnt, 0) >= 3,
        safe_divide(coalesce(tte_cdv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(tte_cdv.cnt, 0) + 10), null) as trainer_course_type_distance_venue_te
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i on h_r.race_id = r_i.race_id
    cross join temp_global_mean_te as g
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'base') as tte_base
      on tte_base.entity_id = h_r.trainer_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'course_type') as tte_ct
      on tte_ct.entity_id = h_r.trainer_code and tte_ct.condition_key = r_i.course_type
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'venue') as tte_venue
      on tte_venue.entity_id = h_r.trainer_code and tte_venue.condition_key = r_i.venue_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'distance_band') as tte_db
      on tte_db.entity_id = h_r.trainer_code
      and tte_db.condition_key = case
        when r_i.distance < 1400 then 'sprint'
        when r_i.distance < 1800 then 'mile'
        when r_i.distance < 2200 then 'intermediate'
        else 'long' end
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'distance') as tte_dist
      on tte_dist.entity_id = h_r.trainer_code
      and tte_dist.condition_key = cast(r_i.distance as string)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'direction') as tte_dir
      on tte_dir.entity_id = h_r.trainer_code and tte_dir.condition_key = r_i.direction
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'cv') as tte_cv
      on tte_cv.entity_id = h_r.trainer_code
      and tte_cv.condition_key = concat(r_i.course_type, '_', r_i.venue_code)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'cd') as tte_cd
      on tte_cd.entity_id = h_r.trainer_code
      and tte_cd.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string))
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'trainer' and condition_type = 'cdv') as tte_cdv
      on tte_cdv.entity_id = h_r.trainer_code
      and tte_cdv.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string), '_', r_i.venue_code)
  where r_i.race_date = date('{target_date}')
    and coalesce(r_i.course_type, '') != 'obstacle'
)

/* 種牡馬 Target Encoding（entity_te_daily JOIN版）
   年齢帯別TE・出走比率も含む */
,temp_sire_te as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_base.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_base.cnt, 0) + 10), null) as sire_te
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_ct.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_ct.cnt, 0) + 10), null) as sire_course_type_te
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_venue.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_venue.cnt, 0) + 10), null) as sire_venue_te
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_db.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_db.cnt, 0) + 10), null) as sire_distance_band_te
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_dist.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_dist.cnt, 0) + 10), null) as sire_distance_te
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_dir.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_dir.cnt, 0) + 10), null) as sire_direction_te
    ,if(coalesce(ste_base.cnt, 0) >= 5,
        safe_divide(coalesce(ste_cv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_cv.cnt, 0) + 10), null) as sire_course_type_venue_te
    ,if(coalesce(ste_base.cnt, 0) >= 5,
        safe_divide(coalesce(ste_cd.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_cd.cnt, 0) + 10), null) as sire_course_type_distance_te
    ,if(coalesce(ste_base.cnt, 0) >= 3,
        safe_divide(coalesce(ste_cdv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_cdv.cnt, 0) + 10), null) as sire_course_type_distance_venue_te
    -- 年齢帯別TE（各年齢帯 >= 5 でマスク）
    ,if(coalesce(ste_age2.cnt, 0) >= 5,
        safe_divide(coalesce(ste_age2.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_age2.cnt, 0) + 10), null) as sire_age2_te
    ,if(coalesce(ste_age3.cnt, 0) >= 5,
        safe_divide(coalesce(ste_age3.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_age3.cnt, 0) + 10), null) as sire_age3_te
    ,if(coalesce(ste_age4.cnt, 0) >= 5,
        safe_divide(coalesce(ste_age4.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_age4.cnt, 0) + 10), null) as sire_age4_te
    ,if(coalesce(ste_age5p.cnt, 0) >= 5,
        safe_divide(coalesce(ste_age5p.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(ste_age5p.cnt, 0) + 10), null) as sire_age5plus_te
    -- 出走比率（sire_count >= 20 でマスク）
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_ct.cnt, 0), nullif(coalesce(ste_base.cnt, 0), 0)), null) as sire_course_type_run_ratio
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_venue.cnt, 0), nullif(coalesce(ste_base.cnt, 0), 0)), null) as sire_venue_run_ratio
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_db.cnt, 0), nullif(coalesce(ste_base.cnt, 0), 0)), null) as sire_distance_band_run_ratio
    ,if(coalesce(ste_base.cnt, 0) >= 20,
        safe_divide(coalesce(ste_dist.cnt, 0), nullif(coalesce(ste_base.cnt, 0), 0)), null) as sire_distance_run_ratio
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i on h_r.race_id = r_i.race_id
    inner join (
      -- horse_master に同一 horse_id が複数行存在する場合、最新 data_date の1行に絞る（Issue #449）
      select * from `{project_id}`.raw.horse_master
      qualify row_number() over (partition by horse_id order by data_date desc, updated_at desc) = 1
    ) as h_m on h_r.horse_id = h_m.horse_id
    cross join temp_global_mean_te as g
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'base') as ste_base
      on ste_base.entity_id = h_m.sire_name
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'course_type') as ste_ct
      on ste_ct.entity_id = h_m.sire_name and ste_ct.condition_key = r_i.course_type
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'venue') as ste_venue
      on ste_venue.entity_id = h_m.sire_name and ste_venue.condition_key = r_i.venue_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'distance_band') as ste_db
      on ste_db.entity_id = h_m.sire_name
      and ste_db.condition_key = case
        when r_i.distance < 1400 then 'sprint'
        when r_i.distance < 1800 then 'mile'
        when r_i.distance < 2200 then 'intermediate'
        else 'long' end
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'distance') as ste_dist
      on ste_dist.entity_id = h_m.sire_name and ste_dist.condition_key = cast(r_i.distance as string)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'direction') as ste_dir
      on ste_dir.entity_id = h_m.sire_name and ste_dir.condition_key = r_i.direction
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'cv') as ste_cv
      on ste_cv.entity_id = h_m.sire_name
      and ste_cv.condition_key = concat(r_i.course_type, '_', r_i.venue_code)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'cd') as ste_cd
      on ste_cd.entity_id = h_m.sire_name
      and ste_cd.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string))
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'cdv') as ste_cdv
      on ste_cdv.entity_id = h_m.sire_name
      and ste_cdv.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string), '_', r_i.venue_code)
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'age_band' and condition_key = '2yo') as ste_age2
      on ste_age2.entity_id = h_m.sire_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'age_band' and condition_key = '3yo') as ste_age3
      on ste_age3.entity_id = h_m.sire_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'age_band' and condition_key = '4yo') as ste_age4
      on ste_age4.entity_id = h_m.sire_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'sire' and condition_type = 'age_band' and condition_key = '5plus') as ste_age5p
      on ste_age5p.entity_id = h_m.sire_name
  where r_i.race_date = date('{target_date}')
    and coalesce(r_i.course_type, '') != 'obstacle'
)

/* 母馬産駒 Target Encoding（entity_te_daily JOIN版、全期間ウィンドウ相当）
   年齢帯別TE・出走比率も含む。マスク閾値 >= 3（産駒数）、年齢帯は >= 5 */
,temp_mare_te as (
  select
    h_r.race_id
    ,h_r.horse_number
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_base.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_base.cnt, 0) + 10), null) as mare_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_ct.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_ct.cnt, 0) + 10), null) as mare_course_type_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_venue.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_venue.cnt, 0) + 10), null) as mare_venue_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_db.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_db.cnt, 0) + 10), null) as mare_distance_band_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_dist.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_dist.cnt, 0) + 10), null) as mare_distance_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_dir.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_dir.cnt, 0) + 10), null) as mare_direction_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_cv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_cv.cnt, 0) + 10), null) as mare_course_type_venue_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_cd.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_cd.cnt, 0) + 10), null) as mare_course_type_distance_te
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_cdv.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_cdv.cnt, 0) + 10), null) as mare_course_type_distance_venue_te
    -- 年齢帯別TE
    ,if(coalesce(mte_age2.cnt, 0) >= 5,
        safe_divide(coalesce(mte_age2.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_age2.cnt, 0) + 10), null) as mare_age2_te
    ,if(coalesce(mte_age3.cnt, 0) >= 5,
        safe_divide(coalesce(mte_age3.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_age3.cnt, 0) + 10), null) as mare_age3_te
    ,if(coalesce(mte_age4.cnt, 0) >= 5,
        safe_divide(coalesce(mte_age4.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_age4.cnt, 0) + 10), null) as mare_age4_te
    ,if(coalesce(mte_age5p.cnt, 0) >= 5,
        safe_divide(coalesce(mte_age5p.sum_top3, 0) + 10 * g.global_top3_rate,
                    coalesce(mte_age5p.cnt, 0) + 10), null) as mare_age5plus_te
    -- 出走比率（mare_count >= 3 でマスク）
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_ct.cnt, 0), nullif(coalesce(mte_base.cnt, 0), 0)), null) as mare_course_type_run_ratio
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_venue.cnt, 0), nullif(coalesce(mte_base.cnt, 0), 0)), null) as mare_venue_run_ratio
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_db.cnt, 0), nullif(coalesce(mte_base.cnt, 0), 0)), null) as mare_distance_band_run_ratio
    ,if(coalesce(mte_base.cnt, 0) >= 3,
        safe_divide(coalesce(mte_dist.cnt, 0), nullif(coalesce(mte_base.cnt, 0), 0)), null) as mare_distance_run_ratio
  from `{project_id}`.raw.horse_results as h_r
    inner join `{project_id}`.raw.race_info as r_i on h_r.race_id = r_i.race_id
    inner join (
      -- horse_master に同一 horse_id が複数行存在する場合、最新 data_date の1行に絞る（Issue #449）
      select * from `{project_id}`.raw.horse_master
      qualify row_number() over (partition by horse_id order by data_date desc, updated_at desc) = 1
    ) as h_m on h_r.horse_id = h_m.horse_id
    cross join temp_global_mean_te as g
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'base') as mte_base
      on mte_base.entity_id = h_m.dam_name
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'course_type') as mte_ct
      on mte_ct.entity_id = h_m.dam_name and mte_ct.condition_key = r_i.course_type
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'venue') as mte_venue
      on mte_venue.entity_id = h_m.dam_name and mte_venue.condition_key = r_i.venue_code
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'distance_band') as mte_db
      on mte_db.entity_id = h_m.dam_name
      and mte_db.condition_key = case
        when r_i.distance < 1400 then 'sprint'
        when r_i.distance < 1800 then 'mile'
        when r_i.distance < 2200 then 'intermediate'
        else 'long' end
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'distance') as mte_dist
      on mte_dist.entity_id = h_m.dam_name and mte_dist.condition_key = cast(r_i.distance as string)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'direction') as mte_dir
      on mte_dir.entity_id = h_m.dam_name and mte_dir.condition_key = r_i.direction
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'cv') as mte_cv
      on mte_cv.entity_id = h_m.dam_name
      and mte_cv.condition_key = concat(r_i.course_type, '_', r_i.venue_code)
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'cd') as mte_cd
      on mte_cd.entity_id = h_m.dam_name
      and mte_cd.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string))
    left join (select entity_id, condition_key, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'cdv') as mte_cdv
      on mte_cdv.entity_id = h_m.dam_name
      and mte_cdv.condition_key = concat(r_i.course_type, '_', cast(r_i.distance as string), '_', r_i.venue_code)
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'age_band' and condition_key = '2yo') as mte_age2
      on mte_age2.entity_id = h_m.dam_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'age_band' and condition_key = '3yo') as mte_age3
      on mte_age3.entity_id = h_m.dam_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'age_band' and condition_key = '4yo') as mte_age4
      on mte_age4.entity_id = h_m.dam_name
    left join (select entity_id, cnt, sum_top3 from temp_entity_te
               where entity_type = 'mare' and condition_type = 'age_band' and condition_key = '5plus') as mte_age5p
      on mte_age5p.entity_id = h_m.dam_name
  where r_i.race_date = date('{target_date}')
    and coalesce(r_i.course_type, '') != 'obstacle'
)

/* 馬自身 TE・TE_diff（temp_horse_te_pre〜temp_horse_te_diff_summary）は差し替え範囲の外にあり、
   予測SQLでも feature_query_raw.sql の定義をそのまま使う（Issue #463）。
   馬TEは馬自身の過去走だけで決まり軽いため、学習時と同じ時系列平均（_diff_avg）を再現できる */
