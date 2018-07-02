show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform activity start time: ' || systimestamp from dual;

prompt building activity_swap_nwis 

prompt dropping nwis activity indexes
exec etl_helper_activity.drop_indexes('nwis');

set define off;
truncate table activity_swap_nwis;

insert /*+ append parallel(4) */
  into activity_swap_nwis (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media,
                           organization, site_type, huc, governmental_unit_code, organization_name, activity_id,
                           activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,
                           activity_stop_date, activity_stop_time, act_stop_time_zone,
                           activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth,
                           activity_upper_depth_unit, activity_lower_depth, activity_lower_depth_unit, project_id,
                           activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name,
                           hydrologic_event_name, sample_collect_method_id,
                           sample_collect_method_ctx, sample_collect_method_name, act_sam_collect_meth_qual_type,
                           act_sam_collect_meth_desc, sample_collect_equip_name, deprecated_flag, web_code, result_count)
select 1 data_source_id,
       'NWIS' data_source,
       s.station_id,
       s.site_id,
       case 
         when length(samp.sample_start_local_disp_fm) > 3 then
           to_date(rpad(substr(samp.sample_start_local_disp_fm, 1, 8), 8, '01'), 'yyyymmdd')
         else
           null
       end event_date,
       samp.nwis_host_nm || '.' || samp.db_no || '.' || samp.record_no activity,
       nwis_wqx_medium_cd.wqx_act_med_nm sample_media,
       s.organization,
       s.site_type,
       s.huc,
       s.governmental_unit_code,
       s.organization_name,
       rownum activity_id,
       case 
         when samp.samp_type_cd = 'A' then
           'Not determined'
         when samp.samp_type_cd = 'B' then
           'Quality Control Sample-Other'
         when samp.samp_type_cd = 'H' then
           'Sample-Composite Without Parents'
         when samp.samp_type_cd = '1' then
           'Quality Control Sample-Field Spike'
         when samp.samp_type_cd = '2' then
           'Quality Control Sample-Field Blank'
         when samp.samp_type_cd = '3' then
           'Quality Control Sample-Reference Sample'
         when samp.samp_type_cd = '4' then
           'Quality Control Sample-Blind'
         when samp.samp_type_cd = '5' then
           'Quality Control Sample-Field Replicate'
         when samp.samp_type_cd = '6' then
           'Quality Control Sample-Reference Material'
         when samp.samp_type_cd = '7' then
           'Quality Control Sample-Field Replicate'
         when samp.samp_type_cd = '8' then
           'Quality Control Sample-Spike Solution'
         when samp.samp_type_cd = '9' then
           'Sample-Routine'
         else
           'Unknown'
       end activity_type_code,
       nwis_wqx_medium_cd.wqx_act_med_sub activity_media_subdiv_name,
       case samp.sample_start_sg
         when 'h' then
           substr(samp.sample_start_local_disp_fm, 9, 2)
         when 'm' then
           substr(samp.sample_start_local_disp_fm, 9, 2) || ':' || substr(samp.sample_start_local_disp_fm, 11, 2)
         else
           null
       end activity_start_time,
       case
         when samp.sample_start_dt is not null and samp.sample_start_sg in ('h','m') then
           samp.local_tz_cd
         else
           null
       end act_start_time_zone,
       case
         when samp.sample_end_sg in ('D', 'h', 'm') and length(samp.sample_end_local_disp_fm) > 7 then
           substr(samp.sample_end_local_disp_fm, 1, 4) || '-' || substr(samp.sample_end_local_disp_fm, 5, 2) || '-' || substr(samp.sample_end_local_disp_fm, 7, 2)
         when samp.sample_end_sg in ('M') and length(samp.sample_end_local_disp_fm) > 5 then
           substr(samp.sample_end_local_disp_fm, 1, 4) || '-' || substr(samp.sample_end_local_disp_fm, 5, 2)
         when samp.sample_end_sg in ('Y') and length(samp.sample_end_local_disp_fm) > 3 then
           substr(samp.sample_end_local_disp_fm, 1, 4)
         else
           null
       end activity_stop_date,
       case samp.sample_end_sg
         when 'h' then
           substr(samp.sample_end_local_disp_fm, 9, 2)
         when 'm' then
           substr(samp.sample_end_local_disp_fm, 9, 2) || ':' || substr(samp.sample_end_local_disp_fm, 11, 2)
         else
           null
       end activity_stop_time,
       case 
         when samp.sample_end_local_disp_fm is not null and samp.sample_end_sg in ('h', 'm') then samp.local_tz_cd
         else null
       end act_stop_time_zone,
       coalesce(parameter.V00003, parameter.V00098, parameter.V78890, parameter.V78891) activity_depth,
       case
         when parameter.V00003 is not null then
           'feet'
         when parameter.V00098 is not null then
           'meters'
         when parameter.V78890 is not null then
           'feet'
         when parameter.V78891 is not null then
           'meters'
         else
           null
       end activity_depth_unit,
       case
         when parameter.V00003 is not null or
              parameter.V00098 is not null then
           null
         when parameter.V78890 is not null or 
              parameter.V78891 is not null then
           'Below mean sea level'
         when parameter.V72015 is not null then
           'Below land-surface datum'
         when parameter.V82047 is not null then
           null
         when parameter.V72016 is not null then
           'Below land-surface datum'
         when parameter.V82048 is not null then
           null
         else
           null
       end activity_depth_ref_point,
       coalesce(parameter.V72015, parameter.V82047) activity_upper_depth,
       nvl2(coalesce(parameter.V72015, parameter.V82047),
            case
              when parameter.V72015 is not null then
                'feet'
              when parameter.V82047 is not null then
                'meters'
              when parameter.V72016 is not null then
                'feet'
              when parameter.V82048 is not null then
                'meters'
              else
                null
            end,
            null) activity_upper_depth_unit,
       case
         when parameter.V72015 is not null then
           parameter.V72016
         when parameter.V82047 is not null then
           parameter.V82048
         when parameter.V72016 is not null then
           parameter.V72016
         when parameter.V82048 is not null then
           parameter.V82048
         else
           null
       end activity_lower_depth,
       nvl2(case
              when parameter.V72015 is not null then
                parameter.V72016
              when parameter.V82047 is not null then
                parameter.V82048
              when parameter.V72016 is not null then
                parameter.V72016
              when parameter.V82048 is not null then
                parameter.V82048
              else
                null
            end,
            case
              when parameter.V72015 is not null then
                'feet'
              when parameter.V82047 is not null then
                'meters'
              when parameter.V72016 is not null then
                'feet'
              when parameter.V82048 is not null then
                'meters'
              else
                null
            end,
            null) activity_lower_depth_unit,
       samp.project_cd project_id,
       coalesce(proto_org.proto_org_nm, samp.coll_ent_cd) activity_conducting_org,
       samp.sample_lab_cm_tx activity_comment,
       aqfr.aqfr_nm sample_aqfr_name,
       hyd_cond_cd.hyd_cond_nm hydrologic_condition_name,
       hyd_event_cd.hyd_event_nm hydrologic_event_name,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null then
           parameter.V82398
         else
           'USGS'
       end sample_collect_method_id,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null then
           cast('USGS parameter code 82398' as varchar2(25))
         else
           'USGS'
       end sample_collect_method_ctx,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null then
           parameter.v82398_fxd_tx
         else
           'USGS'
       end sample_collect_method_name,
       null act_sam_collect_meth_qual_type,
       null act_sam_collect_meth_desc,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null then
           parameter.v84164_fxd_tx
         else
           'Unknown'
       end sample_collect_equip_name,
       samp.deprecated_fg,
       samp.sample_web_cd
       (select count(*) from nwis_ws_star.int_qw_result where int_qw_result.sample_id = samp.sample_id) result_count
  from nwis_ws_star.int_qw_sample samp
       join station_swap_nwis s
         on samp.agency_cd || '-' || samp.site_no = s.site_id and
            'N' = s.deprecated_flag
       left join nwis_ws_star.tu
         on to_number(samp.tu_id) = tu.tu_id
       left join nwis_ws_star.nwis_wqx_medium_cd
         on samp.medium_cd = nwis_wqx_medium_cd.nwis_medium_cd
       left join nwis_ws_star.body_part
         on samp.body_part_id = body_part.body_part_id
       left join nwis_ws_star.proto_org
         on samp.coll_ent_cd = proto_org.proto_org_cd
       left join nwis_ws_star.hyd_event_cd
         on samp.hyd_event_cd = hyd_event_cd.hyd_event_cd
       left join nwis_ws_star.hyd_cond_cd
         on samp.hyd_cond_cd = hyd_cond_cd.hyd_cond_cd
       left join nwis_ws_star.aqfr
         on samp.aqfr_cd = aqfr.aqfr_cd and
            substr(s.state_code,4,5) = aqfr.state_cd
       left join nwis_ws_star.int_sample_parameter parameter
         on samp.nwis_host_nm = parameter.nwis_host_nm and
            samp.db_no = parameter.db_no and
            samp.record_no = parameter.record_no;

commit;

prompt building nwis activity indexes
exec etl_helper_activity.create_indexes('nwis');

select 'transform activity end time: ' || systimestamp from dual;
