show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from natprod start time: ' || systimestamp from dual;

prompt int_qw_result
truncate table int_qw_result;
insert /*+ append parallel(4) */ into int_qw_result
select regexp_replace(nwis_host_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(db_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(record_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       result_va,
       regexp_replace(result_va_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(result_sg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(result_rd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(remark_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       rpt_lev_va,
       regexp_replace(rpt_lev_va_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(rpt_lev_sg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(rpt_lev_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       result_rnd_va,
       regexp_replace(result_rnd_va_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(remark_rnd_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(dqi_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(null_val_qual_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(prep_set_no, '(^[[:space:]]*|[[:space:]]*$)'),
       prep_dt,
       regexp_replace(anl_set_no, '(^[[:space:]]*|[[:space:]]*$)'),
       anl_dt,
       lab_std_dev_va,
       regexp_replace(lab_std_dev_va_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(lab_std_dev_sg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(anl_ent_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(val_qual_cd_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(result_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_cr,
       regexp_replace(result_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_md,
       regexp_replace(result_lab_cm_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(result_lab_cm_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_lab_cm_cr,
       regexp_replace(result_lab_cm_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_lab_cm_md,
       regexp_replace(result_field_cm_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(result_field_cm_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_field_cm_cr,
       regexp_replace(result_field_cm_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       result_field_cm_md,
       regexp_replace(result_web_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       result_ld,
       regexp_replace(deprecated_fg, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.qw_result@natdb.er.usgs.gov;
commit;

prompt int_qw_sample
truncate table int_qw_sample;
insert /*+ append parallel(4) */ into int_qw_sample
select regexp_replace(nwis_host_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(db_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(record_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(agency_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_db_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(medium_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_start_dt,
       regexp_replace(sample_start_sg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_start_tz_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_start_local_tm_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_end_dt,
       regexp_replace(sample_end_sg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_end_tz_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_end_local_tm_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_start_local_disp_fm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_start_utc_disp_fm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_end_local_disp_fm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_end_utc_disp_fm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(local_tz_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tm_datum_rlblty_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_id,
       regexp_replace(anl_stat_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_cond_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(samp_type_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_event_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(project_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(lab_no, '(^[[:space:]]*|[[:space:]]*$)'),
       tu_id,
       body_part_id,
       regexp_replace(coll_ent_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sidno_party_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_cr,
       regexp_replace(sample_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_md,
       regexp_replace(sample_lab_cm_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_lab_cm_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_lab_cm_cr,
       regexp_replace(sample_lab_cm_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_lab_cm_md,
       regexp_replace(sample_field_cm_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(sample_field_cm_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_field_cm_cr,
       regexp_replace(sample_field_cm_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_field_cm_md,
       regexp_replace(sample_web_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(deprecated_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       sample_ld
  from natdb.qw_sample@natdb.er.usgs.gov;
commit;

prompt int_sitefile
truncate table int_sitefile;
insert /*+ append parallel(4) */ into int_sitefile
select regexp_replace(nwis_host_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(db_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(agency_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(station_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(station_ix, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(lat_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(long_va, '(^[[:space:]]*|[[:space:]]*$)'),
       dec_lat_va,
       dec_long_va,
       regexp_replace(coord_meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(coord_acy_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(coord_datum_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(district_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(land_net_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(map_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(mcd_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(map_scale_fc, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(alt_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(alt_meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(alt_acy_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(alt_datum_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(huc_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(agency_use_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(basin_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_tp_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(topo_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(data_types_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(instruments_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_rmks_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(inventory_dt, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(drain_area_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(contrib_drain_area_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(local_time_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_file_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(construction_dt, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(reliability_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(nat_aqfr_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_use_1_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_use_2_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_use_3_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(water_use_1_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(water_use_2_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(water_use_3_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(nat_water_use_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr_type_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(well_depth_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hole_depth_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(depth_src_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(project_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_web_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_cn, '(^[[:space:]]*|[[:space:]]*$)'),
       site_cr,
       regexp_replace(site_mn, '(^[[:space:]]*|[[:space:]]*$)'),
       site_md,
       regexp_replace(deprecated_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       site_ld
  from natdb.sitefile@natdb.er.usgs.gov;
commit;

prompt int_sample_parameter
truncate table int_sample_parameter;
insert /*+ append parallel(4) */ into int_sample_parameter
select nwis_host_nm,
       db_no,
       record_no,
       v71999,
       v50280,
       v72015,
       v82047,
       v72016,
       v82048,
       v00003,
       v00098,
       v78890,
       v78891,
       v82398,
       v84164,
       v71999_fxd_nm,
       v82398_fxd_tx,
       v84164_fxd_tx
  from (select * from (select nwis_host_nm, db_no, record_no, parm_cd, result_va_tx
                         from int_qw_result
                        where parm_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164') and
                              deprecated_fg = 'N')
                       pivot (max(result_va_tx)
                              for parm_cd in ('71999' V71999, '50280' V50280, '72015' V72015, '82047' V82047, '72016' V72016, '82048' V82048,
                                                   '00003' V00003, '00098' V00098, '78890' V78890, '78891' V78891, '82398' V82398, '84164' V84164))
       ) p2
       left join (select fxd_nm v71999_fxd_nm, fxd_va from fxd where parm_cd = '71999') fxd_71999
         on p2.v71999 = fxd_71999.fxd_va
       left join (select fxd_tx v82398_fxd_tx, fxd_va from fxd where parm_cd = '82398') fxd_82398
         on p2.v82398 = fxd_82398.fxd_va
       left join (select fxd_tx v84164_fxd_tx, fxd_va from fxd where parm_cd = '84164') fxd_84164
         on p2.v84164 = fxd_84164.fxd_va;
commit;

select 'copy from natprod end time: ' || systimestamp from dual;
