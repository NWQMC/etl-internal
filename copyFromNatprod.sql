show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from natprod start time: ' || systimestamp from dual;

prompt altitude_method
truncate table altitude_method;
insert /*+ append parallel(4) */ into altitude_method
select regexp_replace(gw_ref_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_ref_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       gw_sort_nu,
       regexp_replace(gw_ref_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_valid_fg, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'saltmt';
commit;

prompt aqfr
truncate table aqfr;
insert /*+ append parallel(4) */ into aqfr
select regexp_replace(aqfr_state.country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr_state.state_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr.aqfr_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr.aqfr_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(aqfr.aqfr_dt, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.aqfr@natdb.er.usgs.gov
       join natdb.aqfr_state@natdb.er.usgs.gov
         on aqfr.aqfr_cd = aqfr_state.aqfr_cd;
commit;

prompt aquifer_type
truncate table aquifer_type;
insert /*+ append parallel(4) */ into aquifer_type
select regexp_replace(gw_ref_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_ref_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       gw_sort_nu,
       regexp_replace(gw_ref_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_valid_fg, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'saqtyp';
commit;

prompt body_part
truncate table body_part;
insert /*+ append parallel(4) */ into body_part
select regexp_replace(body_part_id, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(body_part_nm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.body_part@natdb.er.usgs.gov;
commit;

prompt cit_meth
truncate table cit_meth;
insert /*+ append parallel(4) */ into cit_meth
select cit_meth_id,
       regexp_replace(meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(cit_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(cit_meth_no, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_src_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(cit_meth_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       cit_meth_init_dt,
       regexp_replace(cit_meth_rev_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       cit_meth_rev_dt
  from natdb.cit_meth@natdb.er.usgs.gov;
commit;

prompt country - STORET still has CN as Canada, all but a few NWIS sites have been migrated, and we never expect data for China, so do not include any CN in NWIS data
truncate table country;
insert /*+ append parallel(4) */ into country
select regexp_replace(country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(country_nm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.country@natdb.er.usgs.gov
 where regexp_replace(country_cd, '(^[[:space:]]*|[[:space:]]*$)') != 'CN';
commit;

prompt county
truncate table county;
insert /*+ append parallel(4) */ into county
select regexp_replace(country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_max_lat_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_min_lat_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_max_long_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_min_long_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_max_alt_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_min_alt_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(county_md, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.county@natdb.er.usgs.gov;
commit;

prompt fxd
truncate table fxd;
insert /*+ append parallel(4) */ into fxd
select regexp_replace(parm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       fxd_va,
       regexp_replace(fxd_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(fxd_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       fxd_init_dt,
       regexp_replace(fxd_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       fxd_rev_dt,
       regexp_replace(fxd_rev_nm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.fxd@natdb.er.usgs.gov;
commit;

prompt hyd_cond_cd
truncate table hyd_cond_cd;
insert /*+ append parallel(4) */ into hyd_cond_cd
select regexp_replace(hyd_cond_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       hyd_cond_srt_nu,
       regexp_replace(hyd_cond_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_cond_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_cond_ds, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.hyd_cond_cd@natdb.er.usgs.gov;
commit;

prompt hyd_event_cd
truncate table hyd_event_cd;
insert /*+ append parallel(4) */ into hyd_event_cd
select regexp_replace(hyd_event_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       hyd_event_srt_nu,
       regexp_replace(hyd_event_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_event_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(hyd_event_ds, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.hyd_event_cd@natdb.er.usgs.gov;
commit;

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

prompt lat_long_datum
truncate table lat_long_datum;
insert /*+ append parallel(4) */ into lat_long_datum
select regexp_replace(gw_ref_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_ref_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       gw_sort_nu,
       regexp_replace(gw_ref_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_valid_fg, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'scordm';
commit;

prompt lat_long_method
truncate table lat_long_method;
insert /*+ append parallel(4) */ into lat_long_method
select regexp_replace(gw_ref_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_ref_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       gw_sort_nu,
       regexp_replace(gw_ref_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(gw_valid_fg, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'scormt';
commit;

prompt meth
truncate table meth;
insert /*+ append parallel(4) */ into meth
select regexp_replace(meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_tp, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_rnd_owner_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(discipline_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       meth_init_dt,
       regexp_replace(meth_rev_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       meth_rev_dt
  from natdb.meth@natdb.er.usgs.gov;
commit;

prompt meth_with_cit
truncate table meth_with_cit;
insert /*+ append parallel(4) */ into meth_with_cit
select meth.meth_cd,
       meth.meth_nm,
       min(cit_meth.cit_nm)
  from meth
       left join cit_meth
         on meth.meth_cd = cit_meth.meth_cd
    group by meth.meth_cd, meth.meth_nm;
commit;

prompt nat_aqfr
truncate table nat_aqfr;
insert /*+ append parallel(4) */ into nat_aqfr
select regexp_replace(nat_aqfr_state.country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(nat_aqfr_state.state_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(nat_aqfr.nat_aqfr_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(nat_aqfr.nat_aqfr_nm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.nat_aqfr@natdb.er.usgs.gov
       join natdb.nat_aqfr_state@natdb.er.usgs.gov
         on nat_aqfr.nat_aqfr_cd = nat_aqfr_state.nat_aqfr_cd;
commit;

prompt parm_meth
truncate table parm_meth;
insert /*+ append parallel(4) */ into parm_meth
select regexp_replace(parm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(meth_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_meth_lgcy_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_meth_rnd_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_meth_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       parm_meth_init_dt,
       regexp_replace(parm_meth_rev_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       parm_meth_rev_dt,
       regexp_replace(parm_meth_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       decode(regexp_instr(parm_meth_rnd_tx, '[1-9]', 1, 1),
              1, '0.001',
              2, '0.01',
              3, '0.1',
              4, '1.',
              5, '10',
              6, '100',
              7, '1000',
              8, '10000',
              9, '100000') multiplier
  from natdb.parm_meth@natdb.er.usgs.gov;
commit;

prompt parm
truncate table parm;
insert /*+ append parallel(4) */ into parm
select regexp_replace(parm.parm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_rmk_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_unt_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_seq_nu, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_seq_grp_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_medium_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_frac_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_temp_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_stat_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_tm_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_wt_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_size_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_entry_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_public_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_neg_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_zero_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_null_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_ts_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_init_dt, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_rev_dt, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm.parm_rev_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       parm_seq_grp_cd.parm_seq_grp_nm,
       parm_alias.wqpcrosswalk,
       parm_alias.srsname,
       parm_meth.multiplier
  from natdb.parm@natdb.er.usgs.gov
       left join natdb.parm_seq_grp_cd@natdb.er.usgs.gov
         on regexp_replace(parm.parm_seq_grp_cd, '(^[[:space:]]*|[[:space:]]*$)') = regexp_replace(parm_seq_grp_cd.parm_seq_grp_cd, '(^[[:space:]]*|[[:space:]]*$)')
       join (select *
               from (select parm_cd, parm_alias_cd, parm_alias_nm
                       from natdb.parm_alias@natdb.er.usgs.gov
                      where parm_alias_cd in ('WQPCROSSWALK', 'SRSNAME'))
                     pivot (max(parm_alias_nm)
                            for parm_alias_cd in ('WQPCROSSWALK' wqpcrosswalk, 'SRSNAME' srsname))
               where nvl(wqpcrosswalk, srsname) is not null) parm_alias
         on regexp_replace(parm.parm_cd, '(^[[:space:]]*|[[:space:]]*$)') = regexp_replace(parm_alias.parm_cd, '(^[[:space:]]*|[[:space:]]*$)')
       left join parm_meth
         on regexp_replace(parm.parm_cd, '(^[[:space:]]*|[[:space:]]*$)') = parm_meth.parm_cd and
            parm_meth.meth_cd is null
 where parm.parm_public_fg = 'Y';
commit;

prompt parm_alias
truncate table parm_alias;
insert /*+ append parallel(4) */ into parm_alias
select regexp_replace(parm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_alias_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(parm_alias_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       parm_alias_init_dt,
       regexp_replace(parm_alias_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       parm_alias_rev_dt,
       regexp_replace(parm_alias_rev_nm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.parm_alias@natdb.er.usgs.gov;
commit;

prompt proto_org
truncate table proto_org;
insert /*+ append parallel(4) */ into proto_org
select regexp_replace(proto_org_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(proto_org_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(proto_org_fv_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(proto_org_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(proto_org_init_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       proto_org_init_dt,
       regexp_replace(proto_org_rev_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       proto_org_rev_dt
  from natdb.proto_org@natdb.er.usgs.gov;
commit;

prompt site_tp
truncate table site_tp;
insert /*+ append parallel(4) */ into site_tp
select regexp_replace(site_tp_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       site_tp_srt_nu,
       regexp_replace(site_tp_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_tp_prim_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_tp_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_tp_ln, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(site_tp_ds, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.site_tp@natdb.er.usgs.gov;
commit;

prompt state
truncate table state;
insert /*+ append parallel(4) */ into state
select regexp_replace(country_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_post_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_max_lat_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_min_lat_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_max_long_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_min_long_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_max_alt_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_min_alt_va, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(state_md, '(^[[:space:]]*|[[:space:]]*$)')
 from natdb.state@natdb.er.usgs.gov;
commit;

prompt tu
truncate table tu;
insert /*+ append parallel(4) */ into tu
select tu_id,
       regexp_replace(tu_1_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_1_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_2_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_2_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_3_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_3_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_4_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_4_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_unnm_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_use_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_unaccp_rsn_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_cred_rat_tx, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_cmplt_rat_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tu_curr_rat_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       tu_phyl_srt_nu,
       tu_cr,
       tu_par_id,
       tu_tax_auth_id,
       tu_hybr_auth_id,
       tu_king_id,
       tu_rnk_id,
       tu_md
  from natdb.tu@natdb.er.usgs.gov;
commit;

prompt tz
truncate table tz;
insert /*+ append parallel4) */ into tz
select regexp_replace(tz_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       tz_nu,
       regexp_replace(tz_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_ds, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_utc_offset_tm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_dst_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_dst_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(tz_dst_utc_offset_tm, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.tz@natdb.er.usgs.gov;
commit;

prompt val_qual_cd
truncate table val_qual_cd;
insert /*+ append parallel(4) */ into val_qual_cd
select regexp_replace(val_qual_cd, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(val_qual_tp, '(^[[:space:]]*|[[:space:]]*$)'),
       val_qual_srt_nu,
       regexp_replace(val_qual_vld_fg, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(val_qual_nm, '(^[[:space:]]*|[[:space:]]*$)'),
       regexp_replace(val_qual_ds, '(^[[:space:]]*|[[:space:]]*$)')
  from natdb.val_qual_cd@natdb.er.usgs.gov;
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
