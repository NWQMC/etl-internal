show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform station start time: ' || systimestamp from dual;

prompt dropping nwis station indexes
exec etl_helper_station.drop_indexes('nwis');

prompt populating nwis station
truncate table station_swap_nwis;

insert /*+ append parallel(4) */
  into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                          geom, station_name, organization_name, station_type_name, latitude, longitude, map_scale,
                          geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                          drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,
                          geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,
                          nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,
                          hole_depth_value, hole_depth_unit, deprecated_flag, web_code)
select /*+ parallel(4) */ 
       1 data_source_id,
       'NWIS' data_source,
       rownum station_id,
       sitefile.agency_cd || '-' || sitefile.site_no site_id,
       ndcbh.organization_id organization,
       site_tp.primary_site_type site_type,
       sitefile.huc_cd huc,
       sitefile.country_cd || ':' || sitefile.state_cd || ':' || sitefile.county_cd governmental_unit_code,
       case
         when sitefile.dec_long_va is null or
              sitefile.dec_lat_va is null then
           null
         else
           case sitefile.coord_datum_cd
             when 'NAD27' then
               sdo_cs.transform(mdsys.sdo_geometry(2001, 4267, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             when 'NAD83' then
               mdsys.sdo_geometry(2001, 4269, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null)
             when 'OLDGUAM' then
               sdo_cs.transform(mdsys.sdo_geometry(2001, 4675, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             when 'OLDHI' then
               sdo_cs.transform(mdsys.sdo_geometry(2001, 4135, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             when 'OLDSAMOA' then
               sdo_cs.transform(mdsys.sdo_geometry(2001, 4169, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             when 'WGS84' then
               sdo_cs.transform(mdsys.sdo_geometry(2001, 4326, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             when 'YOFF' then
              sdo_cs.transform(mdsys.sdo_geometry(2001, 4310, mdsys.sdo_point_type(round(sitefile.dec_long_va, 7), round(sitefile.dec_lat_va, 7), null), null, null), 4269)
             else /* IRAQKUWAIT and OLDAK */
               null
           end
       end geom,
       trim(sitefile.station_nm) station_name,
       ndcbh.organization_name,
       site_tp.station_type_name,
       round(sitefile.dec_lat_va , 7) latitude,
       round(sitefile.dec_long_va, 7) longitude,
       trim(sitefile.map_scale_fc) map_scale,
       nvl(lat_long_method.description, 'Unknown') geopositioning_method,
       nvl(sitefile.coord_datum_cd, 'Unknown') hdatum_id_code,
       case
         when sitefile.alt_datum_cd is not null then
           case
             when sitefile.alt_va = '.' then
               '0'
             else
               trim(sitefile.alt_va)
           end
         else
           null
       end elevation_value,
       case
         when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then
           'feet'
         else
           null
       end elevation_unit,
       altitude_method.description elevation_method,
       case
         when sitefile.alt_va is not null then
           sitefile.alt_datum_cd
         else
           null
       end vdatum_id_code,
       to_number(sitefile.drain_area_va) drain_area_value,
       nvl2(sitefile.drain_area_va, 'sq mi', null) drain_area_unit,
       case
         when sitefile.contrib_drain_area_va = '.' then
           0
         else
           to_number(sitefile.contrib_drain_area_va)
       end contrib_drain_area_value,
       nvl2(sitefile.contrib_drain_area_va, 'sq mi', null) contrib_drain_area_unit,
       lat_long_accuracy.accuracy geoposition_accy_value,
       lat_long_accuracy.unit geoposition_accy_unit,
       case
         when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then
           trim(sitefile.alt_acy_va)
         else
           null
       end vertical_accuracy_value,
       case
         when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null and sitefile.alt_acy_va is not null then
           'feet'
         else
           null
       end vertical_accuracy_unit,
       nat_aqfr.nat_aqfr_name,
       aqfr.aqfr_nm,
       aquifer_type.description,
       regexp_replace(sitefile.construction_dt, '([[:space:]])', '0') construction_date,
       to_number(case when sitefile.well_depth_va in ('.', '-') then '0' else sitefile.well_depth_va end) well_depth_value,
       nvl2(sitefile.well_depth_va, 'ft', null) well_depth_unit,
       to_number(case when sitefile.hole_depth_va in ('.', '-') then '0' else sitefile.hole_depth_va end) hole_depth_value,
       nvl2(sitefile.hole_depth_va, 'ft', null) hole_depth_unit,
       sitefile.deprecated_fg,
       sitefile.site_web_cd
  from nwis_ws_star.int_sitefile sitefile
       left join (select distinct cast('USGS-' || state_postal_cd as varchar2(7)) organization_id,
                    'USGS ' || state_name || ' Water Science Center' organization_name,
                    district_cd
               from nwis_ws_star.nwis_district_cds_by_host) ndcbh
         on sitefile.district_cd  = ndcbh.district_cd
       left join (select a.site_tp_cd,
                         case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                           else b.site_tp_ln || ': ' || a.site_tp_ln
                         end as station_type_name,
                         case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                           else b.site_tp_ln
                         end as primary_site_type
                    from nwis_ws_star.site_tp a,
                         nwis_ws_star.site_tp b
                   where substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
                         b.site_tp_prim_fg = 'Y') site_tp
         on sitefile.site_tp_cd = site_tp.site_tp_cd
       left join nwis_ws_star.altitude_method
         on sitefile.alt_meth_cd = altitude_method.code
       left join nwis_ws_star.lat_long_method
         on sitefile.coord_meth_cd= lat_long_method.code
       left join nwis_ws_star.lat_long_accuracy
         on sitefile.coord_acy_cd = lat_long_accuracy.code
       left join (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nwis_ws_star.nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr
         on sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd
       left join nwis_ws_star.aquifer_type
         on sitefile.aqfr_type_cd = aquifer_type.code
       left join nwis_ws_star.aqfr
         on sitefile.aqfr_cd = aqfr.aqfr_cd and
            sitefile.state_cd = aqfr.state_cd;
commit;

prompt building nwis station indexes
exec etl_helper_station.create_indexes('nwis');

select 'transform station end time: ' || systimestamp from dual;
