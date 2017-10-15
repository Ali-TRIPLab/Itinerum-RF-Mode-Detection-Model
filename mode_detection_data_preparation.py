# Ali Yazdizadeh, July 2017
import cPickle
import fiona
import math
import threading
import utm
import experiments
from modules import labels
from pprint import pprint

import foursquare
from foursquare import Foursquare
import pprint
import inspect
#Ali Yzdizadeh May 2017

import psycopg2
import pprint
import numpy
import time
import json
import ijson
import cPickle as pickle


##set connection to psql db
start_time = time.time()
#make a con to postgresql database
#pls change the database, user and password to the correspondence in your system
conn = psycopg2.connect(database='MtlTrajet', user='postgres', password='postgres')
conn.autocommit = True
cur = conn.cursor()


def calc_speed(table_name = segments):
    #calculating the speed between each consecutive points from "segments" table
    # and save the results in "segments_speeds" table
    psql_query = """
    drop table if exists segments_speeds;
    SELECT uuid as uuid, trip_id as trip_id,  St_Distance(geom,lag(geom) over w) as distance_pre, (EXTRACT(epoch FROM (timestamp - lag(timestamp) over w))/60) as time_int_prev,
	case
		when
		(timestamp - (lag(timestamp) over w)) = '00:00:00' then 0
		else
	(St_Distance(geom,lag(geom) over w))/(EXTRACT(epoch FROM (timestamp - lag(timestamp) over w))/60)
	end as speed, timestamp
	into segments_speeds from {0} 
	WINDOW w AS (partition by uuid, trip_id ORDER BY timestamp asc);
    """.format(table_name)

    cur.execute(psql_query)

    print("Speed btw points calculated")

def calc_accelerate(table_name = segments_speeds):
    #calculating the accelerate between each consecutive points from "segments_speeds" table
    # and save the results in "segments_accelarate" table
    psql_query = """
    drop table if exists segments_accelarate;
    SELECT uuid as uuid, segment_group as segment_group, timestamp,
    case
        when
        (timestamp - (lag(timestamp) over w)) = '00:00:00' then 0
        else
    (speed - lag(speed) over w)/(EXTRACT(epoch FROM (timestamp - lag(timestamp) over w))/60)
    end as accelarate
    into segments_accelarate
    from {0} 
    WINDOW w AS (partition by uuid, segment_group ORDER BY timestamp asc);
        """.format(table_name)

    cur.execute(psql_query)

    print("Acceleration btw points calculated")

def calc_85perc_speed(table_name = segments_speeds):
    # the function calculate the 85% speed and add it to the "mode_activity_segments" table

    ##Step1: finding the rank of each speed for each group_segment from "segments_speeds" table
    psql_query = """
    create index idx_segments_speeds on segments_speeds (uuid,segment_group, speed);
    drop table if exists segments_rank_speed;
    select uuid,segment_group, speed, percent_rank() over (partition by uuid,segment_group order by speed) as rank
    into segments_rank_speed
    from {0};
    """.format(table_name)
    cur.execute(psql_query)

    ##step2: find the 85 percentile speed
    psql_query = """
    alter table mode_activity_segments add column speed85 double precision;
    update mode_activity_segments
    set speed85 = a.speed
    from
    (select uuid, segment_group, max(speed) as speed, max(rank) as rank from segments_rank_speed where rank <= 0.85 group by uuid, segment_group
    ) as a
    where a.uuid = mode_activity_segments.uuid and a.segment_group = mode_activity_segments.segment_group;
    """
    cur.execute(psql_query)

    print("85 percentile speed calculated")

def calc_max_accelerate(table_name = segments_accelarate):
    # the function calculate the max. acceleration and add it to the "mode_activity_segments" table

    psql_query = """
    create index idx_segments_accelarate on segments_accelarate (uuid,segment_group, accelarate);
    alter table mode_activity_segments drop column max_accelarate;
    alter table mode_activity_segments add column max_accelarate double precision;
    update mode_activity_segments
    set max_accelarate = a.accel
    from
    (select uuid, segment_group, max(accelarate) as accel from {0} group by uuid, segment_group
    ) as a
    where a.uuid = mode_activity_segments.uuid and a.segment_group = mode_activity_segments.segment_group;
    """.format(table_name)

    cur.execute(psql_query)

    print("Max. acceleration calculated")

def calc_min_accelerate(table_name = segments_accelarate):
    # the function calculate the min. acceleration and add it to the "mode_activity_segments" table

    psql_query = """
    alter table mode_activity_segments add column min_accelarate double precision;
    update mode_activity_segments
    set min_accelarate = a.accel
    from
    (select uuid, segment_group, min(accelarate) as accel from {0} group by uuid, segment_group
    ) as a
    where a.uuid = mode_activity_segments.uuid and a.segment_group = mode_activity_segments.segment_group;
    """.format(table_name)

    cur.execute(psql_query)

    print("Min. acceleration calculated")


def updating_mode_activity_trip(table_name = mode_activity_segments):
    # the function calculate the min. acceleration and add it to the "mode_activity_segments" table

    psql_query = """
    alter table mode_activity_trip add column max_accelarate double precision;
    alter table mode_activity_trip add column min_accelarate double precision;
    alter table mode_activity_trip add column speed85 double precision;
    
    create index mix_speed85_accelrate_mode_trip on mode_activity_trip (uid,trip_id,speed85,max_accelarate);
    create index mix_speed85_accelrate_mode_segments on mode_activity_segments (uid,trip_id,speed85,max_accelarate)
    create index min_accelrate_mode_trip on mode_activity_trip (uid,trip_id,min_accelarate);
    """.format(table_name)

    cur.execute(psql_query)

    print("New columns(indexed) for speed and acc. added to mode_activity_trip table")

    #update mode_activity_trip table by speed85
    psql_query = """
    
    update mode_activity_trip
    set speed85 = a.speed85 
    from
    (
    select b.speed85 as speed85, b.trip_id, b.uid from mode_activity_segments b
    ) as a
    where mode_activity_trip.uid = a.uid and mode_activity_trip.trip_id = a.trip_id
    """.format(table_name)
    cur.execute(psql_query)
    print("Speed 85% is update in mode_activity_trip table")

    #update mode_activity_trip by max_accelerate
    psql_query ="""
    update mode_activity_trip
    set max_accelarate = a.max_accelarate 
    from
    (
    select b.max_accelarate as max_accelarate, b.trip_id, b.uid from mode_activity_segments b
    ) as a
    where mode_activity_trip.uid = a.uid and mode_activity_trip.trip_id = a.trip_id;
    """.format(table_name)
    cur.execute(psql_query)
    print("Max. acceleration is update in mode_activity_trip table")

    #update mode_activity_trip for min_accelerate
    psql_query ="""
    update mode_activity_trip
    set min_accelarate = a.min_accelarate 
    from
    (
    select b.min_accelarate as min_accelarate, b.trip_id, b.uid from mode_activity_segments b
    ) as a
    where mode_activity_trip.uid = a.uid and mode_activity_trip.trip_id = a.trip_id;
    """.format(table_name)
    cur.execute(psql_query)
    print("Min. acceleration is update in mode_activity_trip table")

    ##calculating the diff btw max min accelarate
    psql_query = """
    alter table mode_activity_trip add column diff_max_min_accel double precision;
    create index diff_accelrate_mode_trip2 on mode_activity_trip (uid,trip_id,diff_max_min_accel);
    
    update mode_activity_trip
    set diff_max_min_accel = abs(a.min_accelarate - a.max_accelarate)
    from
    (
    select b.min_accelarate as min_accelarate, b.max_accelarate as max_accelarate, b.trip_id, b.uid from mode_activity_segments b
    ) as a
    where mode_activity_trip.uid = a.uid and mode_activity_trip.trip_id = a.trip_id;
    """.format(table_name)
    cur.execute(psql_query)
    print("Difference between max. and min. acceleration is update in mode_activity_trip table")


def CBD_MTL_Island(table_name = mode_activity_trip):
    #Step 1: Creating the required tables
    #The convexhul of Montreal island is crated from the GTFS route which is generated from the GTFS data
    #It also can be generated from dmti shape file, but at this time, I believe the most accurate and fastest way to
    #create such a convexhul is using the GTFS data
    psql_query = """
    --creating a copy from gtfs_routs table 
    select * into temp_gtfs_routs from gtfs_routs;
    --adding the serial primary key 
    ALTER TABLE temp_gtfs_routs ADD id serial PRIMARY KEY;
    --creating the mtl_convexhull table
    select ST_ConvexHull(ST_Collect(geom)) into mtl_convexhull from temp_gtfs_routs;
    """

    #Step2: Fidning whether the O-D of each trip located in the mtl CBD, also inside or outside of mtl island
    psql_query = """
    alter table {0} add column dest_in_CBD boolean;
    alter table {0} add column orig_in_CBD boolean;
    
    update {0}
    set dest_in_CBD = ST_Intersects({0}.d_geom, b.geom) from
    (select ST_ConvexHull as geom from land_use_CBD_mtl limit 1) as b;
    
    update {0}
    set orig_in_CBD = ST_Intersects({0}.o_geom, b.geom) from
    (select ST_ConvexHull as geom from land_use_CBD_mtl limit 1) as b;
    
    --find whther the o-d is in CBD or mtl_island
    alter table {0} add column dest_in_mtl_island boolean;
    alter table {0} add column orig_in_mtl_island boolean;
    
    update {0}
    set orig_in_mtl_island = ST_Intersects({0}.o_geom, b.geom) from
    (select ST_ConvexHull as geom from mtl_convexhull limit 1) as b;
    
    update {0}
    set dest_in_mtl_island = ST_Intersects({0}.d_geom, b.geom) from
    (select ST_ConvexHull as geom from mtl_convexhull limit 1) as b;
    """.format(table_name)
    
    cur.execute(psql_query)
    print("The boolean values for CBD and MTL_Island are update in mode_activity_trip table")

def time_day(table_name = mode_activity_trip):
    #finding the time of day and day of week for each trip in mode_activity_trip table
    ###
    psql_query = """
    alter table {0} add column hour_start int;
    alter table {0} add column hour_end int;
    alter table {0} add column day int;
    
    update {0}
    set day = EXTRACT(ISODOW FROM start_trip), hour_start = EXTRACT(HOUR FROM start_trip), hour_end = EXTRACT(HOUR FROM end_trip);
    
    --creating the day of week
    select distinct(day) from {0}
    alter table {0} add column hour_day_interv int;
    alter table {0} add column day_week int;
    
    update {0}
    set day_week = (case 
    when day between  2 and 4 then 0 --mid_weekday
    when day = 1 or day = 5 then 1 --'MonFri'
    when day = 6 or day = 7 then 2 --'mid_weekday'
    end
    );
    --creating the time of day intervals
    update {0}
    set hour_day_interv = (case 
    when (hour_end between 19 and 24) or  (hour_end between 0 and 5) then 0 --eveninig_early
    when hour_end between 6 and 8 then 1 --'AM Peak'
    when hour_end between 12 and 13 then 2 --'lunch_time'
    when hour_end between 16 and 18 then 3 --'PM_Peak_time'
    else 4
    end
    );
    """.format(table_name)
    
    cur.execute(psql_query)
    print("The time of day and day of week for each trip is coded into {}".format(table_name))


def calc_distance_from_transit_stops(table_name = mode_activity_trip, GTFS_stops_table = gtfs_stops):
    #calculating the distance btw start and end point of each trip from transit stops
    #NOTE: This function needs the GTFS stops data imported to Postgres db
    #for speeding up the calculation the indices should be created on some columns of
    #GTFS stops table

    psql_query = """
    alter table {0} add column distance_stop_end double precision;
    alter table {0} add column distance_stop_start double precision; 
    
    create index on {1}(geom, stop_id); 
    create index on {0}(uid,trip_id, o_geom); 
    create index idx_uid_dgeom on {0}(uid,trip_id, d_geom); 
    
    SELECT a.uid as uid, a.trip_id as trip_id,
     b.stop_id as stop_id, ST_Distance(a.o_geom, b.geom) as distance_stop_start into temp_distance_start
          FROM {0} a, {1} b
    
    update {0}
    set distance_stop_end = (select st_distance(a.d_geom, b.geom) from {0} a, {1} b order by st_distance(a.d_geom, b.geom) limit 1);
    
    
    update {0}
    set distance_stop_start = (select st_distance(a.o_geom, b.geom) from {0} a, {1} b order by st_distance(a.o_geom, b.geom) limit 1);
    """.format(table_name, GTFS_stops_table)

    cur.execute(psql_query)
    print("The distance btw the origin and destination of each trip are inserted into {0}".format(table_name))

def calc_cumulative_direct_distance():
    psql_query = """
        alter table mode_activity_trip add column cumulative_distance double precision;
        alter table mode_activity_trip add column direct_distance double precision;
        select b.uid, a.utilisatio, count 
        count(a.utilisatio) as frequency from  landuse_cmm_immeubles_p_2011 a, mode_activity_trip_cleaned b
        where st_Distance(a.geom32618, b.d_geom) < 400 and uid = '0002d5c4-af93-4ed0-8e89-e86ce0bc81da'
        group by b.uid, a.utilisatio order by trip_id, frequency;


       """.format(table_name, GTFS_stops_table)

    cur.execute(psql_query)
    print("The distance btw the origin and destination of each trip are inserted into {0}".format(table_name))
