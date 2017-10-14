# Ali Yazdizadeh, July 2017
import psycopg2
import time


#make a con to postgresql database
#pls change the database, user and password to the correspondence in your system
conn = psycopg2.connect(database='MtlTrajet', user='postgres', password='postgres')
conn.autocommit = True
cur = conn.cursor()


def modify_modeprompts(mode_prompts_table = modeprompts, SRID_short = 4326, SRID_long = 32618)
    ##generate and update geometry columns
    psql_query = """
    ALTER TABLE {0} ADD COLUMN geom geometry;
    delete from {0} where longitude > -72.0000 or longitude < -78.0000 or latitude < 0.0000 or latitude  > 84.0000;
    update {0}
    set geom = ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), {1}),{2});
    create index ON {0}(uuid, timestamp );
    """.format(mode_prompts_table, SRID_short, SRID_long)



def detected_trips(table_name =  detected_trips, SRID_short = 4326, SRID_long = 32618):
    #the function import the results of "tripbreaker" algorithm as a table into postgresql db
    psql_query = """
    drop table if exists {0};
    create table {0}
    (
    db varchar,uid varchar,trip_id smallint,olon double precision,olat double precision,dlon double precision,dlat double precision,start_trip timestamp, end_trip timestamp,trip_code smallint,first_last smallint,direct_distance double precision,cumulative_distance double precision,merge_codes character varying(100)
    );
    COPY {0}(db,uid,trip_id,olon,olat,dlon,dlat,start_trip,end_trip,trip_code,first_last,direct_distance,cumulative_distance,merge_codes) FROM '/tmp/{0}.csv' DELIMITER ',' CSV HEADER;
    """.format(table_name)

    cur.execute(psql_query)

    print("{0} imported successfully".format(table_name))

    #creating the geometry columns
    psql_query = """
    ALTER TABLE detected_trips ADD COLUMN o_geom geometry;
    ALTER TABLE detected_trips ADD COLUMN d_geom geometry;
    --removing the geometries outside UMT 18
    delete from detected_trips where olon > -72.0000 or olon < -78.0000 or olat < 0.0000 or olat  > 84.0000;
    delete from detected_trips where dlon > -72.0000 or dlon < -78.0000 or dlat < 0.0000 or dlat  > 84.0000;
    
    update detected_trips
    set o_geom = ST_Transform(ST_SetSRID(ST_MakePoint(olon, olat), {1}),{2});
    
    update detected_trips
    set d_geom = ST_Transform(ST_SetSRID(ST_MakePoint(dlon, dlat), {1}),{2});
    """.format(table_name, SRID_short, SRID_long)

    cur.execute(psql_query)

    print("{0} altered successfully".format(table_name))

##The following function finds all the segment_groups btw each trip start and end time. This code is a complementary
# to tripbreaking algorithm.

def TripBreaking_seg_into_trips (segments_table = segments, detected_trips_table = detected_trips):

    psql_query = """
    drop table if exists segments_first_last_trip_id;
    select n1.uuid, n1.trip_id, n1.first_seg, n2.last_seg into segments_first_last_trip_id from 
    (select a.uuid, a.segment_group as first_seg, b.trip_id from {0} a, {1} b  where  a.uuid = b.uid and b.start_trip = a.timestamp) n1
    inner join 
    (select a.uuid, a.segment_group as last_seg, b.trip_id from {0} a, {1} b  where  a.uuid = b.uid and b.end_trip = a.timestamp) n2
    using(uuid, trip_id)""".format(segments_table,detected_trips_table)

    cur.execute(psql_query)

    psql_query = """
    alter table segments add column trip_id int;
    create index on segments(uuid, segment_group,trip_id);
    update {0}
    set trip_id = query.trip_id 
    from
    segments_first_last_trip_id as query
    where segments.uuid= query.uuid and segments.segment_group between query.first_seg and query.last_seg;
    drop table segments_first_last_trip_id;
    """.format(segments_table)

    cur.execute(psql_query)

    print("All segments of each trip were updated successfully".format(segments_table))

def assign_prompts_to_trips(mode_promt_table = modeprompts, detected_trips_table = detected_trips,detected_validated_trips= mode_activity_trip)
    #the function assign each modeprompts row to the closest(temporal and geographical) trip destination
    psql_quer = """
    drop table if exists loop_modeprompt CASCADE;
    CREATE TABLE loop_modeprompt (uid character varying,trip_id smallint,olon DOUBLE precision, olat DOUBLE precision, dlon DOUBLE precision, dlat DOUBLE precision, start_trip TIMESTAMP, 
    end_trip TIMESTAMP, trip_code SMALLINT, first_last SMALLINT, direct_distance DOUBLE precision, cumulative_distance DOUBLE precision,merge_codes VARCHAR(100), 
    o_geom geometry, d_geom geometry, id_prompt int, uuid character varying (50), latitude double precision, longitude double precision, mode int, purpose int, 
    timestamp timestamp, geom geometry);
    
    CREATE OR REPLACE FUNCTION assign_prompts_to_trips() returns setof loop_modeprompt as
    $$
    --declaring the variable for looping through rows of a table
    DECLARE
       r {0}%rowtype;
       --t sample_prompt%rowtype;
    --begining the definition of function   
    BEGIN
        FOR r IN select * from {0}
        LOOP
            --for t in select * from {1}
            RETURN QUERY 
                SELECT 
                    a.*, r.*
                from (
                    select
                        *
                    from
                        {1} b
                    where 
                        b.uid = r.uuid 
                    order by
                        abs(EXTRACT(epoch FROM (b.end_trip - r.timestamp))), ST_Distance(b.d_geom, r.geom) 
                    limit 1) a;
    end loop;
    
    --end of the definition of the function   
    END
    --definition of the language used by postgis
    $$LANGUAGE plpgsql;
    --this query should be execute to return the content of the table related to the aboved defined function(It may takes more than one hour to execute)
    
    drop table if exists {2};
    with s as(
    select * from assign_prompts_to_trips()
    )
    select * 
    into {2}
    from s;
    """.format(mode_promt_table ,detected_trips_table, detected_validated_trips)

    cur.execute(psql_query)

    print("{2} was created successfully".format(detected_validated_trips))

